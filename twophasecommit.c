#include "twophasecommit.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <postgres.h>
#include <unistd.h>
#include <sys/stat.h>
#include <postmaster/bgworker.h>
#include <utils/builtins.h>

/* 
 * These are only used by this file and are not exposed outside
 */
extern tpc_txnset * tpc_txnset_from_file(char *local_globalid);

const static char preparefmt[] = "PREPARE TRANSACTION '%s'";
const static char commitfmt[] = "COMMIT PREPARED '%s'";
const static char rollbackfmt[] = "ROLLBACK PREPARED '%s'";
const static char checkfmt[] = "SELECT * FROM pg_prepared_xacts "
		               "WHERE gid = '%s'";
const static char phasefmt[] = "phase %s\n";
const static char actionfmt[] = "%s postgresql://%s:%s/%s %s %s\n";
const static char getactionfmt[] = "%s %s %s %s";
void tpc_bgworker(Datum);
static void tpc_register_bgworker(const char *fname);
static void tpc_txnset_transition_state(tpc_txnset *txnset, tpc_state state);
PG_FUNCTION_INFO_V1(tpc_cleanup);

/* Begins a two-hase commit run.
 *
 * Prepares a tpc_txnset with an open file handle to the log.
 *
 * If the log file exists before this run, ereports an ERROR
 *
 * As a general rule, one tpc set per backend per prefix is
 * supported.  Otherwise you may get funny rollbacks due to txn
 * name conflicts on the remote systems.
 *
 */

tpc_txnset *
tpc_begin(char *prefix)
{

	tpc_txnset *new_txnset;
	new_txnset = palloc0(sizeof(tpc_txnset));

        /* only supporting pids to 9999999,
           have two separators, and one terminator, so add + 7 + 3
          */

	if (getpid() > 9999999)
		ereport(ERROR, (errcode(ERRCODE_INDICATOR_OVERFLOW), 
				errmsg("PID above supported value")));

	/* txn length for txn name includes pid, plus counter, plus 2 seps
	   plus the terminator
	*/
	
	if (strlen(prefix) + 7 + 1 + 3 > NAMEDATALEN)
		ereport(ERROR, (errcode(ERRCODE_INDICATOR_OVERFLOW),
				errmsg("Prefix too long for name field")));
	if (strlen(prefix) + 7 + 5 + 3 > NAMEDATALEN)
		ereport(WARNING, 
			(errmsg("Prefix may become too long for name")));
       
        /* setting up data for new_txnset */
	/* TODO: move to UUIDs */
	/* not vulnerable to race since pid is part of file name in terms
	 * of actual duplicate detection but is vulnerable to reuse on busy
	 * systems....
	 */
	new_txnset->tpc_phase = BEGIN;
	tpc_txnfile_write_state(new_txnset, BEGIN);
	snprintf(new_txnset->txn_prefix, sizeof(new_txnset->txn_prefix), 
		"%s_%d", prefix, getpid());
	tpc_txnsetfile_begin(new_txnset, new_txnset->txn_prefix);
	new_txnset->counter = 0;
	new_txnset->head = NULL;
	new_txnset->latest = NULL;
	return new_txnset;
}

/*
 * Registeres a background worker to process the file.
 *
 * We use the bgw_extra field to point to the file rather than using the
 * arg struct.
 *
 */

static void
tpc_register_bgworker(const char *fname)
{
	BackgroundWorkerHandle *bgwhandle = NULL;
	BackgroundWorker *bgw = palloc0(sizeof(bgw));
	snprintf(bgw->bgw_name, sizeof(bgw->bgw_name),
                "TPC Cleanup %s", fname);
        null_bgwmain(bgw);
	strncpy(bgw->bgw_library_name, "copytoremote.so",
	        sizeof(bgw->bgw_library_name));
	strncpy(bgw->bgw_function_name, "tpc_bgworker", 
	       sizeof(bgw->bgw_function_name));
	bgw->bgw_restart_time = 60;
	strncpy(bgw->bgw_extra, fname, sizeof(bgw->bgw_extra));
	bgw->bgw_main_arg = 0;
	bgw->bgw_notify_pid = 0;
	if (!RegisterDynamicBackgroundWorker(bgw, &bgwhandle)){
		ereport(WARNING, (errmsg(
		        "could not start worker for %s, "
		        "Manual cleanup required.", fname)));
	}
	return;
}

/*
 * Represents that we are in a completed state and consistent state
 * as far as the calling action is concerned.
 * 
 * The flag can_complete is passed in which tells this function whether
 * we could complete the remote-sides of the connections.  If so, we
 * mark the file as completed and then delete it (this ensures that cleanup
 * is easier if the unlink fails).
 */

static void
complete(tpc_txnset *txnset, bool can_complete)
{
	tpc_txnsetfile_write_phase(txnset,
		( can_complete ? COMPLETE : INCOMPLETE )
	);
	fclose(txnset->log);
	if (can_complete){
		txnset->tpc_phase = COMPLETE;
		unlink(txnset->logpath);
	}
	else {
		ereport(WARNING, (errmsg("could not clean up.  "
		                         "Starting bgw for xact %s",
		                         txnset->logpath)));
		tpc_register_bgworker(txnset->logpath);
		txnset->tpc_phase = INCOMPLETE;
	}
}

/*
 * Writes a prepare action to the log for the conneciton
 * Then issues the prepare.  If errors, raises an error via ereport()
 *
 * Otherwise returns the transaction name (needed for commit or rollback).
 * The string returned will always be less than or equal to NAMEDATALENGTH
 */

char *
tpc_prepare(tpc_txnset *txnset, PGconn *cnx)
{
	/* probably only need 84 bytes here, but rounding up */
	char prepare_query[128];
	PGresult *res;

	/* initial setup and precondition checks */
	tpc_txn *txn = palloc0(sizeof(tpc_txn));
	int lengthcounter = strlen(txnset->txn_prefix) + 2;

	for (uint t = txnset->counter;t; t = t/10)
		++lengthcounter;

	if (lengthcounter > NAMEDATALEN)
		ereport(ERROR, (errcode(ERRCODE_INDICATOR_OVERFLOW),
				errmsg("Name of transaction is too long")));

	snprintf(txn->txn_name, sizeof(txn->txn_name),
		"%s_%d", txnset->txn_prefix, ++txnset->counter);

	snprintf(prepare_query, sizeof(prepare_query),
		preparefmt, txn->txn_name);

	if (!tpc_state_is_valid_transition(tpc_phase, PREPARE))	
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("Not in a valid phase of transaction")));
	tpc_txnsetfile_write_phase(txnset, PREPARE);
	txnset->tpc_phase = PREPARE;

	/* At this stage our tpc log is a write-ahead log.  We write anything
	 * we might have to clean up later and we fsync it
	 */
	txn->cnx = cnx;

	tpc_txnfile_write_action(txnset, txn, "todo");

	/* Ok, do now we have written to disk what we intend to do
	 * we do it and check the result.  Anything at this stage should 
	 * roll back the calling transaction and with it all
	 * remote transactions
	 */
	res = PQexec(txn->cnx, prepare_query);
	if (PQresultStatus(res) != PGRES_COMMAND_OK){
		PQclear(res);
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("query (%s) failed", prepare_query)));
	}
	PQclear(res);
	if (txnset->head){
		txnset->latest->next = txn;
		txnset->latest = txn;
	} else {
		txnset->head = txn;
		txnset->latest = txn;
	}
	return txn->txn_name;
}

/*
 * Commits a transaction by name on a connection
 *
 * After writes status (committed or error) as action in pending transaction 
 * log.
 *
 * Records our error state for complete run.
 */

tpc_phase
tpc_commit(tpc_txnset *txnset)
{
	bool can_complete = true;

	if (txnset->tpc_phase != PREPARE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("Not in a valid phase of transaction")));
	}

	fprintf(txnset->log, phasefmt, "commit");
	txnset->tpc_phase = COMMIT;

		
	for(tpc_txn *curr = txnset->head; curr; curr = curr->next){
		PGresult *res;
		char commit_query[128];
		snprintf(commit_query, sizeof(commit_query), 
			commitfmt, curr->txn_name);
		res = PQexec(curr->cnx, commit_query);

		/* We are not allowed to throw errors here, but we can flag
		 * the run as impossible to complete.
		 */
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			can_complete = false;
		tpc_txnsetfile_write_action(txnset, curr, COMMIT,
				(PQresultStatus(res) == PGRES_COMMAND_OK
				? "OK" : "BAD"));
	}
	complete(txnset, can_complete);
	return txnset->tpc_phase;
}

/* 
 * Rolls back the transaction by name on a connection
 * Writes data to rollback segment of pending transaction log.
 */
tpc_phase
tpc_rollback(tpc_txnset *txnset)
{
	bool can_complete = true;

	if (txnset->tpc_phase != PREPARE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("Not in a valid phase of transaction")));
	}

	fprintf(txnset->log, phasefmt, "rollback");
	txnset->tpc_phase = ROLLBACK;

		
	for(tpc_txn *curr = txnset->head; curr; curr = curr->next){
		PGresult *res;
		char rollback_query[128];
		snprintf(rollback_query, sizeof(rollback_query), 
			rollbackfmt, curr->txn_name);
		res = PQexec(curr->cnx, rollback_query);

		/* We are not allowed to throw errors here, but we can flag
		 * the run as impossible to complete.
		 */
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			can_complete = false;
		tpc_txnsetfile_write_action(txnset, curr, ROLLBACK, 
				(PQresultStatus(res) == PGRES_COMMAND_OK
				? "OK" : "BAD"));
	}
	complete(txnset, can_complete);
	return txnset->tpc_phase;
}

/* Checks to see if a txn exists.  If the query succeeds and the transaction
 * does not exist then this returns true and removes the transaction from
 * the transaction set.
 *
 * Otherwise return false so the cleanup will try to remove the transaction,
 */
static bool
check_txn(tpc_txnset *txnset, tpc_txn *last, tpc_txn *curr)
{
	char query[128];
	PGresult *res;
	bool removed = false;
	snprintf(query, sizeof(query), 
		checkfmt, curr->txn_name);
	
	res = PQexec(curr->cnx, query);
	if ((PQresultStatus(res) != PGRES_TUPLES_OK) && (PQresultStatus(res) != PGRES_COMMAND_OK)){
		ereport(INFO, (errmsg("Transaction %s query failed", curr->txn_name)));
		removed = false;
	}
	else if (PQntuples(res) >= 1){
		removed = false;
		ereport(WARNING, (errmsg("Transaction %s found %d times", curr->txn_name, PQntuples(res))));
	} else {
		/* txns are palloced so no need to free. 
		 * Besides process dies as soon as we complete
		 * cleanup anyway
		 */
		ereport(INFO, (errmsg("Transaction %s not found", curr->txn_name)));
		PQfinish(curr->cnx);
		if (last)
			last->next = curr->next;
		else
			txnset->head = curr->next;
		removed = true;
	}
	PQclear(res);
	return removed;
}

/* This is the bg_cleanup process which runs once the txnset has been
 * initialized.  It repeatedly loops through the transactions.  If the
 * transactions no longer exist or if they can be brought to completion
 * they are removed from the list.
 *
 * When all transactions are removed from the list, we exit.
 *
 * If rollback is false we commit transactions
 * and if true we roll them back.
 */
static void
bg_cleanup(tpc_txnset *txnset, bool rollback)
{
	tpc_txn *last = NULL;
	tpc_txn *curr;
	PGresult *res;
	do {
		/* check to see if we are in a re-run and if so sleep */
		if (txnset->tpc_phase == INCOMPLETE)
			sleep(1);

		curr = txnset->head;
		for (curr = txnset->head; curr; curr = curr->next){
			ereport(WARNING, (errmsg("cleaning up xact %s", curr->txn_name)));

			/* The connection may have gone away so we had
			 * better check its status and reset if needed
			 */
			if (PQstatus(curr->cnx) == CONNECTION_BAD)
				PQreset(curr->cnx);

			if (check_txn(txnset, last, curr))
				continue;

			char query[128];

			if (rollback)
				snprintf(query, sizeof(query), 
					rollbackfmt, curr->txn_name);
			else
				snprintf(query, sizeof(query), 
					commitfmt, curr->txn_name);
			
			res = PQexec(curr->cnx, query);

			/* if successful, remove this from list */
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
				if (last)
					last->next = curr->next;
				else
					txnset->head = curr->next;
			else
				last = curr;
		}
		txnset->tpc_phase = INCOMPLETE;

	} while (txnset->head);

}

void
tpc_txnset_transition_state(tpc_txnset *txnset, tpc_state state) {tpc_state old_state = txnset->state;
	if (old_state >= state) {
		ereport(ERROR, errmsg("Cannot rewind state!"));
	}
	switch(tpc_state)
	{
		case BEGIN:
			if (NULL != txnset->head || NULL != txnset->tail){
				ereport(ERROR, 
					errmsg("Invalid state for begin: already has transactions!"));
			}
			break;
		case COMMIT:
			if (old_state == BEGIN)
				ereport(ERROR, "Must prepare before commit!");
			break;
		case ROLLBACK:
			switch (old_state)
			{
				case BEGIN:
					ereport(ERROR, "Must prepare transaction before rolling back");
					break;
				case COMMIT:
					ereport(ERROR, "Cannot roll back committed transaction set!");
			};
			break;
		case COMPLETE: /* not sure if begin -> complete is a valid state change */
			switch (old_state)
			{
				case PREPARE:
					ereport(ERROR, "Must commit or rollback prepare");

			}
		case INCOMPLETE

			
		
}

void
tpc_bgworker(Datum unused)
{
	tpc_process_file(MyBgworkerEntry->bgw_extra);
	return;
}

void
tpc_process_file(char *fname)
{
	tpc_txnset *txnset;
	ereport(WARNING, (errmsg("Background worker started")));
	txnset = tpc_txnset_from_file(fname);
	bg_cleanup(txnset, lastphase != COMMIT);
	unlink(txnset->logpath);
	return;
}

/* SQL function for firing off a cleanup worker for a given file.
 * 
 * note that there is not currently any protection for race conditions arising
 * from cleaning up a file that is correctly in a prepare state, though this is
 * not hard to avoid.
 *
 * This can only be run as a superuser because otherwise a normal user could 
 * interfere with another session's in-process txnsets.
 */

Datum
tpc_cleanup(PG_FUNCTION_ARGS)
{
   char *fname = PG_GETARG_CSTRING(0);
   tpc_register_bgworker(fname);
   PG_RETURN_VOID();
}

/* SQL function to list ongoing global transactions.
 * This This can only be run as superuser due to the possibility of malicious use.
 */

Datum
tpc_list_txnset(PG_FUNCTION_ARGS)
{
}

/* SQL function to introspect txnsets on disk.
 * Must be run by superuser.
 * Returns connectoin string and status of each libpq connection registered.
 */
