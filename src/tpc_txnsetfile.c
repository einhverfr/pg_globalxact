/*
 * txnsetfile.c
 * maintainer: Chris Travers <chris.travers@gmail.com>
 *
 * The purpose of this file is to manage the file access for the initial
 * approach of storing global transactoin sets on the local filesystem
 * for PostgreSQL.  This approach has hte benefit that it is easy, but
 * has a downside that it will not perform very well if very large numbers
 * of distributed transactions are occurring at the same time.
 *
 * The implementation here is designed to be correct rather than fast
 * with the idea that it is easier to preserve correctness and obtain
 * speed rather than to obtain correctness and preserve speed.
 *
 * Currently these are stored in a new folder extglobalexact in the
 * data directory.  While the name is longer than might be strictly
 * necessary, I did not want to preclude something eventually in
 * core PostgreSQL using the same path.
 *
 * In general errors thrown in this file are ERROR_INVALID_TRANSACTION_STATE
 * errors, as they affect the state of the existing global transaction.
 *
 * This file is not responsible for any global transaction id semantics or
 * validation.
 *
 */

#include "tpc_txnset.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <postgres.h>
#include <unistd.h>
#include <sys/stat.h>
#include <utils/builtins.h>
#include <postmaster/bgworker.h>

PG_MODULE_MAGIC;

//PG_FUNCTION_INFO_V1(tpc_txnset_contents);
static const char phasefmt[] = "phase %s\n";
static const char actionfmt[] = "%s postgresql://%s:%s/%s %s %s\n";
static const char getactionfmt[] = "%s %s %s %s";
static const char dirpath[] = "extglobalxact";
static const char preparefmt[] = "PREPARE TRANSACTION '%s'";
static const char commitfmt[] = "COMMIT PREPARED '%s'";
static const char rollbackfmt[] = "ROLLBACK PREPARED '%s'";
const static char checkfmt[] = "SELECT * FROM pg_prepared_xacts "
		               "WHERE gid = '%s'";

static void tpc_register_bgworker(const char *fname);

/*Max length of file line.  Going with 512 becaus connection strings in theory could be up to 255 characters long.
 */
#define LINEBUFFSIZE 512

tpc_txnset *tpc_txnset_from_file(const char *local_globalid);
void	    tpc_txnsetfile_start(tpc_txnset * txnset, const char *local_globalid);
void	    tpc_txnsetfile_write_phase(tpc_txnset * txnset, tpc_phase next_phase);
void	    tpc_txnsetfile_write_action(tpc_txnset * txnset, tpc_txn * txn, const char *status);
void	    tpc_txnsetfile_complete(tpc_txnset * txnset);
void        tpc_bgworker(Datum unused);
void        tpc_process_file(char *fname);
static void bg_cleanup(tpc_txnset *txnset, bool rollback);
static bool check_txn(tpc_txnset *txnset, tpc_txn *last, tpc_txn *curr);


/*
 * tpc_txnset *tpc_txnset_from_file(const char *local_globalid)
 * This function takes in the local_globalid of the transaction set
 * and loads the transaction set into memory from the file.  This is
 * used to load the file for the background worker, as well as for
 * administrator commands.
 *
 * This operates in whatever the memory context is current when the
 * function was called.  This allows it to be called in set returning
 * functions for monitoring distributed transaction state.
 */

tpc_txnset
* tpc_txnset_from_file(const char *local_globalid) {
    tpc_txnset *txnset;
    char	linebuff[LINEBUFFSIZE];
    tpc_phase	lastphase;
    txnset = palloc(sizeof(tpc_txnset));
    txnset->head = NULL;
    txnset->latest = NULL;

    strncpy(txnset->logpath, local_globalid, sizeof(txnset->logpath));
    txnset->log = fopen(txnset->logpath, "r");

    /* File does not exist or we cannot open it */
    if (txnset->log == NULL) {
	int	    err = errno;
	ereport(ERROR, (errmsg("Manual cleanup may be necessary. "
		    "Could not open file %s, %s",
		    txnset->logpath, strerror(err))));
    }
    while (fgets(linebuff, sizeof(linebuff), txnset->log)) {
	char	    firstword[12];
	char	    phaselabel[12];
	char	    connectionstr[255];
	char	    txnname[NAMEDATALEN];
	char	    status[64];

	if (LINEBUFFSIZE == strlen(linebuff) && linebuff[LINEBUFFSIZE - 1] != '\0') {
	    ereport(ERROR, (errmsg("line exceeded max length of 255.  Most likely this is file corruption: %s", linebuff)));
	}
	if (strstr(linebuff, "phase") == linebuff) {
	    /* here we set the phase of the txnset. */

	    sscanf(linebuff, "%s %s", firstword, phaselabel);
	    lastphase = tpc_phase_from_label(phaselabel);
	    txnset->tpc_phase = lastphase;
	    if (INCOMPLETE == lastphase)
		ereport(WARNING,
		    (errmsg("Incomplete txnset found.  "
			    "Entering recovery.")));
	} else {
	    tpc_txn    *txn = palloc0(sizeof(tpc_txn));
	    sscanf(linebuff, getactionfmt,
		firstword, connectionstr, txnname, status);

	    if (strstr(linebuff, phaselabel) != linebuff)
		ereport(WARNING, (errmsg("wrong phase.  "
			    "Expected %s but got %s",
			    phaselabel, firstword)));

	    if (!strstr(connectionstr, "postgresql://")) {
		ereport(WARNING, (errmsg("%s in line %s "
			    "does not look like a connection "
			    "string.  Ignoring",
			    connectionstr, linebuff)));
		continue;
	    }
	    txn->conn= PQconnectdb(connectionstr);
	    strncpy(txnset->txn_prefix, txnname, sizeof(txnset->txn_prefix));
	    if (txnset->head) {
		txnset->latest->next = txn;
		txnset->latest = txn;
	    } else {
		txnset->head = txn;
		txnset->latest = txn;
	    }
	}
    }
    return txnset;
}

/* void tpc_txnsetfile_start (tpc_txnset *txnset, const char *local_globalid)
 * initializes a new file, makes sure that directory etc is set up,
 * Only used when starting a global transaction.
 *
 * The txnset must already be created, and the local_globalid is a string
 * intended to be unique on the server.
 */

void
tpc_txnsetfile_start(tpc_txnset * txnset, const char *local_globalid)
{
    if (access(dirpath, 0)) {
	mkdir(dirpath, 0700);
    }
    if ((strlen(dirpath) + strlen(local_globalid) + 1) >= sizeof(txnset->logpath))
	ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		errmsg("File path too long.  Path:  %s Localgtxnid: %s",
		    dirpath, local_globalid)));
    snprintf(txnset->logpath, sizeof(txnset->logpath),
	"%s/%s", dirpath, local_globalid);
    if (access(txnset->logpath, F_OK) != -1)
	ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		errmsg("file %s already exists", txnset->logpath)));

    txnset->log = fopen(txnset->logpath, "w");
    if (!txnset->log)
	ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		errmsg("could not create file %s", txnset->logpath)));
}

/*
 * void tpc_txnsetfile_write_phase(tpc_txnset *txnset, tpc_phase phase)
 *
 * Logs the phase state to the txnsetfile.
 */

void
tpc_txnsetfile_write_phase(tpc_txnset * txnset, tpc_phase phase)
{
    fprintf(txnset->log, phasefmt, tpc_phase_get_label(phase));
}

/*
 * void tpc_txnsetfile_write_action(tpc_txnset *txnset, tpc_phase phase, tpc_txn *txn, const char *status)
 *
 * Writes the action, state, etc to the transactionset file.
 *
 * This causes an fsync on the file to make sure everything is recoverable in the event of server failure.
 */

void
tpc_txnsetfile_write_action(tpc_txnset * txnset, tpc_txn * txn, const char *status)
{

    fprintf(txnset->log, actionfmt,
	tpc_phase_get_label(txnset->tpc_phase),
	PQhost(txn->conn),
	PQport(txn->conn),
	PQdb(txn->conn),
	txnset->txn_prefix,
	status);
    fflush(txnset->log);
}

/*
 * void tpc_txnsetfile_complete(tpc_txnset *txnset)
 *
 * Errors if state is not complete
 * Otherwise closes and removes transaction set file
 */
void
tpc_txnsetfile_complete(tpc_txnset * txnset)
{
    if (txnset->tpc_phase != COMPLETE)
	ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		errmsg("Transaction not compplete!, state is %s", tpc_phase_get_label(txnset->tpc_phase))));

    fclose(txnset->log);
    unlink(txnset->logpath);
}


/* SQL FUNCTION SECTION */

/* SQL function for firing off a cleanup worker for a given file.
 *
 * note that there is not currently any protection for race conditions arising
 * from cleaning up a file that is correctly in a prepare state, though this is
 * not hard to avoid.
 *
 */

PG_FUNCTION_INFO_V1(tpc_cleanup_txnset);
Datum
tpc_cleanup_txnset(PG_FUNCTION_ARGS) {
    char       *fname = PG_GETARG_CSTRING(0);
    tpc_register_bgworker(fname);
    PG_RETURN_VOID();
}

/* SQL function for looking into the transacion set files themselves.
 * This returns a table of
 *   - host
 *   - port
 *   - database
 *   - transaction status
 */

/* not optimizing this size-wise */

typedef struct info_line {
    char       *host; int port; char *database; char *status_label;
} info_line;




/* State so we don't have to keep re-reading the file for each line
 * Using value_per_call mode here.  It is not terribly hard to do it
 * in this case.  Right now this still doesn't work (still needs
 * some of the tuple constructor parts done).  But will be completed
 * soon, after basic test programs are done.
Datum
tpc_txnset_contents(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    //info_line  return_next;
    tpc_txnset *txnset_new;
    char *gtlxid = PG_GETARG_CSTRING(0);

    /* check to see if caller supports us returning a tuplestore */
    /*
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	ereport(ERROR,
	    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
	ereport(ERROR,
	    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("materialize mode required, but it is not allowed in this context")));

    /*
     * need to set up per statement memory context here for the txnset
     */
    /*
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;

    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    if (SRF_IS_FIRSTCALL()){
       // functx = SRF_FIRSTCALL_INIT();
	txnset_new = palloc0(sizeof(tpc_txnset));
    	txnset_new = tpc_txnset_from_file(gtlxid);
	memcpy(txnset, txnset_new, sizeof(&txnset));
    }

    /*
     * For each transaction we return the tuple structure
     */
    //if (
        // build the tuple and return it
    //    info_line.host info_line.port info_line.database info_line.status_label;
    /* Finally, close, and return end */
    /*MemoryContextSwitchTo(oldcontext);
    //SRF_RETURN_DONE(per_query_ctx); // not working yet anyway
}

/* 
 * Rolls back the transaction by name on a connection
 * Writes data to rollback segment of pending transaction log.
 */
tpc_phase
tpc_rollback()
{
	bool can_complete = true;

	if (txnset->tpc_phase != PREPARE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("Not in a valid phase of transaction")));
	}

	txnset->tpc_phase = ROLLBACK;
	tpc_txnsetfile_write_phase(txnset, ROLLBACK);

		
	for(tpc_txn *curr = txnset->head; curr; curr = curr->next){
		PGresult *res;
		char rollback_query[128];
		snprintf(rollback_query, sizeof(rollback_query), 
			rollbackfmt, txnset->txn_prefix);
		res = PQexec(curr->conn, rollback_query);

		/* We are not allowed to throw errors here, but we can flag
		 * the run as impossible to complete.
		 */
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			can_complete = false;
		tpc_txnsetfile_write_action(txnset, curr, 
				(PQresultStatus(res) == PGRES_COMMAND_OK
				? "OK" : "BAD"));
	}
	if (can_complete)
		tpc_txnsetfile_complete(txnset);
	return txnset->tpc_phase;
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
tpc_commit()
{
	bool can_complete = true;

	if (txnset->tpc_phase != PREPARE) {
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				errmsg("Not in a valid phase of transaction")));
	}

	txnset->tpc_phase = COMMIT;
	tpc_txnsetfile_write_phase(txnset, COMMIT);

		
	for(tpc_txn *curr = txnset->head; curr; curr = curr->next){
		PGresult *res;
		char commit_query[128];
		snprintf(commit_query, sizeof(commit_query), 
			commitfmt, txnset->txn_prefix);
		res = PQexec(curr->conn, commit_query);

		/* We are not allowed to throw errors here, but we can flag
		 * the run as impossible to complete.
		 */
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
			can_complete = false;
		tpc_txnsetfile_write_action(txnset, curr,
				(PQresultStatus(res) == PGRES_COMMAND_OK
				? "OK" : "BAD"));
	}
	if (can_complete)
		tpc_txnsetfile_complete(txnset);
	return txnset->tpc_phase;
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
        strncpy(bgw->bgw_library_name, "pg_globalxact.sl",
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
	txnset = tpc_txnset_from_file(fname);
	bg_cleanup(txnset, txnset->tpc_phase != COMMIT);
	unlink(txnset->logpath);
	return;
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
			char query[128];
			ereport(WARNING, (errmsg("cleaning up xact %s", txnset->txn_prefix)));

			/* The connection may have gone away so we had
			 * better check its status and reset if needed
			 */
			if (PQstatus(curr->conn) == CONNECTION_BAD)
				PQreset(curr->conn);

			if (check_txn(txnset, last, curr))
				continue;


			if (rollback)
				snprintf(query, sizeof(query), 
					rollbackfmt, txnset->txn_prefix);
			else
				snprintf(query, sizeof(query), 
					commitfmt, txnset->txn_prefix);
			
			res = PQexec(curr->conn, query);

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
		checkfmt, txnset->txn_prefix);
	
	res = PQexec(curr->conn, query);
	if ((PQresultStatus(res) != PGRES_TUPLES_OK) && (PQresultStatus(res) != PGRES_COMMAND_OK)){
		ereport(INFO, (errmsg("Transaction %s query failed", txnset->txn_prefix)));
		removed = false;
	}
	else if (PQntuples(res) >= 1){
		removed = false;
		ereport(WARNING, (errmsg("Transaction %s found %d times", txnset->txn_prefix, PQntuples(res))));
	} else {
		/* txns are palloced so no need to free. 
		 * Besides process dies as soon as we complete
		 * cleanup anyway
		 */
		ereport(INFO, (errmsg("Transaction %s not found", txnset->txn_prefix)));
		PQfinish(curr->conn);
		if (last)
			last->next = curr->next;
		else
			txnset->head = curr->next;
		removed = true;
	}
	PQclear(res);
	return removed;
}
