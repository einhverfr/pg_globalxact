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

#include "twophasecommit.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <postgres.h>
#include <unistd.h>
#include <sys/stat.h>
#include <utils/builtins.h>

const static char phasefmt[] = "phase %s\n";
const static char actionfmt[] = "%s postgresql://%s:%s/%s %s %s\n";
const static char getactionfmt[] = "%s %s %s %s";
const static char dirpath[] = "extglobalxact";

/*Max length of file line.  Going with 512 becaus connection strings in theory could be up to 255 characters long.
 */
#define LINEBUFFSIZE 512 

tpc_txnset *tpc_txnset_from_file(const char *local_globalid);
void tpc_txnsetfile_start(tpc_txnset *txnset, const char *local_globalid);
void tpc_txnsetfile_write_phase(tpc_txnset *txnset, tpc_phase next_phase);
void tpc_txnsetfile_write_action(tpc_txnset *txnset, tpc_txn *txn, const char *action);
void tpc_txnsetfile_complete(tpc_txnset *txnset);
  

/*
 * tpc_txnset *tpc_txnset_from_file(const char *local_globalid)
 * This function takes in the local_globalid of the transaction set
 * and loads the transaction set into memory from the file.  This is
 * used to load the file for the background worker, as well as for 
 * administrator commands.
 */

tpc_txnset 
*tpc_txnset_from_file(const char *local_globalid)
{
	tpc_txnset *txnset;
	char linebuff[LINEBUFFSIZE];
	tpc_phase lastphase;
	txnset = palloc(sizeof(tpc_txnset));
	txnset->head = NULL;
	txnset->latest = NULL;

	strncpy(txnset->logpath, local_globalid, sizeof(txnset->logpath));
	txnset->log = fopen(txnset->logpath, "r");

	/* File does not exist or we cannot open it */
	if (txnset->log == NULL){
		int err = errno;
		ereport(ERROR, (errmsg("Manual cleanup may be necessary. "
		                  "Could not open file %s, %s", 
			          txnset->logpath, strerror(err))));
	}
	while (fgets(linebuff, sizeof(linebuff), txnset->log)){
		char firstword[12];
		char phaselabel[12];
		char connectionstr[255];
		char txnname[NAMEDATALEN];
		char status[64];

		if (LINEBUFFSIZE == strlen(linebuff) && linebuff[LINEBUFFSIZE - 1] != '\0'){ 
			ereport(ERROR, (errmsg("line exceeded max length of 255.  Most likely this is file corruption: %s", linebuff)));
		}
		if (strstr(linebuff, "phase") == linebuff){
			/* here we set the phase of the txnset. */
			
			sscanf(linebuff, "%s %s", firstword, phaselabel);
			lastphase = tpc_phase_from_label(phaselabel);
			txnset->tpc_phase = lastphase;
			if (INCOMPLETE == lastphase)
				ereport(WARNING, 
				        (errmsg("Incomplete txnset found.  "
				                "Entering recovery.")));
		} else {
			tpc_txn *txn = palloc0(sizeof(tpc_txn));
			sscanf(linebuff, getactionfmt,
			       firstword, connectionstr, txnname, status);

			if (strstr(linebuff, phaselabel) != linebuff)
				ereport(WARNING, (errmsg("wrong phase.  "
				       "Expected %s but got %s", 
				       phaselabel, firstword)));

			if (! strstr(connectionstr, "postgresql://")){
				ereport(WARNING, (errmsg("%s in line %s "
				        "does not look like a connection "
				        "string.  Ignoring", 
				        connectionstr, linebuff)));
				continue;
			}
			txn->cnx = PQconnectdb(connectionstr);
			strncpy(txn->txn_name, txnname, sizeof(txn->txn_name));
			if (txnset->head){
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
tpc_txnsetfile_start(tpc_txnset *txnset, const char *local_globalid)
{
	if (access(dirpath, 0)){
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
	if (! txnset->log)
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		        errmsg("could not create file %s", txnset->logpath)));
}

/*
 * void tpc_txnsetfile_write_phase(tpc_txnset *txnset, tpc_phase phase)
 *
 * Logs the phase state to the txnsetfile.
 */

void tpc_txnsetfile_write_phase(tpc_txnset *txnset, tpc_phase phase) {
	fprintf(txnset->log, phasefmt, tpc_phase_get_label(phase));	
}

/*
 * void tpc_txnsetfile_write_actoin(tpc_txnset *txnset, tpc_phase phase, tpc_txn *txn, const char *action)
 *
 * Writes the action, state, etc to the transactionset file.
 *
 * This causes an fsync on the file to make sure everything is recoverable in the event of server failure.
 */

void
tpc_txnsetfile_write_action(tpc_txnset *txnset, tpc_txn *txn, const char *status)
{
	
	fprintf(txnset->log, actionfmt, 
			tpc_phase_get_label(txnset->tpc_phase),
			PQhost(txn->cnx),
			PQport(txn->cnx),
			PQdb(txn->cnx),
			txn->txn_name,
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
tpc_txnsetfile_complete(tpc_txnset *txnset)
{
	if (txnset->tpc_phase != COMPLETE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE),
		        errmsg("Transaction not compplete!, state is %s", tpc_phase_get_label(txnset->tpc_phase))));
		
	fclose(txnset->log);
	unlink(txnset->logfile);
}
