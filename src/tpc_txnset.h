#ifndef TPC_TXNSET_H

#define TPC_TXNSET_H

#include <libpq-fe.h>
#include <postgres.h>
#include "tpc_phase.h"
#include <access/xact.h>
#include <funcapi.h>

#define TPC_LOGPATH_MAX 255

/* putting the tpc_txnset struct/typedef here
 * because of the fact that whatever tracks state needs
 * this status.
 */

/*
 * We use an enriched single linked list to model the
 * transaction set here.
 *
 * For public usage, tpc_phase may be used to check
 * the results of rollback or commit.  COMPLETE means
 * that the transaction set was completed and cleaned up.
 *
 * INCOMPLETE means that the transaction set left dangling
 * transactions in places that must be externally cleaned up
 * and we don't want to wait around for them on the server.
 *
 * logpath gives you the path to the log file and the log
 * file descriptor will be closed after this point.
 */

typedef struct tpc_txn {
   PGconn *conn;
   struct tpc_txn *next;
} tpc_txn;

typedef struct tpc_txnset {
    uint	counter;
    FILE       *log;
    tpc_phase	tpc_phase;
    tpc_txn    *head;
    tpc_txn    *latest;
    char	logpath[TPC_LOGPATH_MAX];
    char	txn_prefix[NAMEDATALEN];	/* overkill on size */
}	    tpc_txnset;


extern tpc_txnset *txnset;
extern void tpc_begin(void);
extern void tpc_register_cnx(PGconn * cnx);
extern void tpc_process_file(char *fname);
extern void tpc_txnset_register(PGconn * conn);
extern tpc_phase tpc_commit(void);
extern tpc_phase tpc_rollback(void);
#endif
