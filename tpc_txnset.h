#ifndef TPC_TXNSET_H

#define TPC_TXNSET_H

#include <libpq-fe.h>
#include <postgres.h>
#include "tpc_phase.h"
#include <access/xact.h>

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

typedef struct tpc_txn tpc_txn;

typedef struct tpc_txnset {
   char logpath[TPC_LOGPATH_MAX];
   FILE *log;
   char txn_prefix[NAMEDATALEN]; /* overkill on size */
   uint counter;
   tpc_phase tpc_phase;
   tpc_txn *head;
   tpc_txn *latest;
} tpc_txnset;


extern tpc_txnset * tpc_begin(char *prefix);
extern void tpc_register_cnx(PGconn *cnx);
extern void tpc_process_file(char *fname);
#endif
