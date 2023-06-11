
#include <libpq-fe.h>
#include <postgres.h>
#include "tpc_phase.h"

#define TPC_LOGPATH_MAX 255

extern tpc_txnset * tpc_begin(char *prefix);
extern void tpc_register_cnx(tpc_txnset *txnset, PGconn *cnx);
extern char * tpc_prepare(tpc_txnset *txnset);
extern tpc_phase tpc_commit(tpc_txnset *txnset);
extern tpc_phase tpc_rollback(tpc_txnset *txnset);
extern void tpc_process_file(char *fname);
