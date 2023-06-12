#ifndef TPC_PHASE_H

#define TPC_PHASE_H
#include <libpq-fe.h>
#include <postgres.h>

/*
 * The state pipeline is fairly simple.
 *
 * BEGIN -> PREPARE -> (COMMIT | ROLLBACK) -> (COMPLETE | INCOMPLETE)
 * 
 * BEGIN:  We have declared we want to create a two-phase commit set
 *         But have not added transactions to it.
 *
 * PREPARE:  We are asking remote connections to prepare commits
 *
 * COMMIT: We have completed the prepare commands and are committing all.
 *
 * ROLLBACK:  We are rolling back all.
 *
 * COMPLETE:  We have successfully committed or rolled back ALL transactions
 *
 * INCOMPLETE:  We were unable to commit or roll back ALL transactions.
 *              EXTERNAL INTERVENTION IS REQUIRED FOR INCOMLETE TPC SETS
 */
typedef enum {
  BEGIN,
  PREPARE,
  COMMIT,
  ROLLBACK,
  COMPLETE,
  INCOMPLETE
} tpc_phase;

extern char * tpc_phase_get_label(tpc_phase phase);
extern tpc_phase tpc_phase_from_label(const char *label);
extern int tpc_phase_is_valid_transition(tpc_phase old_phase, tpc_phase new_phase);

#endif
