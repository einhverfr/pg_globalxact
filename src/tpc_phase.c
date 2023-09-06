/*
 * tpc_phase.c
 * maintainer: Chris Travers <chris.travers@gmail.com>
 *
 * This file models the state management for two phase commit
 * transaction sets (global write transactions).
 *
 * This allows you to take phase states to labels and vice versa
 * and also check validation of phase states.
 */


#include "tpc_phase.h"

#define true 1
#define false 0

static char phase_labels[6][10] = {
    "begin",
    "prepare",
    "commit",
    "rollback",
    "complete",
    "incomplete"
};

static const tpc_phase phases[] = {
    BEGIN,
    PREPARE,
    COMMIT,
    ROLLBACK,
    COMPLETE,
    INCOMPLETE
};


/*
 * tpc_phase tpc_phase_from_label(const char *label)
 * Returns the tpc_phase from the corresponding string using
 * a lookup table.
 *
 * If the phase is not found, an error is thrown.
 */

tpc_phase
tpc_phase_from_label(const char *label) {
    for (int i = 0; i < 7; ++i) {
	if (strcmp(label, phase_labels[i]) == 0) {
	    return phases[i];
	}
    }
    ereport(ERROR, (errcode(ERRCODE_INVALID_TRANSACTION_STATE), errmsg("invalid txn phase %s",
		label)));
}

/*
 * static char * label_from_phase(tpc_phase phase)
 * Returns the label used for the file for the corresponding
 * phase via a lookup table. This is not intended to be used
 * outside this file, hence the static designation.
 */
char	   *
tpc_phase_get_label(tpc_phase phase)
{
    for (int i = 0; i < 7; ++i) {
	if (phases[i] == phase)
	    return phase_labels[i];
    }
    return NULL; // should never happen
}


/*
 * int tpc_phase_is_valid_transition(tpc_phase old_phase, tpc_phase new_phase)
 * This function returns true (1) if the phase transition is valid and false
 * (0) if not.
 *
 * Note that because no current phase can transition to BEGIN, checking for
 * BEGIN is not supported here.  Instead this should be set on initialization
 * without checking.  All other state transitions should be checked, however.
 */
int
tpc_phase_is_valid_transition(tpc_phase old_phase, tpc_phase new_phase)
{
    switch (old_phase) {
    case BEGIN:
	if (PREPARE == new_phase)
	    return true;
	else
	    return false;
    case PREPARE:
	if (COMMIT == new_phase || ROLLBACK == new_phase)
	    return true;
	else
	    return false;
    case COMMIT:
    case ROLLBACK:
	if (COMPLETE == new_phase || INCOMPLETE == new_phase)
	    return true;
	else
	    return false;
    case INCOMPLETE:
	if (COMPLETE == new_phase)
	    return true;
	else
	    return false;
    default:
	return false;
    }
}
