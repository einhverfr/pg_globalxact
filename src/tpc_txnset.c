#include "tpc_txnset.h"


/* 
 * tpc_txnset for local connections is initialized to NULL at first.
 */

tpc_txnset * txnset = NULL;

/*
 * void tpc_txnset_register(PGconn * conn)
 *
 * Registers the txnset with the current global txnset.  If there is no current
 * txnset, then one is created.
 */

void
tpc_txnset_register(PGconn * conn)
{
	tpc_txn *txn = palloc0(sizeof(tpc_txn));
	txn->next = NULL;
	txn->conn = conn;
	if (NULL == txnset) {
		tpc_begin();
		txnset->head = txn;
		txnset->latest = txn;
	}
	} else {
		txnset->latest->next = txn;
		txnset->latest = txn;
	}
}

/* 
 * creates a new tpttxn if needed.
 */


/* In order to support PostgreSQL 12-13, copying in general code from 
 * PostgreSQL's source.  These probably need to be included longer-term
 * because the ones in /src/backend/utils/adt/uuid.c are built exclusively
 * for use in SQL.  So we copy here.  And of course mark them static and modify
 * for use in C.
 */

static inline *pg_uuid_t
gen_uuid()
{
	pg_uuid_t  *uuid = palloc(UUID_LEN);

	if (!pg_strong_random(uuid, UUID_LEN))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate random values")));

	/*
	 * Set magic numbers for a "version 4" (pseudorandom) UUID, see
	 * http://tools.ietf.org/html/rfc4122#section-4.4
	 */
	uuid->data[6] = (uuid->data[6] & 0x0f) | 0x40;	/* time_hi_and_version */
	uuid->data[8] = (uuid->data[8] & 0x3f) | 0x80;	/* clock_seq_hi_and_reserved */
	return uuid;
}

static inline *char
uuid_to_str(pg_uuid_t *uuid)
{
	static const char hex_chars[] = "0123456789abcdef";
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	for (i = 0; i < UUID_LEN; i++)
	{
		int			hi;
		int			lo;

		/*
		 * We print uuid values as a string of 8, 4, 4, 4, and then 12
		 * hexadecimal characters, with each group is separated by a hyphen
		 * ("-"). Therefore, add the hyphens at the appropriate places here.
		 */
		if (i == 4 || i == 6 || i == 8 || i == 10)
			appendStringInfoChar(&buf, '-');

		hi = uuid->data[i] >> 4;
		lo = uuid->data[i] & 0x0F;

		appendStringInfoChar(&buf, hex_chars[hi]);
		appendStringInfoChar(&buf, hex_chars[lo]);
	}
	return buf.data;
}

/* Initializes a txnset and registers it for commit within the current
 * transaction semantics.  Will refuse to do so if one already exists for the
 * current transaction.  Here we use the transaction memory context for the
 * allocations.
 *
 * The description (txn_prefix) is set to a UUID.
 */

tpc_txnset *
tpc_begin() {
    tpc_txnset txnset = palloc0(sizeof tpc_txnset);
    snprintf(txnset->txn_prefix, sizeof(txnset->txn_prefix),
                uuid_to_str(gen_uuid()));
}

/* 
 * static void cleanup()
 * Deregisters the callback.
 *
 * Earlier versions also closed all connections
 * but that is wasteful.
 */

static void
cleanup(void)
{

    UnregisterXactCallback(txn_cleanup, NULL);
    txnset = NULL;
}


/* 
 * static void rollback()
 *
 * For the current transaction set, rolls all remote connections back
 * or at least tries to.  If the connection goes away and we have not
 * prepared the transaction, we don't need to worry about things because
 * the transaction will have disappeared too.
 */

static void
rollback(void)
{
    struct conn *curr;

    if (!head)
        return;
    if (txnset)
        tpc_rollback(txnset);
    foreach (curr, head)
        execorerr(curr, "ROLLBACK;", false);
    txnset = NULL;
}

/*
 * Commits a transaction by name on a connection
 *
 * After writes status (committed or error) as action in pending transaction 
 * log.
 *
 * Records our error state for complete run.
 */

static tpc_phase
tpc_commit(tpc_txnset *txnset)
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
static tpc_phase
tpc_rollback(tpc_txnset *txnset)
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

/* statuc void txn_ceanup(XactEvent event, void *arg)
 *
 * This is the primary event handler for commit and
 * rollback.  It hides the tpc semantics behind those of
 * the local transactional semantics.
 */


static void
txn_cleanup(XactEvent event, void *arg)
{
    switch (event)
    {
        case XACT_EVENT_PREPARE:
        case XACT_EVENT_PRE_PREPARE:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Two phase commit not supported yet")));
            break;
        case XACT_EVENT_PARALLEL_COMMIT:
        case XACT_EVENT_COMMIT:
	    /* The problem is that if something goes wrong, here it is too late
	     * roll back.  Consequently this warning is because it is not safe.
	     */
            ereport(WARNING,
                    (errmsg("%s", "you are committing a remote transaction implicitly.  This can cause problems.")));
/* fall through for cleanup */
        case XACT_EVENT_PARALLEL_PRE_COMMIT:
        case XACT_EVENT_PRE_COMMIT:
            tpc_commit();
            cleanup();
            break;
        case XACT_EVENT_PARALLEL_ABORT:
        case XACT_EVENT_ABORT:
            tpc_rollback();
            cleanup();
            break;
        default:
            /* ignore */
            break;
    }
}
