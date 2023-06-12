
/*
 * tpc_txnset.c
 * Maintainer: Chris Travers <chris.travers@gmail.com>
 *
 * This is the main file for global transaction management.
 *
 * Here a global transaction is modelled as a set of local
 * transactions that must be all committed or rolled back
 * in an all-or-nothing way.  Global transactions are 
 * eventually write consistent in the sense that they are
 * guaranteed to allow the commit or rollback of transactions
 * on all databases together, but not necessarily at the same
 * time.
 *
 * For example, a node may disappear between the PREPARE
 * and COMMIT PREPARED phases.  This transaction will be 
 * committed after the node returns, assuming it ever does,
 * and catastrophic node failure is outside the bounds 
 *
 * The current implementation uses files to store the 
 * global transaction data.  Future versions might use a
 * custom background worker to move the logging into database
 * tables and out of hte current transactions.
 *
 * As always the current implementation favors correctness
 * rather than performance.  It is easier to make correct
 * things fast, rather than make fast things correct.
 *
 * Global deadlock detection is outside of the current scope.
 * This may be better handled by a separate project and
 * component.
 */


#include "tpc_txnset.h"
#undef foreach
#define foreach(e, l) for ((e) = (l); (e); (e) = (e)->next)


void txn_cleanup(XactEvent event, void *arg);

/*
 * tpc_txn may change without notice
 */

typedef struct tpc_txn {
   PGconn *cnx;                 /* connection to use */
   struct tpc_txn *next;
} tpc_txn;


/* backend global variable curr_txnset
 * This points to the current open transaction set
 * allocated in the transaction memory context.
 *
 * On commit or rollback, this needs to be set to
 * NULL.
 *
 * Since one backend can only have one transaction open
 * at a time, this is effectively a singleton.
 */

tpc_txnset *curr_txnset = NULL;

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

tpc_txnset *
tpc_txnset_begin()
{
	/* errors are safe here since the transaction will be aborted */
	MemoryContext old_context = MemoryContextSwitch(CurTransactionContext);

	tpc_txnset *new_txnset;
	new_txnset = palloc0(sizeof(tpc_txnset));

	curr_txnset = new_txnset;

	/* not vulnerable to race since pid is part of file name in terms
	 * of actual duplicate detection but is vulnerable to reuse on busy
	 * systems....
	 */
	new_txnset->tpc_phase = BEGIN;
	tpc_txnfile_write_state(new_txnset, BEGIN);
	snprintf(new_txnset->txn_prefix, sizeof(new_txnset->txn_prefix),
		uuid_to_str(gen_uuid()));
	tpc_txnsetfile_begin(new_txnset, new_txnset->txn_prefix);
	new_txnset->counter = 0;
	new_txnset->head = NULL;
	new_txnset->latest = NULL;
	RegisterXactCallback(txn_cleanup, NULL);
	MemoryContextSwitch(old_context);
	return new_txnset;
	
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
    curr_txnset = NULL;
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

/*
 * void tpc_txnset_register(PGconn * conn)
 *
 * Registers the txnset with the current global txnset.  If there is no current
 * txnset, then one is created.
 */

void
tpc_txnset_register(PGConn * conn)
{
	tpc_txn *txn = palloc(sizeof(tpc_txn));
	txn->next = NULL;
	txn->conn = conn;
	if (NULL == txnset) {
		tpc_txnset_begin();
		txnset->head = txn;
		txnset->latest = txn;
	}
	} else {
		txnset->latest->next = txn;
		txnset->latest = txn;
	}
}
