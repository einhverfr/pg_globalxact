#include "tpc_txnset.h"
#include <utils/uuid.h>

#undef foreach
#define foreach(e, l) for ((e) = (l); (e); (e) = (e)->next)


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
	/* errors are safe here since the transaction will be aborted */
	MemoryContext old_context = MemoryContextSwitchTo(CurTransactionContext);

	tpc_txn *txn = palloc0(sizeof(tpc_txn));
	txn->next = NULL;
	txn->conn = conn;
	if (NULL == txnset) {
		tpc_begin();
		txnset->head = txn;
		txnset->latest = txn;
	} else {
		txnset->latest->next = txn;
		txnset->latest = txn;
	}
	RegisterXactCallback(txn_cleanup, NULL);
	MemoryContextSwitchTo(old_context);
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

static inline pg_uuid_t *
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

static inline char *
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

void
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

void
rollback(void)
{
    struct conn *curr;
    struct conn *head = txnset->head;


    if (NULL == head)
        return;
    if (txnset)
        tpc_rollback(txnset);
    foreach (curr, head)
        execorerr(curr, "ROLLBACK;", false);
    txnset = NULL;
}

