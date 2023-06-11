
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

typedef struct tpc_txnset {
   char logpath[TPC_LOGPATH_MAX];
   FILE *log;
   char txn_prefix[NAMEDATALEN]; /* overkill on size */
   uint counter;
   tpc_phase tpc_phase;
   tpc_txn *head;
   tpc_txn *latest;
} tpc_txnset;


/*
 * tpc_txn may change without notice
 */

typedef struct tpc_txn {
   PGconn *cnx;                 /* connection to use */
   char txn_name[NAMEDATALEN];  /* transaction name  */
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
	MemoryContextSwitch(old_context);
	return new_txnset;
	
}



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
 * static void cleanuo()
 *
 * Closes all connections, for use
 * after the end of the transaction.
 */

static void
cleanup(void)
{
    struct conn *curr;

    if (!head)
        return;

    foreach (curr, head)
        PQfinish(curr->pg);

    head = NULL;
    conn = NULL;
    UnregisterXactCallback(txn_cleanup, NULL);
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
#if PG_VERSION_NUM >= 90500
        case XACT_EVENT_PARALLEL_COMMIT:
#endif
        case XACT_EVENT_COMMIT:
            ereport(WARNING,
                    (errmsg("%s", "you are committing a remote transaction implicitly.  This can cause problems.")));
/* fall through for cleanup */
#if PG_VERSION_NUM >= 90500
        case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif
        case XACT_EVENT_PRE_COMMIT:
            commit();
            cleanup();
            break;
#if PG_VERSION_NUM >= 90500
        case XACT_EVENT_PARALLEL_ABORT:
#endif
        case XACT_EVENT_ABORT:
            rollback();
            cleanup();
            break;
        default:
            /* ignore */
            break;
    }
}

