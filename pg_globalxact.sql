CREATE FUNCTION tpc_cleanup(text)
RETURNS VOID
LANGUAGE C STRICT
AS '$libdir/pg_globalxact', 'tpc_cleanup_txnset';
