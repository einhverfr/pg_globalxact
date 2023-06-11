NAME: 
    pglobalxact extension

SYNOPSIS:

DESCRIPTION:

This extension provides services for doing destributed transactions within
PostgreSQL and administrative tools for managing the review and recovery 
of distributed transactions when machines die between PREPARE and
COMMIT/ROLLBACK stages.

For this to work, initial remote database connections must be registered
into a transaction set.  These sets are then stored in text files on disk
because they must persist beyond a rollback or even, in some cases, a 
database restart.

The module has two classes of functions:  c functions intended to be used
by other extensions and SQL functions intended to be used by DBAs to view
status of in-flight global transactions and engaging in various recovery
functions.

This extension manages global transactions as "transaction sets" of local 
transactions on the running nodes.  Right now, only PostgreSQL remote ends
are supported though this could change over time.

The intended use is for C functions to be used to register remote
transactions into the global transaction set, and then for local transaction
becomes the control mechanism for the remote transactions.  In terms of
implementation, this means that the remote transactions go through a full
two phase commit process as the local transaction commits or rolls back.

Recovery is guaranteed through transaction state files and a background 
worker whose job it is to retry COMMIT PREPARED or ROLLBACK PREPARED
calls in order to bring things back into consistent states.

The SQL functions are intended to provide administrator access to the
reconciliation etc process as well as allow human intervention when a 
remote node undergoes catastrophic failure and will not come back.

This library does not attempt to provide any assistance in global 
transaction deadlock detectoin or resolution.  Nor does it provide any
assistance on protecting against distributed read anomilies.

C FUNCTIONS

SQL FUNCTIONS

INTERNALS

DESIGN CHOICES

COPYRIGHT
