Module sqlite_backup
====================

Sub-modules
-----------
* sqlite_backup.core
* sqlite_backup.log

Functions
---------

    
`sqlite_backup(source: Union[str, pathlib.Path], destination: Union[str, pathlib.Path, ForwardRef(None)] = None, *, wal_checkpoint: bool = True, copy_use_tempdir: bool = True, copy_retry: int = 100, copy_retry_strict: bool = True, sqlite_connect_kwargs: Optional[Dict[str, Any]] = None, sqlite_backup_kwargs: Optional[Dict[str, Any]] = None, copy_function: Optional[Callable[[str, str], bool]] = None) ‑> Optional[sqlite3.Connection]`
:   'Snapshots' the source database and opens by making a deep copy of it, including journal/WAL files
    
    If you don't specify a 'destination', this copies the database
    into memory and returns an active connection to that.
    
    If you specify a 'destination', this copies the 'source' to the 'destination' file,
    instead of into memory
    
    If 'copy_use_tempdir' is True, this copies the relevant database files to a temporary directory,
    and then copies it into destination using sqlite3.Connection.backup. So, by default, the steps are:
    
    - Copy the source database files to a temporary directory
    - create a connection to the temporary database files
    - create a temporary 'destination' connection in memory
    - backup from the temporary directory database connection to the destination
    - cleanup; close temporary connection and remove temporary files
    - returns the 'destination' connection, which has the data stored in memory
    
    If you instead specify a path as the 'destination', this creates the
    database file there, and returns nothing (If you want access to the
    destination database, open a connection afterwards with sqlite3.connect)
    
    'wal_checkpoint' runs a 'PRAGMA wal_checkpoint(TRUNCATE)' after it writes to
    the destination database, which truncates the write ahead log to 0 bytes.
    Typically the WAL is removed when the database is closed, but particular builds of sqlite
    or sqlite compiled with SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE may prevent that --
    so the checkpoint exists to ensure there are no temporary files leftover
    
    See:
    https://sqlite.org/forum/forumpost/1fdfc1a0e7
    https://www.sqlite.org/c3ref/c_dbconfig_enable_fkey.html
    
    if 'copy_use_tempdir' is False, that skips the copy, which increases the chance that this fails
    (if theres a lock (SQLITE_BUSY, SQLITE_LOCKED)) on the source database,
    which is what we're trying to avoid in the first place
    
    'copy_retry' (default 100) specifies how many times we should attempt to copy the database files, if they
    happen to change while we're copying. If 'copy_retry_strict' is True, this throws an error if it failed
    to safely copy the database files 'copy_retry' times
    
    'sqlite_connect_kwargs' and 'sqlite_backup_kwargs' let you pass additional kwargs
    to the connect (when copying from the source database) and the backup (when copying
    from the source (or database in the tempdir) to the destination