Module sqlite_backup.core
=========================

Functions
---------

    
`atomic_copy(src: str, dest: str) ‑> bool`
:   Copy from src to dest. If src changes while copying to dest, retry till it is the same
    These are very few ways to truly guarantee a file is copied atomically, so this is the closest approximation
    
    This retries till the file doesn't change while we were copying it
    
    If the file did change (before the final copy, which succeeded) while we were copying it, this returns False

    
`copy_all_files(source_files: List[pathlib.Path], temporary_dest: pathlib.Path, retry: int = 100) ‑> bool`
:   Copy all files from source to directory
    This retries (up to 'retry' count) if any of the files change while any of the copies were copying
    
    Returns:
        True if it successfully copied and none of the files changing while it was copying
        False if it retied 'retry' times but files still changed as it was copying
    
    It still *has* copied the files, it just doesn't guarantee that the copies were atomic according to
    atomic_copy's definition of failure

    
`glob_database_files(source_database: pathlib.Path) ‑> List[pathlib.Path]`
:   List any of the temporary database files (and the database itself)

    
`sqlite_backup(source: Union[str, pathlib.Path], destination: Union[str, pathlib.Path, ForwardRef(None)] = None, *, copy_use_tempdir: bool = True, copy_retry: int = 100, copy_retry_strict: bool = False, sqlite_connect_kwargs: Optional[Dict[str, Any]] = None, sqlite_backup_kwargs: Optional[Dict[str, Any]] = None) ‑> Optional[sqlite3.Connection]`
:   'Snapshots' the source database and opens by making a deep copy of it, including journal/WAL files
    
    If you don't specify a 'destination', this copies the database
    into memory and returns an active connection to that. i.e.:,
    
    source sqlite database file with possible extra -wal files
        -> copy to temporary directory
        -> sqlite3.connect(temporary directory/db) ->
        -> temp_database_connection.backup(destination)
    
    If you specify a 'destination', this copies the 'source' to the 'destination' file, instead of
    into memory
    
    If 'copy_use_tempdir' is True, this copies the relevant database files to a temporary directory,
    and then copies it into destination using sqlite3.Connection.backup
    
    if 'copy_use_tempdir' is False, that skips the copy, which increases the chance that this fails
    (if theres a lock (SQLITE_BUSY, SQLITE_LOCKED)) on the source database,
    which is what we're trying to avoid in the first place
    
    'copy_retry' (default 100) specifies how many times we should attempt to copy the database files, if they
    happen to change while we're copying. If 'copy_retry_strict' is True, this throws an error if it failed
    to safely copy the database files 'copy_retry' times
    
    'sqlite_connect_kwargs' and 'sqlite_backup_kwargs' let you pass additional kwargs
    to the connect (when copying from the source database) and the backup (when copying
    from the source (or database in the tempdir) to the destination

Classes
-------

`SqliteBackupError(*args, **kwargs)`
:   Generic error for the sqlite_backup module

    ### Ancestors (in MRO)

    * builtins.RuntimeError
    * builtins.Exception
    * builtins.BaseException