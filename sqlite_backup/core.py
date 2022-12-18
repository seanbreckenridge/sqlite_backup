import os
import errno
import sqlite3
import filecmp
import shutil
import warnings

from typing import (
    Union,
    Optional,
    List,
    Tuple,
    Dict,
    Any,
    Iterator,
    Callable,
)
from pathlib import Path
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from .log import logger


PathIsh = Union[str, Path]

CopyFunction = Callable[[str, str], bool]

COPY_RETRY_DEFAULT: int = 100


class SqliteBackupError(RuntimeError):
    """Generic error for the sqlite_backup module"""


@contextmanager
def sqlite_connect_immutable(db: PathIsh) -> Iterator[sqlite3.Connection]:
    # https://www.sqlite.org/draft/uri.html#uriimmutable
    conn: Union[sqlite3.Connection, None] = None
    try:
        with sqlite3.connect(f"file:{db}?immutable=1", uri=True) as conn:
            yield conn
    finally:
        if conn:
            conn.close()


# https://github.com/karlicoss/promnesia/blob/1b26ccdf9be7c0ac8f8e6e9e4193647450548878/scripts/browser_history.py#L48
def atomic_copy(src: str, dest: str) -> bool:
    """
    Copy from src to dest. If src changes while copying to dest, retry till it is the same
    These are very few ways to truly guarantee a file is copied atomically, so this is the closest approximation

    This retries till the file doesn't change while we were copying it

    If the file did change (before the final copy, which succeeded) while we were copying it, this returns False
    """
    # function-level succeeded value -- i.e., if while trying to copy
    # this this failed, it marks this as False. It still retries, but
    # this is to signify to copy_all_files that something changed while
    # we were copying, so we likely want to retry
    succeeded = True
    while True:
        shutil.copy(src, dest)
        if filecmp.cmp(src, dest, shallow=True):
            logger.debug(
                f"Copied from '{src}' to '{dest}' successfully; copied without file changing: {succeeded}"
            )
            # succeeded, return whether or not this failed on any loop iteration
            return succeeded
        else:
            logger.debug(f"Failed to copy from '{src}' to '{dest}', retrying...")
            succeeded = False


def glob_database_files(source_database: Path) -> List[Path]:
    """
    List any of the temporary database files (and the database itself)
    """
    files: List[Path] = [source_database]
    for temp_db_file in source_database.parent.glob(source_database.name + "-*"):
        # shm should be recreated from scratch -- safer not to copy perhaps
        # https://www.sqlite.org/tempfiles.html#shared_memory_files
        if temp_db_file.name.endswith("-shm"):
            continue
        files.append(temp_db_file)
    return files


def copy_all_files(
    source_files: List[Path],
    temporary_dest: Path,
    copy_function: CopyFunction,
    retry: int,
) -> bool:
    """
    Copy all files from source to directory
    This retries (up to 'retry' count) if any of the files change while any of the copies were copying

    Returns:
        True if it successfully copied and none of the files changing while it was copying
        False if it retied 'retry' times but files still changed as it was copying

    It still *has* copied the files, it just doesn't guarantee that the copies were atomic according to
    atomic_copy's definition of failure
    """
    if not temporary_dest.is_dir():
        raise ValueError(f"Expected a directory, received {temporary_dest}")
    sources = [str(s) for s in source_files]
    destinations = [str(temporary_dest / s.name) for s in source_files]
    # (source, destination) for each file
    logger.debug(f"Source database files: '{sources}'")
    logger.debug(f"Temporary Destination database files: '{destinations}'")
    copies: List[Tuple[str, str]] = list(zip(sources, destinations))
    while retry >= 0:
        # if all files successfully copied according to atomic_copy's definition
        if all([copy_function(s, d) for s, d in copies]):
            return True
        retry -= 1
        logger.debug(
            f"Failed to copy all files without at least one changing, retrying ({retry} left)"
        )
    return False


def sqlite_backup(
    source: PathIsh,
    destination: Optional[PathIsh] = None,
    *,
    wal_checkpoint: bool = True,
    copy_use_tempdir: bool = True,
    copy_retry: int = COPY_RETRY_DEFAULT,
    copy_retry_strict: bool = True,
    sqlite_connect_kwargs: Optional[Dict[str, Any]] = None,
    sqlite_backup_kwargs: Optional[Dict[str, Any]] = None,
    copy_function: Optional[CopyFunction] = None,
) -> Optional[sqlite3.Connection]:
    """
    'Snapshots' the source database and opens by making a deep copy of it, including journal/WAL files

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
    """
    source_path = Path(source)
    copy_from: Path

    if not source_path.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), str(source_path)
        )

    if destination is not None:
        if source_path == Path(destination):
            raise ValueError(
                f"'source' and 'destination' '{source_path}' can't be the same"
            )

    if sqlite_connect_kwargs is None:
        sqlite_connect_kwargs = {}

    if sqlite_backup_kwargs is None:
        sqlite_backup_kwargs = {}

    if copy_function is None:
        copy_function = atomic_copy

    with TemporaryDirectory() as td:
        # if we should copy files to the temporary dir
        # could use a nullcontext but is harder to read
        if copy_use_tempdir:
            tdir = Path(td)
            succeeded = copy_all_files(
                glob_database_files(source_path),
                temporary_dest=tdir,
                copy_function=copy_function,
                retry=copy_retry,
            )
            if not succeeded and copy_retry_strict:
                raise SqliteBackupError(
                    f"While in strict mode, this failed to copy all files without any of them changing {copy_retry} times. Increase 'copy_retry' or disable 'copy_retry_strict'"
                )
            copy_from = tdir / source_path.name
            assert (
                copy_from.exists()
            ), f"Expected copied database to exist at {copy_from} in temporary directory"
        else:
            copy_from = source_path
            warnings.warn(
                "Copying a database in use by another application without copying to a temporary directory could result in corrupt data or incorrect results. Only use this if you know the underlying database is not being modified"
            )

        target_connection: sqlite3.Connection
        if destination is None:
            logger.debug("No destination provided, copying data to ':memory:'")
            target_connection = sqlite3.connect(":memory:")
        else:
            if not isinstance(destination, (str, Path)):
                raise TypeError(
                    f"Unexpected 'destination' type, expected path like object, got {type(destination)}"
                )
            target_connection = sqlite3.connect(destination)

        logger.debug(
            f"Running backup, from '{copy_from}' to '{destination or 'memory'}'"
        )
        with sqlite3.connect(copy_from, **sqlite_connect_kwargs) as conn:
            conn.backup(target_connection, **sqlite_backup_kwargs)

        if destination is not None and wal_checkpoint:
            with target_connection:
                target_connection.execute("PRAGMA wal_checkpoint(TRUNCATE);")

        conn.close()

    # if there was no target, then we copied into memory
    # don't close so that the user has access to the memory
    # otherwise, the data is just copied and lost
    if destination is None:
        return target_connection
    else:
        target_connection.close()
        return None
