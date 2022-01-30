import sqlite3
import filecmp
import shutil

from typing import Union, Optional, List, Iterable, Tuple, Dict, Any
from pathlib import Path
from tempfile import TemporaryDirectory

from atomicwrites import atomic_write


PathIsh = Union[str, Path]


def _sqlite_connect_immutable(db: PathIsh) -> sqlite3.Connection:
    # https://www.sqlite.org/draft/uri.html#uriimmutable
    return sqlite3.connect(f"file:{db}?immutable=1", uri=True)


# https://github.com/karlicoss/promnesia/blob/1b26ccdf9be7c0ac8f8e6e9e4193647450548878/scripts/browser_history.py#L48
def atomic_copy(src: str, dest: str) -> bool:
    """
    Copy from src to dest. If src changes while copying to dest, retry till it is the same
    These are very few ways to truly guarantee a file is copied atomically, so this is the closest approximation

    This retries till the file doesn't change while we were copying it

    If the file did change (before the final copy, which suceeded) while we were copying it, this returns False
    """
    failed = False
    while True:
        res = shutil.copy(src, dest)
        if filecmp.cmp(src, res):
            # failed, return whether or not this failed on any loop iteration
            return failed
        else:
            failed = True


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
    source_files: List[Path], temporary_dest: Path, retry: int = 100
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
        raise ValueError(f"Expected a directory, recieved {temporary_dest}")
    sources = [str(s) for s in source_files]
    destinations = [str(temporary_dest / s.name) for s in source_files]
    # (source, destination) for each file
    copies: Iterable[Tuple[str, str]] = zip(sources, destinations)
    while retry >= 0:
        # if all files successfully copied according to atomic_copy's definition
        if all(atomic_copy(s, d) for s, d in copies):
            return True
        retry -= 1
    return False


def sqlite_backup(
    source: PathIsh,
    destination: Optional[PathIsh] = None,
    *,
    copy_use_tempdir: bool = True,
    copy_retry: int = 100,
    copy_retry_strict: bool = False,
    sqlite_connect_kwargs: Optional[Dict[str, Any]] = None,
    sqlite_backup_kwargs: Optional[Dict[str, Any]] = None,
) -> Optional[sqlite3.Connection]:
    """
    'Snapshots' the source database and opens by making a deep copy of it, including journal/WAL files

    If you don't supply a destination, this copies the database into memory and returns an active connection to that
    If you want

    If 'copy_use_tempdir' is True, this copies the relevant database files to a temporary directory,
    and then copies it into destination using sqlite3.Connection.backup

    if 'copy_use_tempdir' is False, that skips the copy, which increases the chance that this fails
    (if theres a lock (SQLITE_BUSY, SQLITE_LOCKED)) on the source database,
    which is what we're trying to avoid in the first place

    'copy_retry' specifies how many times we should attempt to copy the database files, if they
    happen to change while we're doing so. 'copy_retry_strict' throws an error if it didn't happen
    to copy in 'copy_retry' times
    """
    source_path = Path(source)
    copy_from: Path

    if sqlite_connect_kwargs is None:
        sqlite_connect_kwargs = {}

    if sqlite_backup_kwargs is None:
        sqlite_backup_kwargs = {}

    with TemporaryDirectory() as td:
        # if we should copy files to the temporary dir
        # could use a nullcontext but is harder to read
        if copy_use_tempdir:
            tdir = Path(td)
            succeeded = copy_all_files(
                glob_database_files(source_path), temporary_dest=tdir, retry=copy_retry
            )
            if not succeeded and copy_retry_strict:
                raise RuntimeError(
                    f"While in strict mode, this failed to copy all files without any of them changing {copy_retry} times. Increase 'copy_retry' or disable 'copy_retry_strict'"
                )
            copy_from = tdir / source_path.name
            if not copy_from.exists():
                raise RuntimeError(
                    f"Expected copied database to exist at {copy_from} in temporary directory"
                )
        else:
            copy_from = source_path
            if not copy_from.exists():
                raise RuntimeError(f"Expected source database to exist at {copy_from}")

        target_connection: sqlite3.Connection
        if destination is None:
            target_connection = sqlite3.connect(":memory:")
        else:
            assert isinstance(
                destination, (str, Path)
            ), f"Unexpected database type, expected path like object, got {type(destination)}"
            target_connection = sqlite3.connect(destination)

        with sqlite3.connect(copy_from, **sqlite_connect_kwargs) as conn:
            conn.backup(target_connection, **sqlite_backup_kwargs)

        # if there was no target, then we copied into memory
        # dont close so that the user has access to the memory
        # otherwise, the data is just copied and lost
        if destination is None:
            return target_connection
        else:
            # destination was a file -- close
            target_connection.close()
            return None
