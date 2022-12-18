import shutil
import sqlite3
from pathlib import Path
from typing import Iterator, Any, Set

import pytest

# make sure errors in threads get raised by pytest properly
# https://github.com/bjoluc/pytest-reraise
from pytest_reraise import Reraise  # type: ignore[import]

from sqlite_backup.core import (
    sqlite_connect_immutable,
    sqlite_backup,
    atomic_copy,
    SqliteBackupError,
)

from . import run_in_thread


# tmp_path is test-scoped, not function-scoped


@pytest.fixture(scope="function")
def tmp_path_f(
    request: Any, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Path]:
    """
    Create a new tempdir every time this runs
    """
    # request is a _pytest.fixture.SubRequest, function that called this
    assert isinstance(request.function.__name__, str), str(request)
    assert request.function.__name__.strip(), str(request)
    yield tmp_path_factory.mktemp(request.function.__name__, numbered=True)


def _list_files(p: Path) -> Set[Path]:
    return {f for f in p.iterdir() if f.is_file()}


@pytest.fixture()
def sqlite_with_wal(
    tmp_path_f: Path,
) -> Iterator[Path]:
    """
    In a temporary directory, create a database with a basic table
    insert 5 items into it and let sqlite3.connection close the connection
    then, open in PRAGMA journal_mode=wal;, which writes to the temporary
    write-ahead log, instead of the main database

    This is similar to what applications may be doing while changes
    are uncommitted
    See https://sqlite.org/wal.html
    """
    db = tmp_path_f / "sqlite_with_wal" / "db.sqlite"
    db.parent.mkdir(exist_ok=False)
    # write a bit
    with sqlite3.connect(str(db)) as conn:
        conn.execute("CREATE TABLE testtable (col)")
        for i in range(5):
            conn.execute("INSERT INTO testtable (col) VALUES (?)", str(i))

    # write more in WAL mode
    with sqlite3.connect(str(db)) as conn_db:
        conn.execute("PRAGMA journal_mode=wal;")
        for i in range(5, 10):
            conn_db.execute("INSERT INTO testtable (col) VALUES (?)", str(i))
        conn_db.execute("COMMIT")

        # make sure it has unflushed stuff in wal
        wals = list(db.parent.glob("*-wal"))
        assert len(wals) == 1

        # make sure -wal file is not empty
        assert wals[0].stat().st_size > 0

        yield db

    conn.close()
    conn_db.close()


def test_open_asis(sqlite_with_wal: Path, reraise: Reraise) -> None:
    """
    This works, but leaves potential for DB corruption since we have
    multiple connetions to the same database table on different threads
    """

    @reraise.wrap
    def _run() -> None:
        with sqlite3.connect(sqlite_with_wal) as conn:
            assert len(list(conn.execute("SELECT * FROM testtable"))) == 10

    run_in_thread(_run)


def test_do_copy(sqlite_with_wal: Path, tmp_path_f: Path, reraise: Reraise) -> None:
    """
    a copy of the database itself without the WAL can only read previously committed stuff
    """

    @reraise.wrap
    def _run() -> None:
        cdb = Path(tmp_path_f) / "dbcopy.sqlite"
        shutil.copy(sqlite_with_wal, cdb)
        with sqlite3.connect(cdb) as conn_copy:
            assert len(list(conn_copy.execute("SELECT * FROM testtable"))) == 5

    run_in_thread(_run)


# https://www.sqlite.org/c3ref/open.html
# When immutable is set, SQLite assumes that the database file cannot be
# changed, even by a process with higher privilege, and so the database is opened
# read-only and all locking and change detection is disabled. Caution: Setting
# the immutable property on a database file that does in fact change can result
# in incorrect query results and/or SQLITE_CORRUPT errors. See also:
# SQLITE_IOCAP_IMMUTABLE.
def test_do_immutable(sqlite_with_wal: Path, reraise: Reraise) -> None:
    """
    a copy of the database in immutable mode

    *IF* the application changed this query was executing, this has the oppurtunity
    to corrupt or fetch incorrect results -- this only works because
    we control sqlite_with_wal and know its not going to change

    this also doesn't read anything from the WAL -- only the database
    """

    @reraise.wrap
    def _run() -> None:
        with sqlite_connect_immutable(sqlite_with_wal) as conn_imm:
            assert len(list(conn_imm.execute("SELECT * FROM testtable"))) == 5

    run_in_thread(_run)


def test_no_copy_use_tempdir(sqlite_with_wal: Path, reraise: Reraise) -> None:
    """
    similarly, we could use sqlite_backup without copy_use_tempdir
    which would mean this would run Connection.backup directly
    on the live database. this should work in this case, because
    we know its not changing, but this is prone to data loss
    """

    @reraise.wrap
    def _run() -> None:
        with pytest.warns(UserWarning) as record:
            conn = sqlite_backup(sqlite_with_wal, copy_use_tempdir=False)

        # make sure this warned the user
        assert len(record) == 1
        warning = record[0]
        assert (
            "Copying a database in use by another application without copying to a temporary directory"
            in str(warning.message)
        )

        assert conn is not None
        assert len(list(conn.execute("SELECT * from testtable"))) == 10

    run_in_thread(_run)


def test_do_copy_and_open(sqlite_with_wal: Path, reraise: Reraise) -> None:
    """
    main usage of the sqlite_backup function, this copies all database files
    to a temporary directory, and then reads it into memory using
    pythons Connection.backup
    """

    @reraise.wrap
    def _run() -> None:
        conn = sqlite_backup(sqlite_with_wal)  # copy to memory
        assert conn is not None
        assert len(list(conn.execute("SELECT * FROM testtable"))) == 10
        conn.close()

    run_in_thread(_run)


def test_copy_to_another_file(
    sqlite_with_wal: Path, reraise: Reraise, tmp_path_f: Path
) -> None:
    """
    Copy from the sqlite_with_wal to another database file -- this
    is pretty similar to test_do_copy_and_open, it just doesn't copy to memory

    We can then open the copied database to ensure it has all 10 records

    sqlite_with_wal -> temporary directory ->
    temp_database.backup(other_database) -> sqlite3.connect(other_database)
    """

    @reraise.wrap
    def _run() -> None:
        destination_database = tmp_path_f / "db.sqlite"
        conn = sqlite_backup(
            sqlite_with_wal, destination_database, wal_checkpoint=False
        )
        assert conn is None  # the database connection is closed

        with sqlite3.connect(destination_database) as dest_conn:
            assert len(list(dest_conn.execute("SELECT * FROM testtable"))) == 10
        dest_conn.close()

        # make sure no -wal/-shm files exist
        assert _list_files(tmp_path_f) == {destination_database}

    run_in_thread(_run)


def test_backup_with_checkpoint(
    sqlite_with_wal: Path, reraise: Reraise, tmp_path_f: Path
) -> None:
    """
    Copy from the sqlite_with_wal to another database file
    run a wal_checkpoint to make sure that works

    test this worked by opening it in immutable mode
    """

    @reraise.wrap
    def _run() -> None:
        destination_database = tmp_path_f / "db.sqlite"
        conn = sqlite_backup(sqlite_with_wal, destination_database, wal_checkpoint=True)
        assert conn is None  # the database connection is closed

        # no -wal/-shm files exist after the connection.backup
        assert _list_files(tmp_path_f) == {destination_database}

        # should be able to read all data in immutable mode
        with sqlite_connect_immutable(destination_database) as dest_conn:
            assert len(list(dest_conn.execute("SELECT * FROM testtable"))) == 10
        dest_conn.close()

        # no -wal/-shm files exist after opening in immutable, since
        # it acts as if the data is on a read-only volume
        assert _list_files(tmp_path_f) == {destination_database}

    run_in_thread(_run)


def test_backup_without_checkpoint(
    sqlite_with_wal: Path, reraise: Reraise, tmp_path_f: Path
) -> None:
    """
    similar to test_copy_vacuum, if backup is run without a wal_checkpoint,
    then connecting to the database with immutable=1 doesn't pick up anything from the -wal
    """

    @reraise.wrap
    def _run() -> None:
        destination_database = tmp_path_f / "db.sqlite"
        conn = sqlite_backup(
            sqlite_with_wal, destination_database, wal_checkpoint=False
        )
        assert conn is None  # the database connection is closed

        # even without a checkpoint, should be no WAL in destination
        assert _list_files(tmp_path_f) == {destination_database}

        # both immutable and regular connections should read all the data
        with sqlite_connect_immutable(destination_database) as imm:
            assert len(list(imm.execute("SELECT * FROM testtable"))) == 10
        imm.close()

        with sqlite3.connect(destination_database) as reg_conn:
            assert len(list(reg_conn.execute("SELECT * FROM testtable"))) == 10
        reg_conn.close()

        # no -wal/-shm files should exist, after closing the reg_conn connection
        assert _list_files(tmp_path_f) == {destination_database}

    run_in_thread(_run)


def test_database_doesnt_exist(tmp_path_f: Path, reraise: Reraise) -> None:
    """
    basic test to make sure sqlite_backup fails if db doesnt exist
    """

    db = str(tmp_path_f / "db.sqlite")

    def _run() -> None:
        with reraise:
            sqlite_backup(db)

    run_in_thread(_run, allow_unwrapped=True)

    err = reraise.reset()  # type: ignore
    assert isinstance(err, FileNotFoundError)
    assert err.filename == db
    assert "No such file or directory" in err.strerror


def test_copy_retry_strict(sqlite_with_wal: Path, reraise: Reraise) -> None:
    """
    Test copy_retry_strict, e.g., if the file is constantly being written to and
    this fails to copy, this should raise an error
    """

    def _run() -> None:
        def atomic_copy_failed(src: str, dest: str) -> bool:
            atomic_copy(src, dest)
            return False

        with reraise:
            sqlite_backup(
                sqlite_with_wal,
                copy_retry_strict=True,
                copy_function=atomic_copy_failed,
            )

    run_in_thread(_run, allow_unwrapped=True)

    err = reraise.reset()  # type: ignore
    assert isinstance(err, SqliteBackupError)
    assert (
        "this failed to copy all files without any of them changing 100 times"
        in str(err)
    )


def test_copy_different_source_and_dest(
    sqlite_with_wal: Path, reraise: Reraise
) -> None:
    """
    test to make sure if source == destination, that throws an error
    """

    def _run() -> None:
        with reraise:
            sqlite_backup(sqlite_with_wal, sqlite_with_wal)

    run_in_thread(_run, allow_unwrapped=True)

    err = reraise.reset()  # type: ignore
    assert isinstance(err, ValueError)
    assert "'source' and 'destination'" in str(err)
    assert "can't be the same" in str(err)
