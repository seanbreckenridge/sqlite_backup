import shutil
import sqlite3
from pathlib import Path
from typing import Generator

import pytest

# make sure errors in threads get raised by pytest properly
# https://github.com/bjoluc/pytest-reraise
from pytest_reraise import Reraise  # type: ignore[import]

from sqlite_backup.core import (
    _sqlite_connect_immutable as sqlite_connect_immutable,
    sqlite_backup,
    atomic_copy,
    SqliteBackupError,
)

from . import run_in_thread


# tmp_path is a pytest provided fixture


@pytest.fixture()
def sqlite_with_wal(
    tmp_path: Path,
) -> Generator[Path, None, None]:
    """
    In a temporary directory, create a database with a basic table
    insert 5 items into it and let sqlite3.connection close the connection
    then, open in PRAGMA journal_mode=wal;, which writes to the temporary
    write-ahead log, instead of the main database

    This is similar to what applications may be doing while changes
    are uncommitted
    See https://sqlite.org/wal.html
    """
    db = tmp_path / "db.sqlite"
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


def test_do_copy(sqlite_with_wal: Path, tmp_path: Path, reraise: Reraise) -> None:
    """
    a copy of the database itself without the WAL can only read previously committed stuff
    """

    @reraise.wrap
    def _run() -> None:
        cdb = Path(tmp_path) / "dbcopy.sqlite"
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
    sqlite_with_wal: Path, reraise: Reraise, tmp_path: Path
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
        destination_database = tmp_path / "db.sqlite"
        conn = sqlite_backup(
            sqlite_with_wal, destination_database, wal_checkpoint=False
        )
        assert conn is None  # the database connection is closed

        with sqlite3.connect(destination_database) as dest_conn:
            assert len(list(dest_conn.execute("SELECT * FROM testtable"))) == 10
        dest_conn.close()

        # according to the docs, https://www.sqlite.org/walformat.html#file_lifecycles:
        #
        # If the last client using the database shuts down cleanly by calling
        # sqlite3_close(), then a checkpoint is run automatically in order to transfer
        # all information from the wal file over into the main database, and both the shm
        # file and the wal file are unlinked
        #
        # It does indeed seem to call 'sqlite3_close_v2'
        # https://github.com/python/cpython/blob/8fb36494501aad5b0c1d34311c9743c60bb9926c/Modules/_sqlite/connection.c#L340
        #
        # Perhaps; see https://www.sqlite.org/walformat.html
        # When a database connection closes (via sqlite3_close() or sqlite3_close_v2()),
        # an attempt is made to acquire SQLITE_LOCK_EXCLUSIVE
        #
        # Its not able to acquire a SQLITE_LOCK_EXCLUSIVE, so it doesn't truncate the WAL file?
        # However, below in test_backup_with_checkpoint wal_checkpoint(TRUNCATE) should **block** till that happens
        #
        # which reading some of the comments here, confirms the (incorrectly descrbied?) behaviour I see
        # https://github.com/groue/GRDB.swift/issues/418
        # https://github.com/groue/GRDB.swift/issues/739
        #
        # Also here, it seems that this issue was seemingly resolved by upgrading
        # versions, but no particular commit/bugfix pointed to
        # https://sqlite.org/forum/forumpost/1fdfc1a0e7
        #
        # It may depend on a compilation flag, https://www.sqlite.org/c3ref/c_dbconfig_enable_fkey.html
        # SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE (though that is disabled on my system)
        # so its not a good idea to depend on the
        # the fact that the -shm and -wal files "*wont*" be here:
        # https://github.com/groue/GRDB.swift/issues/771#issuecomment-624479526
        #
        # from a user perspective (me manually testing this on databases), running the
        # while wal_checkpoint=True, I've never seen the -wal/-shm files after
        # the python process ends, so perhaps sqlite or python C code is keeping
        # track of which databases have been connected to, and atexit they
        # respect this behaviour? But I can't reproduce it here

        # with all that in mind, lets just test against the current behaviour
        # so if it changes we know it has

        expected = {
            destination_database,
            Path(str(destination_database) + "-shm"),
            Path(str(destination_database) + "-wal"),
        }
        assert set(destination_database.parent.iterdir()) == expected

    run_in_thread(_run)


def test_backup_with_checkpoint(
    sqlite_with_wal: Path, reraise: Reraise, tmp_path: Path
) -> None:
    """
    Copy from the sqlite_with_wal to another database file, then
    run a WAL checkpoint to absorb any temporary files

    test this worked by opening it in immutable mode
    """

    @reraise.wrap
    def _run() -> None:
        destination_database = tmp_path / "db.sqlite"
        conn = sqlite_backup(sqlite_with_wal, destination_database, wal_checkpoint=True)
        assert conn is None  # the database connection is closed
        # should be able to read all data in immutable mode
        with sqlite_connect_immutable(destination_database) as dest_conn:
            assert len(list(dest_conn.execute("SELECT * FROM testtable"))) == 10
        dest_conn.close()

        # just like above, even if reading in immutable we still have -shm/-wal files here
        #
        # the theories I have are therefore:
        # 1) either connection.backup is copying the -shm/-wal files (which is understandable)
        #    and even running the wal_checkpoint(TRUNCATE) doesnt remove the files till after the process
        #    ends (why??)
        # 2) immutable is creating files, which shoud never be happening according to the docs:
        #    https://www.sqlite.org/uri.html#uriimmutable
        #
        # however, disregarding all the confusion above, I don't think this interferes with how a user
        # would use this. Even while opening in immutable (which should not pick up stuff from the -wal,
        # as tested by test_do_immutable above), this still returns all 10 rows, not just 5

        expected = {
            destination_database,
            Path(str(destination_database) + "-shm"),
            Path(str(destination_database) + "-wal"),
        }
        assert set(destination_database.parent.iterdir()) == expected

    run_in_thread(_run)


def test_backup_without_checkpoint(
    sqlite_with_wal: Path, reraise: Reraise, tmp_path: Path
) -> None:
    """
    similar to test_copy_vacuum, if backup is run without a wal_checkpoint,
    then connecting to the database with immutable=1 doesn't pick up anything from the -wal
    """

    @reraise.wrap
    def _run() -> None:
        destination_database = tmp_path / "db.sqlite"
        conn = sqlite_backup(
            sqlite_with_wal, destination_database, wal_checkpoint=False
        )
        assert conn is None  # the database connection is closed
        # this has 5 and not 10, since immutable reads nothing from the copied
        # -wal file which was not added to the main database file
        with sqlite_connect_immutable(destination_database) as imm:
            assert len(list(imm.execute("SELECT * FROM testtable"))) == 5
        imm.close()

        # however, opening it without immutable should still pick up data in the WAL
        with sqlite3.connect(destination_database) as reg_conn:
            assert len(list(reg_conn.execute("SELECT * FROM testtable"))) == 10
        reg_conn.close()

    run_in_thread(_run)


def test_database_doesnt_exist(tmp_path: Path, reraise: Reraise) -> None:
    """
    basic test to make sure sqlite_backup fails if db doesnt exist
    """

    db = str(tmp_path / "db.sqlite")

    def _run() -> None:
        with reraise:
            sqlite_backup(db)

    run_in_thread(_run, allow_unwrapped=True)

    err = reraise.reset()
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

    err = reraise.reset()
    assert isinstance(err, SqliteBackupError)
    assert (
        "this failed to copy all files without any of them changing 100 times"
        in str(err)
    )
