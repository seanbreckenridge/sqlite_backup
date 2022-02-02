# sqlite_backup

This exposes the python stdlib [`sqlite.backup`](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup) function as a library, with a couple extra steps.

The main purpose for writing this is to copy sqlite databases that you may not own -- perhaps it belongs to an application (your browser) and is locked since that's currently open, or the OS keeps it open while the computer is active (e.g. Mac with iMessage)

### Features

- Has the option (true by default) to first safely copy the database from disk to a temporary directory, which is:
  - useful in case the source is in read-only mode (e.g. in some sort of docker container)
  - safer if you're especially worried about corrupting or losing data
- Uses [`Cpython`s Connection.backup](https://github.com/python/cpython/blob/8fb36494501aad5b0c1d34311c9743c60bb9926c/Modules/_sqlite/connection.c#L1716), which directly uses the [underlying Sqlite C code](https://www.sqlite.org/c3ref/backup_finish.html)
- Performs a [`wal_checkpoint`](https://www.sqlite.org/pragma.html#pragma_wal_checkpoint) after copying to the destination, to remove the WAL (write-ahead log; temporary database file). Typically the WAL is removed when the database is closed, but [particular builds of sqlite](https://sqlite.org/forum/forumpost/1fdfc1a0e7) or sqlite compiled with [`SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE` enabled](https://www.sqlite.org/c3ref/c_dbconfig_enable_fkey.html) may prevent that -- so the checkpoint exists to ensure there are no temporary files leftover

In short, this **prioritizes safety of the data** over performance (temporarily copying data files to `/tmp`) - because we often don't know what the application may be doing while we're copying underlying sqlite databases

The initial backup function and some tests were extracted out of the [`karlicoss/HPI` `core/sqlite`](https://github.com/karlicoss/HPI/blob/a1f03f9c028df9d1898de2cc14f1df4fa6d8c471/my/core/sqlite.py#L33-L51) module

If other tools exist to do this, please [let me know!](https://github.com/seanbreckenridge/sqlite_backup/issues/new)

## Installation

Requires `python3.7+`

To install with pip, run:

    pip install sqlite_backup

## Usage

```
Usage: sqlite_backup [OPTIONS] SOURCE_DATABASE DESTINATION

  SOURCE_DATABASE is the database to copy

  DESTINATION is where to write the database. If a directory, uses
  the SOURCE_DATABASE name. If a file, the directory must exist,
  and the destination file must not already exist (to prevent
  possibly overwriting old data)

Options:
  --debug                         Increase log verbosity  [default: False]
  --wal-checkpoint / --no-wal-checkpoint
                                  After writing to the destination, run a
                                  checkpoint to truncate the WAL to zero bytes
                                  [default: wal-checkpoint]
  --copy-use-tempdir / --no-copy-use-tempdir
                                  Copy the source database files to a
                                  temporary directory, then connect to the
                                  copied files  [default: copy-use-tempdir]
  --copy-retry INTEGER            If the files change while copying to the
                                  temporary directory, retry <n> times
                                  [default: 100]
  --copy-retry-strict / --no-copy-retry-strict
                                  Throws an error if this fails to safely copy
                                  the database files --copy-retry times
                                  [default: copy-retry-strict]
  --help                          Show this message and exit.  [default:
                                  False]
```

For usage in python, use the `sqlite_backup` function, see the [docs](./docs/sqlite_backup/core.md)

If you plan on reading from these backed up databases (and you're not planning on modifying these at all), I would recommend using the [`mode=ro`](https://www.sqlite.org/uri.html#urimode) (readonly) or [`immutable` flag](https://www.sqlite.org/uri.html#uriimmutable) flags when connecting to the database. In python, like:

```python
import sqlite3
from typing import Iterator

def sqlite_connect(database: str) -> Iterator[sqlite3.Connection]:
    try:
        # or for immutable, f"file:{database}?immutable=1"
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as conn:
            yield conn
    finally:
        conn.close()

with sqlite_connect("/path/to/database") as conn:
    conn.execute("...")
```

### Example

```
sqlite_backup --debug ~/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite ./firefox.sqlite
[D 220202 13:00:32 core:110] Source database files: '['/home/sean/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite', '/home/sean/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite-wal']'
[D 220202 13:00:32 core:111] Temporary Destination database files: '['/tmp/tmpm2nhl1p3/places.sqlite', '/tmp/tmpm2nhl1p3/places.sqlite-wal']'
[D 220202 13:00:32 core:64] Copied from '/home/sean/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite' to '/tmp/tmpm2nhl1p3/places.sqlite' successfully; copied without file changing: True
[D 220202 13:00:32 core:64] Copied from '/home/sean/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite-wal' to '/tmp/tmpm2nhl1p3/places.sqlite-wal' successfully; copied without file changing: True
[D 220202 13:00:32 core:240] Running backup, from '/tmp/tmpm2nhl1p3/places.sqlite' to '/home/sean/Repos/sqlite_backup/firefox.sqlite'
Backed up /home/sean/.mozilla/firefox/ew9cqpqe.dev-edition-default/places.sqlite to /home/sean/Repos/sqlite_backup/firefox.sqlite
```

### Tests

```bash
git clone 'https://github.com/seanbreckenridge/sqlite_backup'
cd ./sqlite_backup
pip install '.[testing]'
mypy ./sqlite_backup
pytest
```
