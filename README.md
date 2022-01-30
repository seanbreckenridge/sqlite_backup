## WORK IN PROGRESS

The [core](./sqlite_backup/core.py) here is my first attempt at a solution of this, combining ideas from [browser_history.py](https://github.com/karlicoss/promnesia/blob/0e1e9a1ccd1f07b2a64336c18c7f41ca24fcbcd4/scripts/browser_history.py) and [`karlicoss/HPI/sqlite.py`](https://github.com/karlicoss/HPI/blob/a1f03f9c028df9d1898de2cc14f1df4fa6d8c471/my/core/sqlite.py#L33-L51) to create a library/CLI tool to (as safely as possible) copy databases which may be in use from other applications

# sqlite_backup

This exposes the python stdlib [`sqlite.backup`](https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.backup) function as a library, with a couple extra steps.

The main purpose for writing this is to copy sqlite databases that you may not own -- perhaps it belongs to an application (your browser) and is locked since that's currently open, or the OS keeps it open while the computer is active (e.g. Mac with iMessage)

### Features

- Has the option (true by default) to first atomically copy the database from disk to a temporary directory, which is:
  - useful in case the source is in read-only mode (e.g. in some sort of docker container)
  - safer if you're especially worried about corrupting or losing data
- Uses [`Cpython`s conn.backup](https://github.com/python/cpython/blob/main/Modules/_sqlite/connection.c#L1716), which directly uses the [underlying Sqlite C code](https://www.sqlite.org/c3ref/backup_finish.html)

In short, this prioritizes safety of the data over speed or performance -- because we often don't know what the application may be doing while we're copying underlying sqlite databases

This was extracted out of the [`karlicoss/HPI`](https://github.com/karlicoss/HPI/blob/a1f03f9c028df9d1898de2cc14f1df4fa6d8c471/my/core/sqlite.py#L33-L51) `sqlite` module

If other tools exist to do this, please [let me know!](https://github.com/seanbreckenridge/sqlite_backup/issues/new)

## Installation

Requires `python3.7+`

To install with pip, run:

    pip install sqlite_backup

## Usage

```
TODO: Fill this out

Usage: ...
```

### Tests

```bash
git clone 'https://github.com/seanbreckenridge/sqlite_backup'
cd ./sqlite_backup
pip install '.[testing]'
mypy ./sqlite_backup
pytest
```
