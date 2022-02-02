import sys
import logging
from pathlib import Path

import click

from .core import sqlite_backup, COPY_RETRY_DEFAULT
from .log import setup

CONTEXT_SETTINGS = {
    "max_content_width": 120,
    "show_default": True,
}


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--debug", is_flag=True, default=False, help="Increase log verbosity")
@click.option(
    "--wal-checkpoint/--no-wal-checkpoint",
    default=True,
    is_flag=True,
    help="After writing to the destination, run a checkpoint to truncate the WAL to zero bytes",
)
@click.option(
    "--copy-use-tempdir/--no-copy-use-tempdir",
    default=True,
    is_flag=True,
    help="Copy the source database files to a temporary directory, then connect to the copied files",
)
@click.option(
    "--copy-retry",
    default=COPY_RETRY_DEFAULT,
    type=int,
    show_default=False,
    help="If the files change while copying to the temporary directory, retry <n> times",
)
@click.option(
    "--copy-retry-strict/--no-copy-retry-strict",
    default=True,
    is_flag=True,
    help="Throws an error if this fails to safely copy the database files --copy-retry times",
)
@click.argument(
    "SOURCE_DATABASE",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
@click.argument("DESTINATION", required=True, type=click.Path(path_type=Path))
def main(
    debug: bool,
    wal_checkpoint: bool,
    copy_use_tempdir: bool,
    copy_retry: int,
    copy_retry_strict: bool,
    source_database: Path,
    destination: Path,
) -> None:
    """
    SOURCE_DATABASE is the database to copy

    \b
    DESTINATION is where to write the database. If a directory, uses
    the SOURCE_DATABASE name. If a file, the directory must exist,
    and the destination file must not already exist (to prevent
    possibly overwriting old data)
    """
    if debug:
        setup(logging.DEBUG)

    source_database = source_database.absolute()
    dest: Path
    if destination.exists():
        if destination.is_dir():
            dest = (destination / source_database.name).absolute()
            if dest.exists():
                click.echo(
                    f"Computed DESTINATION '{dest}' using SOURCE_DATABASE name already exists",
                    err=True,
                )
                sys.exit(1)
        elif destination.is_file():
            click.echo(f"Target DESTINATION already exists: '{destination}'", err=True)
            sys.exit(1)
        else:
            click.echo(
                f"Target DESTINATION '{destination}' is not a directory or a file",
                err=True,
            )
            sys.exit(1)
    else:
        # doesnt exist, check if parent dir exists
        if not destination.parent.exists():
            click.echo(
                f"Parent directory '{destination.parent}' does not exist", err=True
            )
            sys.exit(1)
        dest = destination.absolute()

    sqlite_backup(
        source_database,
        dest,
        wal_checkpoint=wal_checkpoint,
        copy_use_tempdir=copy_use_tempdir,
        copy_retry=copy_retry,
        copy_retry_strict=copy_retry_strict,
    )
    click.echo(f"Backed up {source_database} to {dest}", err=True)


if __name__ == "__main__":
    main(prog_name="sqlite_backup")
