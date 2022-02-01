import sys
from pathlib import Path

import click

from .core import sqlite_backup


# - would be nice to add defaults to the `--help`, especially considering that some flags are true by default and some are false. 
# I figured out how to do this in click for all parameters recently: https://github.com/karlicoss/bleanser/blob/7497b98a3187f7fa75ce88002d858ff8699ac789/src/bleanser/core/main.py#L15-L24
@click.command()
@click.option(
    # Not sure if even worth exposing in cli? Although pehaps useful for truly paranoid people :)
    "--wal-checkpoint/--no-wal-checkpoint",
    default=True,
    is_flag=True,
    help="After writing to the destination, run a checkpoint to truncate the WAL to zero bytes",
)
@click.option(
    "--copy-use-tempdir/--copy-no-tempdir",
    default=True,
    is_flag=True,
    help="Copy the source database files to a temporary directory, then connect to the copied files",
)
@click.option(
    "--copy-retry",
    default=100,
    type=int,
    help="If the files change while copying to the temporary directory, retry <n> times",
)
@click.option(
    # hmm imo isn't clear from the help message what happens if it fails to copy after --copy-retry times 
    # seems it just continues? I guess that's sensible in some cases, but IMO the default=False feels somewhat unexpected?
    "--copy-retry-strict/--no-copy-retry-strict",
    default=False,
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
            click.echo(f"Target DESTINATION is not a directory or a file", err=True)
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
