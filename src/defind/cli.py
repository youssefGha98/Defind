from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import click


@click.group(help="Defind command line interface.")
def cli() -> None:
    """Root CLI group."""


@cli.command("version", help="Print installed Defind version.")
def version_cmd() -> None:
    """Display package version."""
    try:
        click.echo(version("defind"))
    except PackageNotFoundError:
        click.echo("defind (editable install)")
