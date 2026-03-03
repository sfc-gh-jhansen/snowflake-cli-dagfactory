from pathlib import Path

import typer
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CollectionResult, CommandResult, MessageResult

app = SnowTyperFactory(
    name="dagfactory",
    help="Manages Task DAGs with Snowflake.",
)


@app.command(
    name="greet",
    requires_connection=False,
    requires_global_options=False,
)
def greet_command(
    name: str = typer.Option("Jane", "--name", "-n", help="Name to greet"),
) -> MessageResult:
    """
    Says hello to someone.
    """
    return MessageResult(f"Hello, {name}!")
