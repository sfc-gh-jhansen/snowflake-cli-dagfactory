from pathlib import Path
from typing import Optional

import typer
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CollectionResult, CommandResult, MessageResult

from snowflake_cli_dagfactory.config import load_config
from snowflake_cli_dagfactory.manager import SqlTaskDeployer, TaskDeployer
from snowflake_cli_dagfactory.project_definition import load_project_entity

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
    """Says hello to someone."""
    return MessageResult(f"Hello, {name}!")


@app.command(
    name="deploy",
    requires_connection=True,
)
def deploy_command(
    project_file: Path = typer.Option(
        Path("snowflake.yml"),
        "--project",
        "-p",
        help="Path to snowflake.yml project definition file.",
    ),
    entity_id: Optional[str] = typer.Option(
        None,
        "--entity-id",
        "-e",
        help="Entity ID to deploy. Required if snowflake.yml has multiple dagfactory entities.",
    ),
    replace: bool = typer.Option(
        False,
        "--replace",
        help="Replace existing tasks (CREATE OR REPLACE).",
    ),
    **options,
) -> CommandResult:
    """Deploy task DAGs defined in a YAML config file to Snowflake."""
    project_def = load_project_entity(project_file.resolve(), entity_id)
    config_path = (project_file.resolve().parent / project_def.config_file).resolve()
    config = load_config(config_path)

    conn = get_cli_context().connection
    resolved_database = project_def.database or conn.database
    resolved_schema = project_def.schema_name or conn.schema
    resolved_warehouse = project_def.warehouse or conn.warehouse

    if not resolved_database:
        raise typer.BadParameter(
            "Database must be specified in snowflake.yml, via --database, or connection config."
        )
    if not resolved_schema:
        raise typer.BadParameter(
            "Schema must be specified in snowflake.yml, via --schema, or connection config."
        )
    if not resolved_warehouse:
        raise typer.BadParameter(
            "Warehouse must be specified in snowflake.yml, via --warehouse, or connection config."
        )

    deployer = _get_deployer(project_def.deployer)
    results = deployer.deploy(
        config=config,
        database=resolved_database,
        schema=resolved_schema,
        default_warehouse=resolved_warehouse,
        replace=replace,
    )

    output_rows = []
    for result in results:
        output_rows.append(
            {
                "dag_name": result.dag_name,
                "root_task": result.root_task,
                "tasks_created": ", ".join(result.tasks_created),
                "finalizer_task": result.finalizer_task or "N/A",
                "status": result.status,
            }
        )

    return CollectionResult(output_rows)


DEPLOYER_REGISTRY: dict[str, type[TaskDeployer]] = {
    "sql": SqlTaskDeployer,
}


def _get_deployer(deployer_type: str) -> TaskDeployer:
    cls = DEPLOYER_REGISTRY.get(deployer_type)
    if cls is None:
        raise typer.BadParameter(
            f"Unknown deployer '{deployer_type}'. Available: {list(DEPLOYER_REGISTRY.keys())}"
        )
    return cls()
