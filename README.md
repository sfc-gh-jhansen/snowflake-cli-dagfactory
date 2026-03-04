# Snowflake CLI DAG Factory Plugin

A [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) plugin that deploys [Snowflake Task DAGs](https://docs.snowflake.com/en/user-guide/tasks-graphs) from a YAML configuration file, inspired by Astronomer's [DAG Factory](https://github.com/astronomer/dag-factory).

## Quick Start

Define your DAG in a `dag_config.yml` file:

```yaml
default:
  warehouse: "DEMO_WH"
  task_auto_retry_attempts: 2

my_pipeline:
  schedule: "USING CRON 0 6 * * * America/Los_Angeles"
  description: "Daily ETL pipeline"

  tasks:
    - task_id: extract
      sql: "INSERT INTO staging SELECT * FROM raw_table"

    - task_id: transform
      sql: "INSERT INTO transformed SELECT * FROM staging"
      dependencies: [extract]

    - task_id: cleanup
      sql: "TRUNCATE TABLE staging"
      finalizer: true
```

Create a `snowflake.yml` project file:

```yaml
definition_version: 2
entities:
  my_pipeline:
    type: dagfactory
    config_file: dag_config.yml
    deployer: sql
    database: MY_DB
    schema_name: MY_SCHEMA
    warehouse: MY_WH
```

Deploy:

```bash
snow dagfactory deploy
```

## Configuration Reference

### `default` Block

Shared DAG-level settings that apply to all DAGs in the file. Individual DAGs can override any value.

```yaml
default:
  warehouse: "SHARED_WH"
  task_auto_retry_attempts: 2
  suspend_task_after_num_failures: 3
```

### DAG Settings

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `schedule` | string | Yes* | Cron expression or interval (e.g. `60 MINUTES`). |
| `when` | string | Yes* | Stream-based trigger (e.g. `SYSTEM$STREAM_HAS_DATA('my_stream')`). |
| `warehouse` | string | Yes | Default warehouse for all tasks in this DAG. |
| `description` | string | No | Maps to Snowflake `COMMENT` on the root task. |
| `task_auto_retry_attempts` | int | No | Retry count for failed tasks. |
| `suspend_task_after_num_failures` | int | No | Auto-suspend threshold. |
| `user_task_timeout_ms` | int | No | Task timeout in milliseconds. |
| `allow_overlapping_execution` | bool | No | Allow concurrent DAG runs. Default `false`. |
| `config` | object | No | Arbitrary key-value config passed to tasks. |

\* One of `schedule` or `when` is required.

### Task Types

Tasks are defined as a list under the `tasks` key. Each task requires a `task_id` and exactly one execution type.

Tasks can also be defined as a dictionary keyed by task name (without `task_id`).

#### SQL Task

Executes inline SQL.

```yaml
- task_id: my_task
  sql: "INSERT INTO target SELECT * FROM source"
```

#### Stored Procedure Task

Calls a stored procedure.

```yaml
- task_id: my_task
  stored_procedure: "my_db.my_schema.my_proc"
```

#### Notebook Project Task

Executes a notebook via [`EXECUTE NOTEBOOK PROJECT`](https://docs.snowflake.com/en/sql-reference/sql/execute-notebook-project).

```yaml
- task_id: my_task
  notebook_project: "my_db.my_schema.my_project"
  main_file: "notebook.ipynb"
  compute_pool: "GPU_POOL"
  query_warehouse: "ML_WH"
  runtime: "2.2-CPU-PY3.11"
  arguments: "env prod"                       # optional
  requirements_file: "requirements.txt"       # optional
  external_access_integrations: [http_eai]    # optional
```

### Common Task Attributes

These apply to any task type.

| Key | Type | Description |
|-----|------|-------------|
| `dependencies` | list | Task IDs this task depends on. |
| `warehouse` | string | Override the DAG-level warehouse for this task. |
| `user_task_timeout_ms` | int | Task-level timeout override. |
| `task_auto_retry_attempts` | int | Task-level retry override. |
| `condition` | string | `WHEN` clause for conditional execution. |
| `description` | string | Maps to Snowflake `COMMENT` on the task. |
| `finalizer` | bool | Marks this as the DAG's finalizer task (runs after all tasks complete or fail). Only one per DAG. Cannot have dependencies. |

### Full Example

See [`examples/dag_config.yml`](examples/dag_config.yml) for a complete example with all task types.

## Snowflake CLI Plugins

This project is built as a [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) plugin. Plugins extend the `snow` command with new subcommands, allowing third-party tools to integrate directly into the Snowflake developer workflow.

### How Plugins Work

Snowflake CLI discovers plugins via Python [entry points](https://packaging.python.org/en/latest/specifications/entry-points/). When you `pip install` a plugin package, its entry point is registered and the CLI can load it on demand.

A plugin has three main components:

**1. Entry point registration** (`pyproject.toml`)

The entry point tells Snowflake CLI where to find the plugin's command spec:

```toml
[project.entry-points."snowflake.cli.plugin.command"]
dagfactory = "snowflake_cli_dagfactory.plugin_spec"
```

**2. Plugin spec** (`plugin_spec.py`)

Defines the command hierarchy using `CommandSpec`. The `@plugin_hook_impl` decorator registers it with the CLI's plugin system:

```python
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)

@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=commands.app.create_instance(),
    )
```

**3. Commands** (`commands.py`)

Commands are defined using `SnowTyperFactory`, a wrapper around [Typer](https://typer.tiangolo.com/) that adds Snowflake connection management, output formatting, and project definition support:

```python
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(name="dagfactory", help="Manages Task DAGs.")

@app.command(name="deploy", requires_connection=True)
def deploy_command(...) -> CommandResult:
    ...
```

Setting `requires_connection=True` automatically handles Snowflake authentication and injects the active connection.

## Development Setup

1. **Install Snowflake CLI**
   ```bash
   pip install snowflake-cli
   ```

2. **Install this plugin in editable mode**
   ```bash
   pip install -e .
   ```

3. **Enable this plugin**
   ```bash
   snow plugin enable dagfactory
   ```
   The `dagfactory` command should now be available in the CLI.

4. **Run tests**
   ```bash
   pytest
   ```
