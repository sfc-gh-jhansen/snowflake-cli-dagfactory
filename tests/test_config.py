from pathlib import Path

import pytest
import yaml

from snowflake_cli_dagfactory.config import (
    DAGConfig,
    DagFactoryConfig,
    NotebookProjectTask,
    SqlTask,
    StoredProcedureTask,
    _deep_merge,
    load_config,
    topological_sort,
)


class TestSqlTask:
    def test_sql_task(self):
        task = SqlTask(sql="SELECT 1")
        assert task.sql == "SELECT 1"

    def test_resolve_definition(self):
        task = SqlTask(sql="SELECT 1")
        assert task.resolve_definition() == "SELECT 1"

    def test_description(self):
        task = SqlTask(sql="SELECT 1", description="my task")
        assert task.description == "my task"

    def test_retry_attempts(self):
        task = SqlTask(sql="SELECT 1", task_auto_retry_attempts=5)
        assert task.task_auto_retry_attempts == 5

    def test_finalizer_with_dependencies_raises(self):
        with pytest.raises(ValueError, match="finalizer task cannot have dependencies"):
            SqlTask(sql="SELECT 1", finalizer=True, dependencies=["task_a"])


class TestStoredProcedureTask:
    def test_stored_procedure_task(self):
        task = StoredProcedureTask(stored_procedure="my_db.my_schema.my_proc")
        assert task.stored_procedure == "my_db.my_schema.my_proc"

    def test_resolve_definition(self):
        task = StoredProcedureTask(stored_procedure="my_db.my_schema.my_proc")
        assert task.resolve_definition() == "CALL my_db.my_schema.my_proc()"


class TestNotebookProjectTask:
    def test_notebook_project_task(self):
        task = NotebookProjectTask(
            notebook_project="my_db.my_schema.my_project",
            main_file="notebook.ipynb",
            compute_pool="GPU_POOL",
            query_warehouse="ML_WH",
            runtime="2.2-CPU-PY3.11",
        )
        assert task.notebook_project == "my_db.my_schema.my_project"

    def test_missing_required_raises(self):
        with pytest.raises(ValueError):
            NotebookProjectTask(
                notebook_project="my_db.my_schema.my_project",
                compute_pool="GPU_POOL",
                query_warehouse="ML_WH",
                runtime="2.2-CPU-PY3.11",
            )

    def test_resolve_definition(self):
        task = NotebookProjectTask(
            notebook_project="my_db.my_schema.my_project",
            main_file="notebook.ipynb",
            compute_pool="GPU_POOL",
            query_warehouse="ML_WH",
            runtime="2.2-CPU-PY3.11",
        )
        sql = task.resolve_definition()
        assert "EXECUTE NOTEBOOK PROJECT my_db.my_schema.my_project" in sql
        assert "MAIN_FILE = 'notebook.ipynb'" in sql
        assert "COMPUTE_POOL = 'GPU_POOL'" in sql
        assert "QUERY_WAREHOUSE = 'ML_WH'" in sql
        assert "RUNTIME = '2.2-CPU-PY3.11'" in sql

    def test_resolve_with_optionals(self):
        task = NotebookProjectTask(
            notebook_project="my_db.my_schema.my_project",
            main_file="notebook.ipynb",
            compute_pool="GPU_POOL",
            query_warehouse="ML_WH",
            runtime="2.2-CPU-PY3.11",
            arguments="env prod",
            requirements_file="requirements.txt",
            external_access_integrations=["http_eai", "s3_eai"],
        )
        sql = task.resolve_definition()
        assert "ARGUMENTS = 'env prod'" in sql
        assert "REQUIREMENTS_FILE = 'requirements.txt'" in sql
        assert "EXTERNAL_ACCESS_INTEGRATIONS = (http_eai, s3_eai)" in sql


class TestDAGConfig:
    def test_valid_dag(self):
        dag = DAGConfig(
            schedule="60 MINUTES",
            warehouse="WH",
            tasks={
                "t1": SqlTask(sql="SELECT 1"),
                "t2": SqlTask(sql="SELECT 2", dependencies=["t1"]),
            },
        )
        assert len(dag.tasks) == 2

    def test_unknown_dependency_raises(self):
        with pytest.raises(ValueError, match="unknown task 'nonexistent'"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks={
                    "t1": SqlTask(sql="SELECT 1", dependencies=["nonexistent"]),
                },
            )

    def test_multiple_finalizers_raises(self):
        with pytest.raises(ValueError, match="Only one finalizer"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks={
                    "t1": SqlTask(sql="SELECT 1"),
                    "f1": SqlTask(sql="SELECT 2", finalizer=True),
                    "f2": SqlTask(sql="SELECT 3", finalizer=True),
                },
            )

    def test_no_schedule_or_when_raises(self):
        with pytest.raises(ValueError, match="schedule.*when"):
            DAGConfig(
                warehouse="WH",
                tasks={
                    "t1": SqlTask(sql="SELECT 1"),
                },
            )

    def test_when_clause_without_schedule(self):
        dag = DAGConfig(
            warehouse="WH",
            when="SYSTEM$STREAM_HAS_DATA('my_stream')",
            tasks={
                "t1": SqlTask(sql="SELECT 1"),
            },
        )
        assert dag.when is not None

    def test_description_field(self):
        dag = DAGConfig(
            schedule="60 MINUTES",
            warehouse="WH",
            description="my pipeline",
            tasks={"t1": SqlTask(sql="SELECT 1")},
        )
        assert dag.description == "my pipeline"

    def test_tasks_list_format(self):
        dag = DAGConfig(
            schedule="60 MINUTES",
            warehouse="WH",
            tasks=[
                {"task_id": "t1", "sql": "SELECT 1"},
                {"task_id": "t2", "sql": "SELECT 2", "dependencies": ["t1"]},
            ],
        )
        assert "t1" in dag.tasks
        assert "t2" in dag.tasks
        assert dag.tasks["t2"].dependencies == ["t1"]

    def test_tasks_list_missing_task_id_raises(self):
        with pytest.raises(ValueError, match="task_id"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks=[{"sql": "SELECT 1"}],
            )

    def test_tasks_list_duplicate_task_id_raises(self):
        with pytest.raises(ValueError, match="Duplicate task_id"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks=[
                    {"task_id": "t1", "sql": "SELECT 1"},
                    {"task_id": "t1", "sql": "SELECT 2"},
                ],
            )


class TestTopologicalSort:
    def test_linear_chain(self):
        tasks = {
            "a": SqlTask(sql="SELECT 1"),
            "b": SqlTask(sql="SELECT 2", dependencies=["a"]),
            "c": SqlTask(sql="SELECT 3", dependencies=["b"]),
        }
        result = topological_sort(tasks)
        assert result == ["a", "b", "c"]

    def test_diamond(self):
        tasks = {
            "a": SqlTask(sql="SELECT 1"),
            "b": SqlTask(sql="SELECT 2", dependencies=["a"]),
            "c": SqlTask(sql="SELECT 3", dependencies=["a"]),
            "d": SqlTask(sql="SELECT 4", dependencies=["b", "c"]),
        }
        result = topological_sort(tasks)
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")

    def test_finalizer_at_end(self):
        tasks = {
            "a": SqlTask(sql="SELECT 1"),
            "b": SqlTask(sql="SELECT 2", dependencies=["a"]),
            "cleanup": SqlTask(sql="SELECT 3", finalizer=True),
        }
        result = topological_sort(tasks)
        assert result[-1] == "cleanup"

    def test_cycle_detection(self):
        tasks = {
            "a": SqlTask(sql="SELECT 1", dependencies=["b"]),
            "b": SqlTask(sql="SELECT 2", dependencies=["a"]),
        }
        with pytest.raises(ValueError, match="Cycle detected"):
            topological_sort(tasks)

    def test_multiple_roots(self):
        tasks = {
            "a": SqlTask(sql="SELECT 1"),
            "b": SqlTask(sql="SELECT 2"),
            "c": SqlTask(sql="SELECT 3", dependencies=["a", "b"]),
        }
        result = topological_sort(tasks)
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("c")


class TestLoadConfig:
    def test_load_example_config(self):
        example_path = Path(__file__).resolve().parent.parent / "examples" / "dag_config.yml"
        config = load_config(example_path)
        assert "my_etl_pipeline" in config.root
        dag = config.root["my_etl_pipeline"]
        assert dag.warehouse == "DEMO_WH"
        assert len(dag.tasks) == 7

    def test_load_from_tmp(self, tmp_path):
        cfg = {
            "test_dag": {
                "schedule": "60 MINUTES",
                "warehouse": "WH",
                "tasks": {
                    "step1": {"sql": "SELECT 1"},
                    "step2": {"sql": "SELECT 2", "dependencies": ["step1"]},
                },
            }
        }
        config_file = tmp_path / "dag_config.yml"
        config_file.write_text(yaml.dump(cfg))
        config = load_config(config_file)
        assert "test_dag" in config.root

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/dag_config.yml"))

    def test_invalid_yaml_raises(self, tmp_path):
        config_file = tmp_path / "dag_config.yml"
        config_file.write_text("not a mapping: [")
        with pytest.raises(Exception):
            load_config(config_file)

    def test_default_block_merges(self, tmp_path):
        cfg = {
            "default": {
                "warehouse": "DEFAULT_WH",
                "task_auto_retry_attempts": 3,
            },
            "dag_a": {
                "schedule": "60 MINUTES",
                "tasks": {
                    "t1": {"sql": "SELECT 1"},
                },
            },
            "dag_b": {
                "schedule": "60 MINUTES",
                "warehouse": "OVERRIDE_WH",
                "tasks": {
                    "t1": {"sql": "SELECT 2"},
                },
            },
        }
        config_file = tmp_path / "dag_config.yml"
        config_file.write_text(yaml.dump(cfg))
        config = load_config(config_file)
        assert config.root["dag_a"].warehouse == "DEFAULT_WH"
        assert config.root["dag_a"].task_auto_retry_attempts == 3
        assert config.root["dag_b"].warehouse == "OVERRIDE_WH"
        assert config.root["dag_b"].task_auto_retry_attempts == 3

    def test_load_fixture_dict_format(self):
        fixture_path = Path(__file__).resolve().parent / "fixtures" / "test_dag_config.yml"
        config = load_config(fixture_path)
        dag = config.root["test_pipeline"]
        assert dag.warehouse == "DEMO_WH"
        assert dag.description == "Integration test pipeline"
        assert len(dag.tasks) == 4
        assert dag.tasks["step_four"].dependencies == ["step_two", "step_three"]

    def test_load_fixture_list_format(self):
        fixture_path = Path(__file__).resolve().parent / "fixtures" / "test_dag_config_list.yml"
        config = load_config(fixture_path)
        dag = config.root["test_pipeline"]
        assert dag.warehouse == "DEMO_WH"
        assert dag.description == "Integration test pipeline (list format)"
        assert len(dag.tasks) == 4
        assert "step_one" in dag.tasks
        assert dag.tasks["step_four"].dependencies == ["step_two", "step_three"]


class TestHelpers:
    def test_deep_merge_override(self):
        base = {"a": 1, "b": {"x": 10, "y": 20}}
        override = {"b": {"y": 99}, "c": 3}
        result = _deep_merge(base, override)
        assert result == {"a": 1, "b": {"x": 10, "y": 99}, "c": 3}


class TestSQLGeneration:
    def test_manager_sql_for_root_task(self):
        from snowflake_cli_dagfactory.manager import SqlTaskDeployer

        manager = SqlTaskDeployer.__new__(SqlTaskDeployer)

        fqn = manager._fqn("MY_DB", "MY_SCHEMA", "my_task")
        assert fqn == "MY_DB.MY_SCHEMA.my_task"
