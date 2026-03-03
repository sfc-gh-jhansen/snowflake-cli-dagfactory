from pathlib import Path

import pytest
import yaml

from snowflake_cli_dagfactory.config import (
    DAGConfig,
    DagFactoryConfig,
    TaskConfig,
    load_config,
    topological_sort,
)


class TestTaskConfig:
    def test_sql_task(self):
        task = TaskConfig(sql="SELECT 1")
        assert task.sql == "SELECT 1"
        assert task.stored_procedure is None

    def test_stored_procedure_task(self):
        task = TaskConfig(stored_procedure="my_db.my_schema.my_proc")
        assert task.stored_procedure == "my_db.my_schema.my_proc"

    def test_no_definition_raises(self):
        with pytest.raises(ValueError, match="exactly one of"):
            TaskConfig(dependencies=[])

    def test_multiple_definitions_raises(self):
        with pytest.raises(ValueError, match="only one of"):
            TaskConfig(sql="SELECT 1", stored_procedure="my_proc")

    def test_finalizer_with_dependencies_raises(self):
        with pytest.raises(ValueError, match="finalizer task cannot have dependencies"):
            TaskConfig(sql="SELECT 1", finalizer=True, dependencies=["task_a"])

    def test_resolve_inline_sql(self):
        task = TaskConfig(sql="SELECT 1")
        assert task.resolve_definition() == "SELECT 1"

    def test_resolve_stored_procedure(self):
        task = TaskConfig(stored_procedure="my_db.my_schema.my_proc")
        assert task.resolve_definition() == "CALL my_db.my_schema.my_proc()"


class TestDAGConfig:
    def test_valid_dag(self):
        dag = DAGConfig(
            schedule="60 MINUTES",
            warehouse="WH",
            tasks={
                "t1": TaskConfig(sql="SELECT 1"),
                "t2": TaskConfig(sql="SELECT 2", dependencies=["t1"]),
            },
        )
        assert len(dag.tasks) == 2

    def test_unknown_dependency_raises(self):
        with pytest.raises(ValueError, match="unknown task 'nonexistent'"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks={
                    "t1": TaskConfig(sql="SELECT 1", dependencies=["nonexistent"]),
                },
            )

    def test_multiple_finalizers_raises(self):
        with pytest.raises(ValueError, match="Only one finalizer"):
            DAGConfig(
                schedule="60 MINUTES",
                warehouse="WH",
                tasks={
                    "t1": TaskConfig(sql="SELECT 1"),
                    "f1": TaskConfig(sql="SELECT 2", finalizer=True),
                    "f2": TaskConfig(sql="SELECT 3", finalizer=True),
                },
            )

    def test_no_schedule_or_when_raises(self):
        with pytest.raises(ValueError, match="schedule.*when"):
            DAGConfig(
                warehouse="WH",
                tasks={
                    "t1": TaskConfig(sql="SELECT 1"),
                },
            )

    def test_when_clause_without_schedule(self):
        dag = DAGConfig(
            warehouse="WH",
            when="SYSTEM$STREAM_HAS_DATA('my_stream')",
            tasks={
                "t1": TaskConfig(sql="SELECT 1"),
            },
        )
        assert dag.when is not None


class TestTopologicalSort:
    def test_linear_chain(self):
        tasks = {
            "a": TaskConfig(sql="SELECT 1"),
            "b": TaskConfig(sql="SELECT 2", dependencies=["a"]),
            "c": TaskConfig(sql="SELECT 3", dependencies=["b"]),
        }
        result = topological_sort(tasks)
        assert result == ["a", "b", "c"]

    def test_diamond(self):
        tasks = {
            "a": TaskConfig(sql="SELECT 1"),
            "b": TaskConfig(sql="SELECT 2", dependencies=["a"]),
            "c": TaskConfig(sql="SELECT 3", dependencies=["a"]),
            "d": TaskConfig(sql="SELECT 4", dependencies=["b", "c"]),
        }
        result = topological_sort(tasks)
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")

    def test_finalizer_at_end(self):
        tasks = {
            "a": TaskConfig(sql="SELECT 1"),
            "b": TaskConfig(sql="SELECT 2", dependencies=["a"]),
            "cleanup": TaskConfig(sql="SELECT 3", finalizer=True),
        }
        result = topological_sort(tasks)
        assert result[-1] == "cleanup"

    def test_cycle_detection(self):
        tasks = {
            "a": TaskConfig(sql="SELECT 1", dependencies=["b"]),
            "b": TaskConfig(sql="SELECT 2", dependencies=["a"]),
        }
        with pytest.raises(ValueError, match="Cycle detected"):
            topological_sort(tasks)

    def test_multiple_roots(self):
        tasks = {
            "a": TaskConfig(sql="SELECT 1"),
            "b": TaskConfig(sql="SELECT 2"),
            "c": TaskConfig(sql="SELECT 3", dependencies=["a", "b"]),
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
        assert len(dag.tasks) == 6

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


class TestSQLGeneration:
    def test_manager_sql_for_root_task(self):
        from snowflake_cli_dagfactory.manager import SqlTaskDeployer

        manager = SqlTaskDeployer.__new__(SqlTaskDeployer)

        fqn = manager._fqn("MY_DB", "MY_SCHEMA", "my_task")
        assert fqn == "MY_DB.MY_SCHEMA.my_task"
