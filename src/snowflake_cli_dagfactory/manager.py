import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol

from snowflake.cli.api.sql_execution import SqlExecutionMixin

from snowflake_cli_dagfactory.config import (
    BaseTaskConfig,
    DAGConfig,
    DagFactoryConfig,
    TaskConfig,
    topological_sort,
)

log = logging.getLogger(__name__)


@dataclass
class DeployResult:
    dag_name: str
    tasks_created: List[str]
    root_task: str
    finalizer_task: Optional[str]
    status: str


class TaskDeployer(Protocol):
    def deploy(
        self,
        config: DagFactoryConfig,
        database: str,
        schema: str,
        default_warehouse: str,
        replace: bool = False,
    ) -> List[DeployResult]: ...


class SqlTaskDeployer(SqlExecutionMixin):

    def deploy(
        self,
        config: DagFactoryConfig,
        database: str,
        schema: str,
        default_warehouse: str,
        replace: bool = False,
    ) -> List[DeployResult]:
        results = []
        for dag_name, dag_config in config.root.items():
            result = self._deploy_dag(
                dag_name=dag_name,
                dag_config=dag_config,
                database=database,
                schema=schema,
                default_warehouse=default_warehouse,
                replace=replace,
            )
            results.append(result)
        return results

    def _deploy_dag(
        self,
        dag_name: str,
        dag_config: DAGConfig,
        database: str,
        schema: str,
        default_warehouse: str,
        replace: bool,
    ) -> DeployResult:
        ordered_tasks = topological_sort(dag_config.tasks)
        warehouse = dag_config.warehouse or default_warehouse

        non_finalizer_tasks = [
            t for t in ordered_tasks if not dag_config.tasks[t].finalizer
        ]
        finalizer_tasks = [
            t for t in ordered_tasks if dag_config.tasks[t].finalizer
        ]

        root_candidates = [
            t for t in non_finalizer_tasks
            if not dag_config.tasks[t].dependencies
        ]

        synthetic_root = None
        if len(root_candidates) > 1:
            synthetic_root = f"{dag_name}__root"
            log.info(
                "Multiple root tasks found (%s), creating synthetic root: %s",
                root_candidates,
                synthetic_root,
            )

        root_task_name = synthetic_root if synthetic_root else root_candidates[0]

        try:
            existing_root = self._get_existing_task(database, schema, root_task_name)
            if existing_root:
                log.info("Suspending existing root task: %s", root_task_name)
                self._suspend_task(database, schema, root_task_name)
        except Exception:
            pass

        if synthetic_root:
            self._create_task(
                database=database,
                schema=schema,
                task_name=synthetic_root,
                warehouse=warehouse,
                definition="SELECT 1",
                replace=replace,
                schedule=dag_config.schedule,
                dag_config=dag_config,
            )

            for candidate in root_candidates:
                task_cfg = dag_config.tasks[candidate]
                task_warehouse = task_cfg.warehouse or warehouse
                definition = task_cfg.resolve_definition()
                self._create_task(
                    database=database,
                    schema=schema,
                    task_name=candidate,
                    warehouse=task_warehouse,
                    definition=definition,
                    replace=replace,
                    predecessors=[synthetic_root],
                    task_config=task_cfg,
                )
        else:
            root_cfg = dag_config.tasks[root_candidates[0]]
            root_warehouse = root_cfg.warehouse or warehouse
            definition = root_cfg.resolve_definition()
            self._create_task(
                database=database,
                schema=schema,
                task_name=root_candidates[0],
                warehouse=root_warehouse,
                definition=definition,
                replace=replace,
                schedule=dag_config.schedule,
                dag_config=dag_config,
            )

        for task_name in non_finalizer_tasks:
            if task_name in root_candidates:
                continue
            task_cfg = dag_config.tasks[task_name]
            task_warehouse = task_cfg.warehouse or warehouse
            definition = task_cfg.resolve_definition()
            predecessors = task_cfg.dependencies
            self._create_task(
                database=database,
                schema=schema,
                task_name=task_name,
                warehouse=task_warehouse,
                definition=definition,
                replace=replace,
                predecessors=predecessors,
                task_config=task_cfg,
            )

        finalizer_task_name = None
        for task_name in finalizer_tasks:
            task_cfg = dag_config.tasks[task_name]
            task_warehouse = task_cfg.warehouse or warehouse
            definition = task_cfg.resolve_definition()
            self._create_task(
                database=database,
                schema=schema,
                task_name=task_name,
                warehouse=task_warehouse,
                definition=definition,
                replace=replace,
                finalize_root=root_task_name,
            )
            finalizer_task_name = task_name

        tasks_to_resume = []
        if finalizer_task_name:
            tasks_to_resume.append(finalizer_task_name)
        for task_name in reversed(non_finalizer_tasks):
            if task_name == root_task_name:
                continue
            tasks_to_resume.append(task_name)
        if synthetic_root:
            for candidate in root_candidates:
                if candidate not in tasks_to_resume:
                    tasks_to_resume.append(candidate)
        tasks_to_resume.append(root_task_name)

        for task_name in tasks_to_resume:
            self._resume_task(database, schema, task_name)

        all_created = []
        if synthetic_root:
            all_created.append(synthetic_root)
        all_created.extend(non_finalizer_tasks)
        if finalizer_task_name:
            all_created.append(finalizer_task_name)

        return DeployResult(
            dag_name=dag_name,
            tasks_created=all_created,
            root_task=root_task_name,
            finalizer_task=finalizer_task_name,
            status="deployed",
        )

    def _fqn(self, database: str, schema: str, name: str) -> str:
        return f"{database}.{schema}.{name}"

    def _create_task(
        self,
        database: str,
        schema: str,
        task_name: str,
        warehouse: str,
        definition: str,
        replace: bool,
        schedule: Optional[str] = None,
        predecessors: Optional[List[str]] = None,
        finalize_root: Optional[str] = None,
        dag_config: Optional[DAGConfig] = None,
        task_config: Optional[BaseTaskConfig] = None,
    ) -> None:
        """Build and execute a CREATE TASK statement.

        Supports three task variants via optional parameters:
          - Root task: pass schedule + dag_config
          - Child task: pass predecessors + task_config
          - Finalizer task: pass finalize_root
        """
        fqn = self._fqn(database, schema, task_name)
        create_keyword = "CREATE OR REPLACE" if replace else "CREATE"
        parts = [f"{create_keyword} TASK {fqn}"]
        parts.append(f"  WAREHOUSE = '{warehouse}'")

        if schedule:
            parts.append(f"  SCHEDULE = '{schedule}'")

        if predecessors:
            after_clause = ", ".join(
                self._fqn(database, schema, dep) for dep in predecessors
            )
            parts.append(f"  AFTER {after_clause}")

        if finalize_root:
            root_fqn = self._fqn(database, schema, finalize_root)
            parts.append(f"  FINALIZE = {root_fqn}")

        timeout = (
            (dag_config.user_task_timeout_ms if dag_config else None)
            or (task_config.user_task_timeout_ms if task_config else None)
        )
        if timeout is not None:
            parts.append(f"  USER_TASK_TIMEOUT_MS = {timeout}")

        retry = (
            (dag_config.task_auto_retry_attempts if dag_config else None)
            or (task_config.task_auto_retry_attempts if task_config else None)
        )
        if retry is not None:
            parts.append(f"  TASK_AUTO_RETRY_ATTEMPTS = {retry}")

        if dag_config:
            if dag_config.suspend_task_after_num_failures is not None:
                parts.append(f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {dag_config.suspend_task_after_num_failures}")

            if dag_config.allow_overlapping_execution:
                parts.append("  ALLOW_OVERLAPPING_EXECUTION = TRUE")

            if dag_config.config:
                config_json = json.dumps(dag_config.config)
                parts.append(f"  CONFIG = '{config_json}'")

        comment = (
            (dag_config.description if dag_config else None)
            or (task_config.description if task_config else None)
        )
        if comment:
            escaped = comment.replace("'", "''")
            parts.append(f"  COMMENT = '{escaped}'")

        condition = (
            (dag_config.when if dag_config else None)
            or (task_config.condition if task_config else None)
        )
        if condition:
            parts.append(f"  WHEN\n    {condition}")

        parts.append(f"  AS\n    {definition}")

        sql = "\n".join(parts)
        log.info("Creating task: %s", task_name)
        log.debug("SQL: %s", sql)
        self.execute_query(sql)

    def _suspend_task(self, database: str, schema: str, task_name: str) -> None:
        fqn = self._fqn(database, schema, task_name)
        self.execute_query(f"ALTER TASK {fqn} SUSPEND")

    def _resume_task(self, database: str, schema: str, task_name: str) -> None:
        fqn = self._fqn(database, schema, task_name)
        log.info("Resuming task: %s", task_name)
        self.execute_query(f"ALTER TASK {fqn} RESUME")

    def _get_existing_task(
        self, database: str, schema: str, task_name: str
    ) -> Optional[Any]:
        try:
            cursor = self.execute_query(
                f"SHOW TASKS LIKE '{task_name}' IN {database}.{schema}"
            )
            rows = cursor.fetchall()
            return rows[0] if rows else None
        except Exception:
            return None
