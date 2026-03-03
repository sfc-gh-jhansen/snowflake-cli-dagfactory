import json
import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, RootModel, model_validator

log = logging.getLogger(__name__)


class TaskConfig(BaseModel):
    sql: Optional[str] = None
    stored_procedure: Optional[str] = None
    dependencies: List[str] = Field(default_factory=list)
    warehouse: Optional[str] = None
    user_task_timeout_ms: Optional[int] = None
    condition: Optional[str] = None
    finalizer: bool = False

    @model_validator(mode="after")
    def validate_definition(self):
        defined = sum(
            1
            for v in [self.sql, self.stored_procedure]
            if v is not None
        )
        if defined == 0:
            raise ValueError(
                "Task must define exactly one of: sql, stored_procedure"
            )
        if defined > 1:
            raise ValueError(
                "Task must define only one of: sql, stored_procedure"
            )
        if self.finalizer and self.dependencies:
            raise ValueError(
                "A finalizer task cannot have dependencies"
            )
        return self

    def resolve_definition(self) -> str:
        if self.sql is not None:
            return self.sql
        if self.stored_procedure is not None:
            return f"CALL {self.stored_procedure}()"
        raise ValueError("No task definition found")


class DAGConfig(BaseModel):
    schedule: Optional[str] = None
    warehouse: str
    comment: Optional[str] = None
    task_auto_retry_attempts: Optional[int] = None
    suspend_task_after_num_failures: Optional[int] = None
    user_task_timeout_ms: Optional[int] = None
    allow_overlapping_execution: bool = False
    config: Optional[Dict[str, Any]] = None
    when: Optional[str] = None
    tasks: Dict[str, TaskConfig]

    @model_validator(mode="after")
    def validate_dag(self):
        task_names = set(self.tasks.keys())

        for task_name, task_cfg in self.tasks.items():
            for dep in task_cfg.dependencies:
                if dep not in task_names:
                    raise ValueError(
                        f"Task '{task_name}' depends on unknown task '{dep}'"
                    )

        finalizers = [
            name for name, t in self.tasks.items() if t.finalizer
        ]
        if len(finalizers) > 1:
            raise ValueError(
                f"Only one finalizer task allowed per DAG, found: {finalizers}"
            )

        if not self.schedule and not self.when:
            raise ValueError(
                "DAG must have either a 'schedule' or a 'when' clause"
            )

        return self


class DagFactoryConfig(RootModel[Dict[str, DAGConfig]]):
    pass


def topological_sort(tasks: Dict[str, TaskConfig]) -> List[str]:
    in_degree: Dict[str, int] = {name: 0 for name in tasks}
    children: Dict[str, List[str]] = {name: [] for name in tasks}

    for name, task_cfg in tasks.items():
        if task_cfg.finalizer:
            continue
        for dep in task_cfg.dependencies:
            children[dep].append(name)
            in_degree[name] += 1

    queue = deque(
        name
        for name, degree in in_degree.items()
        if degree == 0 and not tasks[name].finalizer
    )
    result: List[str] = []

    while queue:
        current = queue.popleft()
        result.append(current)
        for child in children[current]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    non_finalizer_count = sum(
        1 for t in tasks.values() if not t.finalizer
    )
    if len(result) != non_finalizer_count:
        raise ValueError(
            "Cycle detected in task dependencies"
        )

    finalizers = [name for name, t in tasks.items() if t.finalizer]
    result.extend(finalizers)

    return result


def load_config(config_path: Path) -> DagFactoryConfig:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Config file must contain a YAML mapping, got {type(raw)}")

    return DagFactoryConfig.model_validate(raw)
