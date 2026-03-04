import json
import logging
from collections import deque
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Discriminator, Field, RootModel, Tag, model_validator

log = logging.getLogger(__name__)


class BaseTaskConfig(BaseModel):
    dependencies: List[str] = Field(default_factory=list)
    warehouse: Optional[str] = None
    user_task_timeout_ms: Optional[int] = None
    task_auto_retry_attempts: Optional[int] = None
    condition: Optional[str] = None
    description: Optional[str] = None
    finalizer: bool = False

    @model_validator(mode="after")
    def validate_base(self):
        if self.finalizer and self.dependencies:
            raise ValueError(
                "A finalizer task cannot have dependencies"
            )
        return self

    def resolve_definition(self) -> str:
        raise NotImplementedError


class SqlTask(BaseTaskConfig):
    task_type: Literal["sql"] = "sql"
    sql: str

    def resolve_definition(self) -> str:
        return self.sql


class StoredProcedureTask(BaseTaskConfig):
    task_type: Literal["stored_procedure"] = "stored_procedure"
    stored_procedure: str

    def resolve_definition(self) -> str:
        return f"CALL {self.stored_procedure}()"


class NotebookProjectTask(BaseTaskConfig):
    task_type: Literal["notebook_project"] = "notebook_project"
    notebook_project: str
    main_file: str
    compute_pool: str
    query_warehouse: str
    runtime: str
    arguments: Optional[str] = None
    requirements_file: Optional[str] = None
    external_access_integrations: List[str] = Field(default_factory=list)

    def resolve_definition(self) -> str:
        parts = [
            f"EXECUTE NOTEBOOK PROJECT {self.notebook_project}",
            f"  MAIN_FILE = '{self.main_file}'",
            f"  COMPUTE_POOL = '{self.compute_pool}'",
            f"  QUERY_WAREHOUSE = '{self.query_warehouse}'",
            f"  RUNTIME = '{self.runtime}'",
        ]
        if self.arguments is not None:
            parts.append(f"  ARGUMENTS = '{self.arguments}'")
        if self.requirements_file is not None:
            parts.append(f"  REQUIREMENTS_FILE = '{self.requirements_file}'")
        if self.external_access_integrations:
            eais = ", ".join(self.external_access_integrations)
            parts.append(f"  EXTERNAL_ACCESS_INTEGRATIONS = ({eais})")
        return "\n".join(parts)


def _task_discriminator(data: Any) -> str:
    if isinstance(data, dict):
        if "notebook_project" in data:
            return "notebook_project"
        if "stored_procedure" in data:
            return "stored_procedure"
        return "sql"
    return getattr(data, "task_type", "sql")


TaskConfig = Annotated[
    Union[
        Annotated[SqlTask, Tag("sql")],
        Annotated[StoredProcedureTask, Tag("stored_procedure")],
        Annotated[NotebookProjectTask, Tag("notebook_project")],
    ],
    Discriminator(_task_discriminator),
]


class DAGConfig(BaseModel):
    schedule: Optional[str] = None
    warehouse: str
    description: Optional[str] = None
    task_auto_retry_attempts: Optional[int] = None
    suspend_task_after_num_failures: Optional[int] = None
    user_task_timeout_ms: Optional[int] = None
    allow_overlapping_execution: bool = False
    config: Optional[Dict[str, Any]] = None
    when: Optional[str] = None
    tasks: Dict[str, TaskConfig]

    @model_validator(mode="before")
    @classmethod
    def normalize_tasks(cls, data: Any) -> Any:
        if isinstance(data, dict) and isinstance(data.get("tasks"), list):
            tasks_dict: Dict[str, Any] = {}
            for item in data["tasks"]:
                if not isinstance(item, dict) or "task_id" not in item:
                    raise ValueError(
                        "Each task in list format must be a mapping with a 'task_id' key"
                    )
                task_id = item.pop("task_id")
                if task_id in tasks_dict:
                    raise ValueError(f"Duplicate task_id: '{task_id}'")
                tasks_dict[task_id] = item
            data["tasks"] = tasks_dict
        return data

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


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


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

    defaults = raw.pop("default", None)
    if defaults and isinstance(defaults, dict):
        merged = {}
        for dag_name, dag_cfg in raw.items():
            if isinstance(dag_cfg, dict):
                merged[dag_name] = _deep_merge(defaults, dag_cfg)
            else:
                merged[dag_name] = dag_cfg
        raw = merged

    return DagFactoryConfig.model_validate(raw)
