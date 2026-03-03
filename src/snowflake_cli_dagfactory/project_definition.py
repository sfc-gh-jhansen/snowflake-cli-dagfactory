from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class DagFactoryProjectDefinition(BaseModel):
    type: str = "dagfactory"
    config_file: Path = Field(
        default=Path("dag_config.yml"),
        title="Path to the DAG configuration YAML file",
    )
    database: Optional[str] = Field(default=None, title="Target database")
    schema_name: Optional[str] = Field(default=None, title="Target schema")
    warehouse: Optional[str] = Field(default=None, title="Default warehouse")
    role: Optional[str] = Field(default=None, title="Role for task ownership")
    deployer: str = Field(default="sql", title="Deployer backend: sql, python_api, dag_api")


def load_project_entity(
    project_path: Path, entity_id: Optional[str] = None
) -> "DagFactoryProjectDefinition":
    if not project_path.exists():
        raise FileNotFoundError(f"Project file not found: {project_path}")

    with open(project_path) as f:
        raw = yaml.safe_load(f)

    entities = raw.get("entities", {})
    dagfactory_entities = {
        name: data
        for name, data in entities.items()
        if data.get("type") == "dagfactory"
    }

    if not dagfactory_entities:
        raise ValueError("No dagfactory entities found in snowflake.yml")

    if entity_id:
        if entity_id not in dagfactory_entities:
            raise ValueError(
                f"Entity '{entity_id}' not found. Available: {list(dagfactory_entities.keys())}"
            )
        entity_data = dagfactory_entities[entity_id]
    elif len(dagfactory_entities) == 1:
        entity_data = next(iter(dagfactory_entities.values()))
    else:
        raise ValueError(
            f"Multiple dagfactory entities found: {list(dagfactory_entities.keys())}. "
            "Use --entity-id to specify which one."
        )

    return DagFactoryProjectDefinition.model_validate(entity_data)
