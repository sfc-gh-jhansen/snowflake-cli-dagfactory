import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)
