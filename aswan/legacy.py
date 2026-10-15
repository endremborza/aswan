"""
Legacy depot converter: upgrades a pre-1.0 aswan depot to the current format.

Changes applied:
- context.yaml -> context.json (YAML removed), `pip_freeze` -> `uv_freeze`
- status-cache.pkl and statuses/ DAG structure removed
- Object store compression unchanged (dirark migration is a separate step)
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from .depot.base import CONTEXT_JSON

_RENAMED_KEYS = {"pip_freeze": "uv_freeze"}


def convert_depot(legacy_path: Path, output_path: Path | None = None) -> Path:
    """Convert a legacy (pre-1.0) depot directory to the new format.

    If output_path is None, converts in-place after making a .bak backup.
    Returns the path to the converted depot.
    """
    if output_path is None:
        backup = legacy_path.parent / (legacy_path.name + ".bak")
        shutil.copytree(legacy_path, backup)
        output_path = legacy_path
    else:
        shutil.copytree(legacy_path, output_path)

    _convert_context_files(output_path)
    _remove_dag_artifacts(output_path)
    return output_path


def _convert_context_files(root: Path) -> None:
    """Recursively convert context.yaml to context.json."""
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml required for legacy conversion: uv add pyyaml")

    for yaml_file in root.rglob("context.yaml"):
        data = yaml.safe_load(yaml_file.read_text())
        renamed = {_RENAMED_KEYS.get(k, k): v for k, v in data.items()}
        yaml_file.with_name(CONTEXT_JSON).write_text(json.dumps(renamed))
        yaml_file.unlink()


def _remove_dag_artifacts(root: Path) -> None:
    """Remove StatusCache pickle and legacy statuses/ directory."""
    cache = root / "status-cache.pkl"
    if cache.exists():
        cache.unlink()

    statuses_dir = root / "statuses"
    if statuses_dir.exists():
        shutil.rmtree(statuses_dir)
