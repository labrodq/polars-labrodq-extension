from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import yaml

from polars_labrodq_extension.config.models import CheckConfig, DQConfig


def load_config(path: str | Path) -> DQConfig:
    """
    Load DQ configuration from a YAML file.

    Expected structure:

        dataset: test_dataset
        checks:
          - name: col1_not_null
            type: not_null
            column: col1
            level: error
            params:
              ...

    """
    path = Path(path)
    raw: Dict[str, Any]
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    dataset = raw.get("dataset")
    checks_raw = raw.get("checks", []) or []

    checks: List[CheckConfig] = []
    for item in checks_raw:
        checks.append(
            CheckConfig(
                name=item["name"],
                type=item["type"],
                column=item["column"],
                level=item.get("level", "error"),
                params=item.get("params") or {},
            )
        )

    return DQConfig(dataset=dataset, checks=checks)


def load_checks_from_yaml(path: str | Path):
    """
    Convenience helper to go directly from YAML → list[CheckDef].
    """
    cfg = load_config(path)
    return cfg.to_checks()
