from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List

from polars_labrodq_extension.core.types import CheckDef, Severity


@dataclass
class CheckConfig:
    name: str
    type: str
    column: str
    level: str = "error"
    params: dict[str, Any] = field(default_factory=dict)

    def to_check_def(self) -> CheckDef:
        return CheckDef(
            name=self.name,
            type=self.type,
            column=self.column,
            level=Severity.from_str(self.level),
            params=self.params or None,
        )


@dataclass
class DQConfig:
    dataset: str | None
    checks: List[CheckConfig]

    def to_checks(self) -> List[CheckDef]:
        return [c.to_check_def() for c in self.checks]
