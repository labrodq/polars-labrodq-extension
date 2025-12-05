from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Severity(str, Enum):
    ERROR = "error"
    WARN = "warn"
    INFO = "info"

    @classmethod
    def from_str(cls, value: str) -> "Severity":
        value = value.lower()
        match value:
            case "error":
                return cls.ERROR
            case "warn" | "warning":
                return cls.WARN
            case "info":
                return cls.INFO
            case _:
                raise ValueError(f"Unknown severity: {value!r}")


@dataclass
class CheckDef:
    """
    Definition of a single data-quality check.

    Supported types (MVP):
      - "not_null": column must not contain nulls
      - "max_null_ratio": null_ratio(column) <= threshold
    """

    name: str
    type: str
    column: str
    level: Severity = Severity.ERROR
    params: dict[str, Any] | None = None


@dataclass
class CheckResult:
    """
    Result of a single check evaluation.
    """

    name: str
    type: str
    column: str
    level: Severity
    passed: bool
    message: str
    details: dict[str, Any]
