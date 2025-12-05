from __future__ import annotations

from typing import Iterable, List

import polars as pl

from polars_labrodq_extension.config.loader import load_checks_from_yaml
from polars_labrodq_extension.core.types import CheckDef, CheckResult


@pl.api.register_dataframe_namespace("ldq")
class DataFrameDQNamespace:
    """
    Polars DataFrame namespace for data-quality checks.

    After importing this module (or labrodq that imports it),
    every `pl.DataFrame` instance will have a `.ldq` property:

        df.ldq.run_checks(...)
        df.ldq.run_yaml("dq/test.yml")
        df.ldq.quality_report_yaml("dq/test.yml")

    This is a minimal MVP implementation.
    """

    def __init__(self, df: pl.DataFrame) -> None:
        self._df: pl.DataFrame = df

    # -------- Public API --------

    def run_checks(self, checks: Iterable[CheckDef]) -> List[CheckResult]:
        results: List[CheckResult] = []

        for check in checks:
            if check.type == "not_null":
                result = self._eval_not_null(check)
            elif check.type == "max_null_ratio":
                result = self._eval_max_null_ratio(check)
            else:
                msg = f"Unsupported check type: {check.type!r}"
                result = CheckResult(
                    name=check.name,
                    type=check.type,
                    column=check.column,
                    level=check.level,
                    passed=False,
                    message=msg,
                    details={},
                )
            results.append(result)

        return results

    def quality_report(self, checks: Iterable[CheckDef]) -> pl.DataFrame:
        """
        Run checks and return a tabular Polars DataFrame with results.
        """
        results = self.run_checks(checks)

        return pl.DataFrame(
            {
                "check": [r.name for r in results],
                "type": [r.type for r in results],
                "column": [r.column for r in results],
                "level": [r.level.value for r in results],
                "passed": [r.passed for r in results],
                "message": [r.message for r in results],
            }
        )

    # --- YAML helpers ---

    def run_yaml(self, path: str) -> List[CheckResult]:
        """
        Load checks from a YAML file and execute them.
        """
        checks = load_checks_from_yaml(path)
        return self.run_checks(checks)

    def quality_report_yaml(self, path: str) -> pl.DataFrame:
        """
        Shortcut: load checks from YAML and return a quality report as DataFrame.
        """
        checks = load_checks_from_yaml(path)
        return self.quality_report(checks)

    # -------- Internal helpers for concrete check types --------

    def _eval_not_null(self, check: CheckDef) -> CheckResult:
        col = check.column
        if col not in self._df.columns:
            msg = f"Column {col!r} not found"
            return CheckResult(
                name=check.name,
                type=check.type,
                column=col,
                level=check.level,
                passed=False,
                message=msg,
                details={"column_exists": False},
            )

        null_count = self._df[col].null_count()
        passed = null_count == 0

        if passed:
            msg = f"Column {col!r} has no nulls"
        else:
            msg = f"Column {col!r} has {null_count} null values"

        return CheckResult(
            name=check.name,
            type=check.type,
            column=col,
            level=check.level,
            passed=passed,
            message=msg,
            details={
                "null_count": null_count,
                "row_count": self._df.height,
                "null_ratio": (
                    null_count / self._df.height if self._df.height > 0 else None
                ),
            },
        )

    def _eval_max_null_ratio(self, check: CheckDef) -> CheckResult:
        col = check.column
        params = check.params or {}
        threshold = float(params.get("threshold", 0.0))

        if col not in self._df.columns:
            msg = f"Column {col!r} not found"
            return CheckResult(
                name=check.name,
                type=check.type,
                column=col,
                level=check.level,
                passed=False,
                message=msg,
                details={"column_exists": False, "threshold": threshold},
            )

        row_count = self._df.height
        null_count = self._df[col].null_count()
        null_ratio = null_count / row_count if row_count > 0 else 0.0

        passed = null_ratio <= threshold

        if passed:
            msg = (
                f"Null ratio for column {col!r} is {null_ratio:.4f} "
                f"(<= {threshold:.4f})"
            )
        else:
            msg = (
                f"Null ratio for column {col!r} is {null_ratio:.4f} "
                f"(> {threshold:.4f})"
            )

        return CheckResult(
            name=check.name,
            type=check.type,
            column=col,
            level=check.level,
            passed=passed,
            message=msg,
            details={
                "row_count": row_count,
                "null_count": null_count,
                "null_ratio": null_ratio,
                "threshold": threshold,
            },
        )
