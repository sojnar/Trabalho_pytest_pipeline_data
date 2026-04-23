from __future__ import annotations

import csv
from pathlib import Path

from pipeline.sellin_validation import split_issues, validate_csv

BASE_DIR = Path(__file__).resolve().parents[1]
MOCKS_DIR = BASE_DIR / "data" / "mocks"


def load_manifest(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def codes_for_row(issues, severity: str) -> dict[int, set[str]]:
    mapping: dict[int, set[str]] = {}
    for issue in issues:
        if issue.severity != severity or issue.row_number is None:
            continue
        mapping.setdefault(issue.row_number, set()).add(issue.code)
    return mapping


def test_valid_mock_has_no_errors_or_warnings() -> None:
    issues = validate_csv(MOCKS_DIR / "sellin_mock_valid.csv")
    errors, warnings = split_issues(issues)
    assert not errors
    assert not warnings


def test_warning_mock_only_raises_expected_warnings() -> None:
    issues = validate_csv(MOCKS_DIR / "sellin_mock_warning.csv")
    errors, warnings = split_issues(issues)
    assert not errors
    assert warnings

    warning_codes = codes_for_row(warnings, "warning")
    manifest = load_manifest(MOCKS_DIR / "sellin_mock_warning_manifest.csv")

    for expected in manifest:
        row_number = int(expected["row_number"])
        assert expected["expected_code"] in warning_codes.get(row_number, set())


def test_invalid_mock_raises_expected_errors() -> None:
    issues = validate_csv(MOCKS_DIR / "sellin_mock_invalid.csv")
    errors, _ = split_issues(issues)
    assert errors

    error_codes = codes_for_row(errors, "error")
    manifest = load_manifest(MOCKS_DIR / "sellin_mock_invalid_manifest.csv")

    for expected in manifest:
        row_number = int(expected["row_number"])
        assert expected["expected_code"] in error_codes.get(row_number, set())


def test_invalid_batch_mock_raises_expected_errors() -> None:
    issues = validate_csv(MOCKS_DIR / "sellin_mock_invalid_batch.csv")
    errors, _ = split_issues(issues)
    assert errors

    error_codes = codes_for_row(errors, "error")
    manifest = load_manifest(MOCKS_DIR / "sellin_mock_invalid_batch_manifest.csv")

    for expected in manifest:
        row_number = int(expected["row_number"])
        assert expected["expected_code"] in error_codes.get(row_number, set())
