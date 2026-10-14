from __future__ import annotations

import ast
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SCAN_ROOTS = (
    _PROJECT_ROOT / "src" / "payrelay",
    _PROJECT_ROOT / "tests",
    _PROJECT_ROOT / "alembic",
)
_SKIP_PARTS = {".venv", "__pycache__", ".git", ".ruff_cache", ".mypy_cache"}


def _iter_python_files() -> list[Path]:
    """Собрать отслеживаемые `.py` без venv/кэшей.

    Returns:
        Отсортированный список путей.
    """
    files: list[Path] = []
    for root in _SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if _SKIP_PARTS.intersection(path.parts):
                continue
            files.append(path)
    return sorted(files)


@pytest.mark.unit
def test_no_module_level_docstrings() -> None:
    """В исходниках PayRelay не должно быть docstring уровня модуля."""
    offenders: list[str] = []
    for path in _iter_python_files():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        if ast.get_docstring(tree) is not None:
            offenders.append(str(path.relative_to(_PROJECT_ROOT)))
    assert offenders == [], f"Module docstrings found: {offenders}"


@pytest.mark.unit
def test_function_and_class_docstrings_preserved_in_api() -> None:
    """Функциональные Google-style docstring в API-слое сохраняются."""
    path = _PROJECT_ROOT / "src" / "payrelay" / "api" / "schemas.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert ast.get_docstring(tree) is None

    by_name = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    assert ast.get_docstring(by_name["validate_idempotency_key_value"])
    assert ast.get_docstring(by_name["CreatePaymentRequest"])
