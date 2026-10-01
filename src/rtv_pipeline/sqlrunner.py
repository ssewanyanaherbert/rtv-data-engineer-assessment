"""Templating for SQL models and tests: ``{{ name }}`` is replaced by the matching rule in config/pipeline.yml."""

from __future__ import annotations

import re
from pathlib import Path

from rtv_pipeline.settings import get_settings

_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")
_HEADER = re.compile(r"^--\s*(\w+):\s*(.+)$")


def render(sql: str) -> str:
    rules = get_settings().rules

    def sub(match: re.Match) -> str:
        key = match.group(1)
        if key not in rules:
            raise KeyError(f"Unknown SQL template variable: {key}")
        value = rules[key]
        return f"'{value}'" if isinstance(value, str) else str(value)

    return _PLACEHOLDER.sub(sub, sql)


def read(path: Path) -> str:
    return render(path.read_text(encoding="utf-8"))


def header(path: Path) -> dict[str, str]:
    """Key/value pairs from the leading ``-- key: value`` comment block."""
    meta = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        m = _HEADER.match(line.strip())
        if not m:
            break
        meta[m.group(1)] = m.group(2).strip()
    return meta
