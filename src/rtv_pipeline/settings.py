"""Configuration loading. All tunables live in config/*.yml; secrets come from the environment."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
CONFIG_DIR = PROJECT_ROOT / "config"
SQL_DIR = PROJECT_ROOT / "sql"

_ENV_PATTERN = re.compile(r"\$\{(\w+)(?::-([^}]*))?\}")


def _expand_env(value):
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_yaml(name: str) -> dict:
    with open(CONFIG_DIR / name, encoding="utf-8") as fh:
        return _expand_env(yaml.safe_load(fh))


@dataclass(frozen=True)
class Cycle:
    code: str
    name: str
    order: int
    source_file: str
    form_file: str


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    lake_dir: Path
    reports_dir: Path
    cycles: tuple[Cycle, ...]
    spark: dict
    rules: dict

    def cycle(self, code: str) -> Cycle:
        for c in self.cycles:
            if c.code == code:
                return c
        raise KeyError(f"Unknown survey cycle: {code}")

    @property
    def bronze_dir(self) -> Path:
        return self.lake_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.lake_dir / "silver"

    @property
    def catalog_dir(self) -> Path:
        return self.lake_dir / "catalog"


def _resolve(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else PROJECT_ROOT / p


@lru_cache
def get_settings() -> Settings:
    cfg = load_yaml("pipeline.yml")
    paths = cfg["paths"]
    return Settings(
        data_dir=_resolve(paths["data_dir"]),
        lake_dir=_resolve(paths["lake_dir"]),
        reports_dir=_resolve(paths["reports_dir"]),
        cycles=tuple(Cycle(**c) for c in sorted(cfg["cycles"], key=lambda c: c["order"])),
        spark=cfg["spark"],
        rules=cfg["rules"],
    )


@lru_cache
def get_variable_map() -> dict:
    return load_yaml("variable_map.yml")["fields"]


def warehouse_dsn() -> str:
    return (
        f"host={os.environ.get('DB_HOST', 'localhost')} "
        f"port={os.environ.get('DB_PORT', '5432')} "
        f"dbname={os.environ.get('DB_NAME', 'rtv_warehouse')} "
        f"user={os.environ.get('DB_USER', 'rtv')} "
        f"password={os.environ.get('DB_PASSWORD', '')}"
    )
