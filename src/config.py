from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None


DEFAULT_CONFIG: Dict[str, Any] = {
    "paths": {
        "raw_dir": "data/raw",
        "raw_gleif_dir": "data/raw/gleif",
        "processed_dir": "data/processed",
        "samples_dir": "data/samples",
        "logs_dir": "logs",
        "sustainability_template": "data/raw/sustainability_source_template.csv",
        "sustainability_output": "data/processed/sustainability_source.csv",
        "run_manifest": "data/processed/run_manifest.json",
        "match_crosswalk": "data/processed/match_crosswalk.csv",
        "review_candidates": "data/processed/review_candidates.csv",
        "match_diagnostics": "data/processed/match_diagnostics.json",
        "level1_csv": "data/processed/gleif_entities_clean.csv",
        "level1_parquet": "data/processed/gleif_entities_clean.parquet",
        "level2_csv": "data/processed/gleif_relationships_clean.csv",
        "level2_parquet": "data/processed/gleif_relationships_clean.parquet",
        "edges_csv": "data/processed/edges.csv",
        "nodes_csv": "data/processed/nodes.csv",
        "preprocess_stats": "data/processed/preprocess_stats.json",
    },
    "inputs": {
        "gleif_level1": "",
        "gleif_level2": "",
        "gleif_repex": "",
        "sbti_excel": "",
        "re100_csv": "",
        "ev100_csv": "",
        "ep100_csv": "",
        "sustainability_source_csv": "",
    },
    "matching": {
        "auto_threshold": 95.0,
        "review_threshold": 85.0,
        "unmatched_threshold": 0.0,
        "top_k_candidates": 3,
        "max_block_candidates": 50,
    },
    "blocking": {
        "use_country": True,
        "use_first_token": True,
        "use_first_char": True,
    },
    "parsing": {
        "write_chunk_size": 100000,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_path(value: str, project_root: Path) -> Optional[Path]:
    raw = (value or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path


def _require_yaml_if_needed(config_path: Path) -> None:
    if yaml is None:
        raise RuntimeError(
            f"Found config file at {config_path}, but PyYAML is unavailable. Install 'pyyaml' to load YAML config files."
        )


def _load_yaml(path: Path) -> Dict[str, Any]:
    _require_yaml_if_needed(path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError(f"Config root must be a mapping/dict: {path}")
    return payload


@dataclass(frozen=True)
class Settings:
    project_root: Path
    config_path: Optional[Path]

    raw_dir: Path
    raw_gleif_dir: Path
    processed_dir: Path
    samples_dir: Path
    logs_dir: Path

    sustainability_template_path: Path
    sustainability_output_path: Path
    run_manifest_path: Path
    match_crosswalk_path: Path
    review_candidates_path: Path
    match_diagnostics_path: Path
    level1_csv_path: Path
    level1_parquet_path: Path
    level2_csv_path: Path
    level2_parquet_path: Path
    edges_csv_path: Path
    nodes_csv_path: Path
    preprocess_stats_path: Path

    gleif_level1_input: Optional[Path]
    gleif_level2_input: Optional[Path]
    gleif_repex_input: Optional[Path]
    sbti_excel_input: Optional[Path]
    re100_csv_input: Optional[Path]
    ev100_csv_input: Optional[Path]
    ep100_csv_input: Optional[Path]
    sustainability_source_input: Optional[Path]

    auto_match_threshold: float
    review_match_threshold: float
    unmatched_threshold: float
    top_k_candidates: int
    max_block_candidates: int

    block_on_country: bool
    block_on_first_token: bool
    block_on_first_char: bool

    write_chunk_size: int


def load_settings(config_path: Optional[Path] = None) -> Settings:
    project_root = Path(__file__).resolve().parents[1]
    explicit_path = config_path
    if explicit_path is None:
        env_path = os.getenv("SGMN_CONFIG_PATH", "").strip()
        explicit_path = Path(env_path).expanduser() if env_path else None

    default_path = project_root / "config.yaml"
    candidate = explicit_path or (default_path if default_path.exists() else None)

    user_config: Dict[str, Any] = {}
    used_config: Optional[Path] = None
    if candidate is not None and candidate.exists():
        user_config = _load_yaml(candidate)
        used_config = candidate

    cfg = _deep_merge(DEFAULT_CONFIG, user_config)

    paths = cfg["paths"]
    inputs = cfg["inputs"]
    matching = cfg["matching"]
    blocking = cfg["blocking"]
    parsing = cfg["parsing"]

    def path_required(key: str) -> Path:
        resolved = _resolve_path(str(paths[key]), project_root)
        if resolved is None:
            raise ValueError(f"Config path '{key}' cannot be empty")
        return resolved

    return Settings(
        project_root=project_root,
        config_path=used_config,
        raw_dir=path_required("raw_dir"),
        raw_gleif_dir=path_required("raw_gleif_dir"),
        processed_dir=path_required("processed_dir"),
        samples_dir=path_required("samples_dir"),
        logs_dir=path_required("logs_dir"),
        sustainability_template_path=path_required("sustainability_template"),
        sustainability_output_path=path_required("sustainability_output"),
        run_manifest_path=path_required("run_manifest"),
        match_crosswalk_path=path_required("match_crosswalk"),
        review_candidates_path=path_required("review_candidates"),
        match_diagnostics_path=path_required("match_diagnostics"),
        level1_csv_path=path_required("level1_csv"),
        level1_parquet_path=path_required("level1_parquet"),
        level2_csv_path=path_required("level2_csv"),
        level2_parquet_path=path_required("level2_parquet"),
        edges_csv_path=path_required("edges_csv"),
        nodes_csv_path=path_required("nodes_csv"),
        preprocess_stats_path=path_required("preprocess_stats"),
        gleif_level1_input=_resolve_path(str(inputs.get("gleif_level1", "")), project_root),
        gleif_level2_input=_resolve_path(str(inputs.get("gleif_level2", "")), project_root),
        gleif_repex_input=_resolve_path(str(inputs.get("gleif_repex", "")), project_root),
        sbti_excel_input=_resolve_path(str(inputs.get("sbti_excel", "")), project_root),
        re100_csv_input=_resolve_path(str(inputs.get("re100_csv", "")), project_root),
        ev100_csv_input=_resolve_path(str(inputs.get("ev100_csv", "")), project_root),
        ep100_csv_input=_resolve_path(str(inputs.get("ep100_csv", "")), project_root),
        sustainability_source_input=_resolve_path(str(inputs.get("sustainability_source_csv", "")), project_root),
        auto_match_threshold=float(matching["auto_threshold"]),
        review_match_threshold=float(matching["review_threshold"]),
        unmatched_threshold=float(matching["unmatched_threshold"]),
        top_k_candidates=int(matching["top_k_candidates"]),
        max_block_candidates=int(matching["max_block_candidates"]),
        block_on_country=bool(blocking["use_country"]),
        block_on_first_token=bool(blocking["use_first_token"]),
        block_on_first_char=bool(blocking["use_first_char"]),
        write_chunk_size=int(parsing["write_chunk_size"]),
    )


SETTINGS = load_settings()
