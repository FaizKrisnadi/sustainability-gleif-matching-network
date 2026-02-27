from __future__ import annotations

import json
import traceback
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .config import SETTINGS
from .io_utils import (
    append_markdown_lines,
    detect_file_type,
    ensure_dirs,
    format_kv_lines,
    utc_now_iso,
    write_json,
)


def _command_log_path() -> Path:
    return SETTINGS.logs_dir / "COMMAND_LOG.md"


def _run_summary_path() -> Path:
    return SETTINGS.logs_dir / "RUN_SUMMARY.md"


def _decision_log_path() -> Path:
    return SETTINGS.logs_dir / "DECISION_LOG.md"


def _error_log_path() -> Path:
    return SETTINGS.logs_dir / "ERROR_LOG.md"


def ensure_log_files() -> None:
    ensure_dirs([SETTINGS.logs_dir])
    defaults = {
        _command_log_path(): "# Command Log",
        _run_summary_path(): "# Run Summary",
        _decision_log_path(): "# Decision Log",
        _error_log_path(): "# Error Log",
    }
    for path, header in defaults.items():
        if not path.exists():
            path.write_text(f"{header}\n", encoding="utf-8")


def log_command(command_text: str) -> None:
    ensure_log_files()
    append_markdown_lines(_command_log_path(), [f"- [{utc_now_iso()}] {command_text}"])


def log_error(command_text: str, exc: Exception) -> None:
    ensure_log_files()
    lines = [
        "",
        f"## [{utc_now_iso()}] {command_text}",
        f"- error: {exc}",
        "```",
        traceback.format_exc().rstrip(),
        "```",
    ]
    append_markdown_lines(_error_log_path(), lines)


def log_decision(note: str) -> None:
    ensure_log_files()
    append_markdown_lines(
        _decision_log_path(),
        [
            "",
            f"## [{utc_now_iso()}] Decision",
            f"- {note}",
        ],
    )


def _flatten_metrics(prefix: str, payload: Dict[str, object], out: Dict[str, object]) -> None:
    for key, value in payload.items():
        joined = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            _flatten_metrics(joined, value, out)
        else:
            out[joined] = value


def append_run_summary(command_name: str, metrics: Dict[str, object]) -> None:
    ensure_log_files()
    flat: Dict[str, object] = {}
    _flatten_metrics("", metrics, flat)
    lines = ["", f"## [{utc_now_iso()}] {command_name}"]
    lines.extend(format_kv_lines(flat))
    append_markdown_lines(_run_summary_path(), lines)


def _read_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def inspect_inputs() -> Dict[str, object]:
    from .gleif_level1 import resolve_level1_input
    from .gleif_level2 import resolve_level2_input, resolve_repex_input

    level1 = resolve_level1_input()
    level2 = resolve_level2_input()
    repex = resolve_repex_input()

    files = {
        "gleif_level1": level1,
        "gleif_level2": level2,
        "gleif_repex": repex,
        "sbti_excel": SETTINGS.sbti_excel_input,
        "re100_csv": SETTINGS.re100_csv_input,
        "ev100_csv": SETTINGS.ev100_csv_input,
        "ep100_csv": SETTINGS.ep100_csv_input,
        "sustainability_source_csv": SETTINGS.sustainability_source_input or SETTINGS.sustainability_output_path,
    }

    info: Dict[str, object] = {}
    for key, path in files.items():
        if path is None:
            info[key] = {
                "path": None,
                "exists": False,
                "detected_type": "missing",
                "size_bytes": 0,
            }
            continue
        info[key] = detect_file_type(path)

    return {
        "timestamp_utc": utc_now_iso(),
        "config_path": str(SETTINGS.config_path) if SETTINGS.config_path else None,
        "inputs": info,
    }


def build_data_quality_report(preprocess_stats: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    if preprocess_stats is None:
        preprocess_stats = _read_json(SETTINGS.preprocess_stats_path)

    entities = pd.read_csv(SETTINGS.level1_csv_path, dtype=str) if SETTINGS.level1_csv_path.exists() else pd.DataFrame()
    relationships = pd.read_csv(SETTINGS.level2_csv_path, dtype=str) if SETTINGS.level2_csv_path.exists() else pd.DataFrame()
    nodes = pd.read_csv(SETTINGS.nodes_csv_path, dtype=str) if SETTINGS.nodes_csv_path.exists() else pd.DataFrame()
    edges = pd.read_csv(SETTINGS.edges_csv_path, dtype=str) if SETTINGS.edges_csv_path.exists() else pd.DataFrame()

    duplicate_lei_count = int(entities["lei"].duplicated().sum()) if "lei" in entities.columns else 0
    missing_lei_count = int((entities["lei"].fillna("").astype(str).str.strip() == "").sum()) if "lei" in entities.columns else 0

    duplicate_edges = (
        int(edges.duplicated(subset=["firm_i", "firm_j", "relation_type", "year"]).sum())
        if {"firm_i", "firm_j", "relation_type", "year"}.issubset(set(edges.columns))
        else 0
    )

    report = {
        "checks": {
            "duplicate_leis_in_entities": duplicate_lei_count,
            "missing_lei_in_entities": missing_lei_count,
            "duplicate_edges": duplicate_edges,
        },
        "row_counts": {
            "entities": int(len(entities)),
            "relationships": int(len(relationships)),
            "nodes": int(len(nodes)),
            "edges": int(len(edges)),
            "level1_rows_seen": int(preprocess_stats.get("level1", {}).get("rows_seen", 0)) if preprocess_stats else 0,
            "level1_rows_written": int(preprocess_stats.get("level1", {}).get("rows_written", 0)) if preprocess_stats else 0,
            "level2_rows_seen": int(preprocess_stats.get("level2", {}).get("rows_seen", 0)) if preprocess_stats else 0,
            "level2_rows_written": int(preprocess_stats.get("level2", {}).get("rows_written", 0)) if preprocess_stats else 0,
        },
    }

    write_json(SETTINGS.processed_dir / "data_quality.json", report)
    return report


def write_preprocess_stats(
    level1_stats: Optional[Dict[str, object]] = None,
    level2_stats: Optional[Dict[str, object]] = None,
    repex_stats: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    current = _read_json(SETTINGS.preprocess_stats_path)

    if level1_stats is not None:
        current["level1"] = level1_stats
    if level2_stats is not None:
        current["level2"] = level2_stats
    if repex_stats is not None:
        current["repex"] = repex_stats

    write_json(SETTINGS.preprocess_stats_path, current)
    return current


def build_run_manifest(
    level1_stats: Optional[Dict[str, object]] = None,
    level2_stats: Optional[Dict[str, object]] = None,
    outputs: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    current = _read_json(SETTINGS.run_manifest_path)
    preprocess_stats = _read_json(SETTINGS.preprocess_stats_path)

    level1 = level1_stats or preprocess_stats.get("level1", {})
    level2 = level2_stats or preprocess_stats.get("level2", {})

    merged_outputs: Dict[str, str] = dict(current.get("outputs", {}))
    for candidate in [
        SETTINGS.level1_csv_path,
        SETTINGS.level2_csv_path,
        SETTINGS.nodes_csv_path,
        SETTINGS.edges_csv_path,
        SETTINGS.sustainability_output_path,
        SETTINGS.match_crosswalk_path,
        SETTINGS.match_diagnostics_path,
    ]:
        if candidate.exists():
            merged_outputs[candidate.name] = str(candidate)
    if outputs:
        merged_outputs.update(outputs)

    inputs: Dict[str, object] = {}
    for label, stats in [("level1", level1), ("level2", level2)]:
        input_path = stats.get("input_path")
        if input_path:
            inputs[label] = detect_file_type(Path(str(input_path)))

    manifest = {
        "timestamp_utc": utc_now_iso(),
        "inputs": inputs,
        "level1": {
            "rows_read": int(level1.get("rows_seen", 0)),
            "rows_written": int(level1.get("rows_written", 0)),
            "skipped_rows": int(level1.get("skipped_rows", 0)),
        },
        "level2": {
            "rows_read": int(level2.get("rows_seen", 0)),
            "rows_written": int(level2.get("rows_written", 0)),
            "skipped_rows": int(level2.get("skipped_rows", 0)),
        },
        "outputs": merged_outputs,
    }

    write_json(SETTINGS.run_manifest_path, manifest)
    return manifest


def create_output_samples(sample_rows: int = 50) -> Dict[str, object]:
    ensure_dirs([SETTINGS.samples_dir])

    outputs = {
        "match_crosswalk": (SETTINGS.match_crosswalk_path, SETTINGS.samples_dir / "match_crosswalk_sample.csv"),
        "edges": (SETTINGS.edges_csv_path, SETTINGS.samples_dir / "edges_sample.csv"),
        "nodes": (SETTINGS.nodes_csv_path, SETTINGS.samples_dir / "nodes_sample.csv"),
    }

    summary: Dict[str, object] = {}

    for label, (src, dst) in outputs.items():
        if src.exists():
            df = pd.read_csv(src, nrows=sample_rows, dtype=str)
            df.to_csv(dst, index=False)
            summary[label] = {
                "source": str(src),
                "sample": str(dst),
                "sample_rows": int(len(df)),
            }
        else:
            pd.DataFrame().to_csv(dst, index=False)
            summary[label] = {
                "source": str(src),
                "sample": str(dst),
                "sample_rows": 0,
            }

    manifest_sample_path = SETTINGS.samples_dir / "run_manifest_sample.json"
    if SETTINGS.run_manifest_path.exists():
        manifest = _read_json(SETTINGS.run_manifest_path)
        write_json(manifest_sample_path, manifest)
        summary["run_manifest"] = {
            "sample": str(manifest_sample_path),
        }

    return summary
