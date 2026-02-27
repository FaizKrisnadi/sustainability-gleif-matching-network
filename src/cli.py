from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict

from .build_network import build_network_outputs
from .build_sustainability_source import build_sustainability_source
from .diagnostics import (
    append_run_summary,
    build_data_quality_report,
    build_run_manifest,
    create_output_samples,
    ensure_log_files,
    inspect_inputs,
    log_command,
    log_error,
    write_preprocess_stats,
)
from .gleif_level1 import preprocess_level1
from .gleif_level2 import parse_repex, preprocess_level2
from .match_sustainability import run_matching
from .config import SETTINGS
from .io_utils import ensure_dirs, write_json


def _run_with_logging(command_name: str, fn) -> Dict[str, object]:
    ensure_log_files()
    command_text = f"python -m src.cli {' '.join(sys.argv[1:])}".strip()
    log_command(command_text)
    try:
        metrics = fn()
        append_run_summary(command_name, metrics)
        return metrics
    except Exception as exc:
        log_error(command_text, exc)
        raise


def cmd_inspect_inputs(args: argparse.Namespace) -> Dict[str, object]:
    info = inspect_inputs()
    inspect_path = SETTINGS.processed_dir / "inspect_inputs.json"
    write_json(inspect_path, info)
    return {
        "inspect_file": str(inspect_path),
        "inputs_present": sum(1 for _, v in info["inputs"].items() if v.get("exists")),
        "inputs_total": len(info["inputs"]),
    }


def cmd_preprocess_level1(args: argparse.Namespace) -> Dict[str, object]:
    input_path = Path(args.input) if args.input else None
    stats = preprocess_level1(input_path)
    write_preprocess_stats(level1_stats=stats)
    manifest = build_run_manifest(level1_stats=stats)
    return {
        "input_path": stats.get("input_path"),
        "rows_read": stats.get("rows_seen", 0),
        "rows_written": stats.get("rows_written", 0),
        "skipped_rows": stats.get("skipped_rows", 0),
        "output_csv": stats.get("outputs", {}).get("csv"),
        "manifest": str(SETTINGS.run_manifest_path),
        "manifest_timestamp": manifest.get("timestamp_utc"),
    }


def cmd_preprocess_level2(args: argparse.Namespace) -> Dict[str, object]:
    input_path = Path(args.input) if args.input else None
    stats = preprocess_level2(input_path)
    repex_stats = parse_repex() if args.parse_repex else {"status": "skipped", "reason": "parse_repex disabled"}
    write_preprocess_stats(level2_stats=stats, repex_stats=repex_stats)
    manifest = build_run_manifest(level2_stats=stats)
    return {
        "input_path": stats.get("input_path"),
        "rows_read": stats.get("rows_seen", 0),
        "rows_written": stats.get("rows_written", 0),
        "skipped_rows": stats.get("skipped_rows", 0),
        "output_csv": stats.get("outputs", {}).get("csv"),
        "repex_status": repex_stats.get("status", "parsed"),
        "manifest": str(SETTINGS.run_manifest_path),
        "manifest_timestamp": manifest.get("timestamp_utc"),
    }


def cmd_build_sustainability_source(args: argparse.Namespace) -> Dict[str, object]:
    diagnostics = build_sustainability_source(
        sbti_excel_path=Path(args.sbti) if args.sbti else None,
        re100_csv_path=Path(args.re100) if args.re100 else None,
        ev100_csv_path=Path(args.ev100) if args.ev100 else None,
        ep100_csv_path=Path(args.ep100) if args.ep100 else None,
        output_csv_path=Path(args.output) if args.output else None,
        diagnostics_json_path=Path(args.diagnostics) if args.diagnostics else None,
    )

    counts = diagnostics.get("row_counts", {})
    return {
        "source_rows_final": counts.get("final_rows", 0),
        "source_rows_sbti_raw": counts.get("sbti_raw_rows", 0),
        "output_csv": diagnostics.get("outputs", {}).get("sustainability_source_csv", ""),
    }


def cmd_match_sustainability(args: argparse.Namespace) -> Dict[str, object]:
    source = Path(args.input) if args.input else None
    diagnostics = run_matching(source)
    build_run_manifest()
    return {
        "source_path": str(source) if source else str(SETTINGS.sustainability_output_path),
        "n_source": diagnostics.get("n_source", 0),
        "n_auto": diagnostics.get("n_auto", 0),
        "n_review": diagnostics.get("n_review", 0),
        "n_unmatched": diagnostics.get("n_unmatched", 0),
    }


def cmd_build_network(args: argparse.Namespace) -> Dict[str, object]:
    network_stats = build_network_outputs()
    quality = build_data_quality_report()
    build_run_manifest(outputs=network_stats.get("outputs", {}))
    return {
        "nodes_rows": network_stats.get("nodes_rows", 0),
        "edges_rows": network_stats.get("edges_rows", 0),
        "output_nodes": network_stats.get("outputs", {}).get("nodes_csv", ""),
        "output_edges": network_stats.get("outputs", {}).get("edges_csv", ""),
        "duplicate_edges": quality.get("checks", {}).get("duplicate_edges", 0),
    }


def cmd_run_all(args: argparse.Namespace) -> Dict[str, object]:
    level1 = preprocess_level1()
    level2 = preprocess_level2()
    repex = parse_repex()
    preprocess_stats = write_preprocess_stats(level1_stats=level1, level2_stats=level2, repex_stats=repex)

    source_build = build_sustainability_source()
    network_stats = build_network_outputs()
    match_stats = run_matching(SETTINGS.sustainability_output_path)

    quality = build_data_quality_report(preprocess_stats=preprocess_stats)
    manifest = build_run_manifest(level1_stats=level1, level2_stats=level2, outputs=network_stats.get("outputs", {}))
    samples = create_output_samples(sample_rows=args.sample_rows)

    return {
        "level1_rows_read": level1.get("rows_seen", 0),
        "level1_rows_written": level1.get("rows_written", 0),
        "level2_rows_read": level2.get("rows_seen", 0),
        "level2_rows_written": level2.get("rows_written", 0),
        "nodes_rows": network_stats.get("nodes_rows", 0),
        "edges_rows": network_stats.get("edges_rows", 0),
        "sustainability_source_rows": source_build.get("row_counts", {}).get("final_rows", 0),
        "match_n_source": match_stats.get("n_source", 0),
        "match_n_auto": match_stats.get("n_auto", 0),
        "match_n_review": match_stats.get("n_review", 0),
        "match_n_unmatched": match_stats.get("n_unmatched", 0),
        "manifest": str(SETTINGS.run_manifest_path),
        "manifest_timestamp": manifest.get("timestamp_utc"),
        "sample_outputs": samples,
        "duplicate_edges": quality.get("checks", {}).get("duplicate_edges", 0),
    }


def _run_tool_script(script_name: str) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "tools" / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Missing tool script: {script_path}")
    subprocess.run([sys.executable, str(script_path)], check=True)


def cmd_sample_matching_eval(args: argparse.Namespace) -> Dict[str, object]:
    _run_tool_script("sample_matching_eval.py")
    return {
        "sample_auto_csv": "data/samples/matching_eval_sample_auto.csv",
        "sample_review_csv": "data/samples/matching_eval_sample_review.csv",
        "sample_unmatched_csv": "data/samples/matching_eval_sample_unmatched.csv",
        "labels_template_csv": "data/samples/matching_eval_labels_template.csv",
    }


def cmd_matching_eval_report(args: argparse.Namespace) -> Dict[str, object]:
    _run_tool_script("generate_matching_eval_report.py")
    return {
        "report_markdown": "docs/matching_eval.md",
        "metrics_json": "data/samples/matching_eval_metrics.json",
    }


def cmd_network_sanity(args: argparse.Namespace) -> Dict[str, object]:
    _run_tool_script("network_sanity.py")
    return {
        "report_markdown": "docs/network_sanity.md",
        "stats_json": "data/samples/network_stats.json",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GLEIF + sustainability matching network pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inspect = sub.add_parser("inspect-inputs", help="Inspect configured input files and detect types")
    p_inspect.set_defaults(func=lambda args: _run_with_logging("inspect-inputs", lambda: cmd_inspect_inputs(args)))

    p_l1 = sub.add_parser("preprocess-gleif-level1", help="Parse GLEIF LEI-CDF Level 1 into clean entity table")
    p_l1.add_argument("--input", default="", help="Optional explicit Level 1 input path")
    p_l1.set_defaults(func=lambda args: _run_with_logging("preprocess-gleif-level1", lambda: cmd_preprocess_level1(args)))

    p_l2 = sub.add_parser("preprocess-gleif-level2", help="Parse GLEIF RR-CDF Level 2 into relationship table")
    p_l2.add_argument("--input", default="", help="Optional explicit Level 2 input path")
    p_l2.add_argument("--parse-repex", action="store_true", help="Also parse REPEX exceptions when available")
    p_l2.set_defaults(func=lambda args: _run_with_logging("preprocess-gleif-level2", lambda: cmd_preprocess_level2(args)))

    p_build = sub.add_parser(
        "build-sustainability-source",
        help="Build sustainability_source.csv from SBTi + manual RE100/EV100/EP100 lists",
    )
    p_build.add_argument("--sbti", default="", help="Path to SBTi Excel")
    p_build.add_argument("--re100", default="", help="Path to RE100 CSV")
    p_build.add_argument("--ev100", default="", help="Path to EV100 CSV")
    p_build.add_argument("--ep100", default="", help="Path to EP100 CSV")
    p_build.add_argument("--output", default="", help="Output sustainability source CSV")
    p_build.add_argument("--diagnostics", default="", help="Output diagnostics JSON")
    p_build.set_defaults(
        func=lambda args: _run_with_logging("build-sustainability-source", lambda: cmd_build_sustainability_source(args))
    )

    p_match = sub.add_parser("match-sustainability", help="Run fuzzy matching to produce crosswalk and diagnostics")
    p_match.add_argument("--input", default="", help="Optional path to sustainability_source.csv")
    p_match.set_defaults(func=lambda args: _run_with_logging("match-sustainability", lambda: cmd_match_sustainability(args)))

    p_network = sub.add_parser("build-network", help="Build nodes.csv + edges.csv from processed Level 1 + Level 2")
    p_network.set_defaults(func=lambda args: _run_with_logging("build-network", lambda: cmd_build_network(args)))

    p_all = sub.add_parser("run-all", help="Run full pipeline end-to-end")
    p_all.add_argument("--sample-rows", type=int, default=50, help="Rows to include in git-safe sample CSVs")
    p_all.set_defaults(func=lambda args: _run_with_logging("run-all", lambda: cmd_run_all(args)))

    p_sample_eval = sub.add_parser(
        "sample-matching-eval",
        help="Create reproducible sampling files and labeling template for matching evaluation",
    )
    p_sample_eval.set_defaults(
        func=lambda args: _run_with_logging("sample-matching-eval", lambda: cmd_sample_matching_eval(args))
    )

    p_eval_report = sub.add_parser(
        "matching-eval-report",
        help="Generate matching quality evaluation report and metrics from manual labels",
    )
    p_eval_report.set_defaults(
        func=lambda args: _run_with_logging("matching-eval-report", lambda: cmd_matching_eval_report(args))
    )

    p_network_sanity = sub.add_parser(
        "network-sanity",
        help="Run lightweight network sanity checks and write stats/report artifacts",
    )
    p_network_sanity.set_defaults(
        func=lambda args: _run_with_logging("network-sanity", lambda: cmd_network_sanity(args))
    )

    return parser


def main() -> None:
    ensure_dirs([SETTINGS.raw_dir, SETTINGS.raw_gleif_dir, SETTINGS.processed_dir, SETTINGS.samples_dir, SETTINGS.logs_dir])
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
