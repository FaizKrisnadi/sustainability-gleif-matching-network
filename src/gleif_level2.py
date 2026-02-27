from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .config import SETTINGS
from .io_utils import (
    detect_file_type,
    ensure_dirs,
    extract_header_fields,
    find_latest_by_patterns,
    get_child,
    get_text,
    iter_xml_records,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover
    pa = None
    pq = None


@dataclass
class ChunkedWriter:
    csv_path: Path
    parquet_path: Optional[Path]
    columns: List[str]

    def __post_init__(self) -> None:
        self.csv_written = False
        self.parquet_writer = None
        self.parquet_enabled = self.parquet_path is not None and pa is not None and pq is not None

    def write_rows(self, rows: List[Dict[str, object]]) -> None:
        if not rows:
            return

        df = pd.DataFrame(rows).reindex(columns=self.columns)
        mode = "w" if not self.csv_written else "a"
        df.to_csv(self.csv_path, mode=mode, header=not self.csv_written, index=False)
        self.csv_written = True

        if self.parquet_enabled:
            table = pa.Table.from_pandas(df, preserve_index=False)
            if self.parquet_writer is None:
                self.parquet_writer = pq.ParquetWriter(self.parquet_path, table.schema, compression="snappy")
            self.parquet_writer.write_table(table)

    def close(self) -> None:
        if self.parquet_writer is not None:
            self.parquet_writer.close()


def resolve_level2_input(explicit_path: Optional[Path] = None) -> Optional[Path]:
    if explicit_path is not None and explicit_path.exists():
        return explicit_path
    if SETTINGS.gleif_level2_input is not None and SETTINGS.gleif_level2_input.exists():
        return SETTINGS.gleif_level2_input
    return find_latest_by_patterns(
        SETTINGS.raw_gleif_dir,
        [
            "*rr*.xml",
            "*rr*.xml.*",
            "*rr*.zip",
        ],
    )


def resolve_repex_input(explicit_path: Optional[Path] = None) -> Optional[Path]:
    if explicit_path is not None and explicit_path.exists():
        return explicit_path
    if SETTINGS.gleif_repex_input is not None and SETTINGS.gleif_repex_input.exists():
        return SETTINGS.gleif_repex_input
    return find_latest_by_patterns(
        SETTINGS.raw_gleif_dir,
        [
            "*repex*.xml",
            "*repex*.xml.*",
            "*repex*.zip",
        ],
    )


def preprocess_level2(input_path: Optional[Path] = None) -> Dict[str, object]:
    ensure_dirs([SETTINGS.raw_gleif_dir, SETTINGS.processed_dir])

    resolved = resolve_level2_input(input_path)
    if resolved is None:
        raise FileNotFoundError("Could not resolve GLEIF Level 2 (RR-CDF) input file.")

    columns = [
        "row_id",
        "parent_lei",
        "child_lei",
        "relationship_type",
        "relationship_status",
        "start_date",
        "end_date",
        "period_type",
        "source_snapshot_date",
        "source_file",
    ]

    header = extract_header_fields(resolved, "Header")
    source_snapshot_date = header.get("ContentDate")

    writer = ChunkedWriter(SETTINGS.level2_csv_path, SETTINGS.level2_parquet_path, columns)
    chunk: List[Dict[str, object]] = []

    rows_seen = 0
    rows_written = 0
    skipped_rows = 0
    parse_errors: List[str] = []

    try:
        for rec in iter_xml_records(resolved, "RelationshipRecord"):
            rows_seen += 1
            try:
                relationship = get_child(rec, "Relationship")
                child_lei = get_text(relationship, ["StartNode", "NodeID"])
                parent_lei = get_text(relationship, ["EndNode", "NodeID"])

                rel_period = None
                periods = get_child(relationship, "RelationshipPeriods")
                if periods is not None:
                    children = list(periods)
                    rel_period = children[0] if children else None

                row = {
                    "row_id": rows_seen,
                    "parent_lei": parent_lei,
                    "child_lei": child_lei,
                    "relationship_type": get_text(relationship, ["RelationshipType"]),
                    "relationship_status": get_text(relationship, ["RelationshipStatus"]),
                    "start_date": get_text(rel_period, ["StartDate"]),
                    "end_date": get_text(rel_period, ["EndDate"]),
                    "period_type": get_text(rel_period, ["PeriodType"]),
                    "source_snapshot_date": source_snapshot_date,
                    "source_file": resolved.name,
                }
                chunk.append(row)

                if len(chunk) >= SETTINGS.write_chunk_size:
                    writer.write_rows(chunk)
                    rows_written += len(chunk)
                    chunk = []
            except Exception as exc:
                skipped_rows += 1
                if len(parse_errors) < 10:
                    parse_errors.append(f"row {rows_seen}: {exc}")

        if chunk:
            writer.write_rows(chunk)
            rows_written += len(chunk)

        if not writer.csv_written:
            pd.DataFrame(columns=columns).to_csv(SETTINGS.level2_csv_path, index=False)
    finally:
        writer.close()

    return {
        "input_path": str(resolved),
        "input_file_type": detect_file_type(resolved),
        "header": header,
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "skipped_rows": skipped_rows,
        "parse_error_samples": parse_errors,
        "outputs": {
            "csv": str(SETTINGS.level2_csv_path),
            "parquet": str(SETTINGS.level2_parquet_path),
        },
    }


def parse_repex(input_path: Optional[Path] = None) -> Dict[str, object]:
    resolved = resolve_repex_input(input_path)
    if resolved is None:
        return {
            "status": "skipped",
            "reason": "No REPEX input path configured/found",
        }

    output_csv = SETTINGS.processed_dir / "gleif_reporting_exceptions_clean.csv"
    columns = [
        "row_id",
        "lei",
        "exception_category",
        "exception_reason",
        "next_version",
        "source_snapshot_date",
        "source_file",
    ]

    header = extract_header_fields(resolved, "Header")
    source_snapshot_date = header.get("ContentDate")

    writer = ChunkedWriter(output_csv, None, columns)
    chunk: List[Dict[str, object]] = []

    rows_seen = 0
    rows_written = 0
    skipped_rows = 0

    try:
        for rec in iter_xml_records(resolved, "Exception"):
            rows_seen += 1
            try:
                row = {
                    "row_id": rows_seen,
                    "lei": get_text(rec, ["LEI"]),
                    "exception_category": get_text(rec, ["ExceptionCategory"]),
                    "exception_reason": get_text(rec, ["ExceptionReason"]),
                    "next_version": get_text(rec, ["NextVersion"]),
                    "source_snapshot_date": source_snapshot_date,
                    "source_file": resolved.name,
                }
                chunk.append(row)
                if len(chunk) >= SETTINGS.write_chunk_size:
                    writer.write_rows(chunk)
                    rows_written += len(chunk)
                    chunk = []
            except Exception:
                skipped_rows += 1

        if chunk:
            writer.write_rows(chunk)
            rows_written += len(chunk)

        if not writer.csv_written:
            pd.DataFrame(columns=columns).to_csv(output_csv, index=False)
    finally:
        writer.close()

    return {
        "input_path": str(resolved),
        "input_file_type": detect_file_type(resolved),
        "header": header,
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "skipped_rows": skipped_rows,
        "outputs": {
            "csv": str(output_csv),
        },
    }
