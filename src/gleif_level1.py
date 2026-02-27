from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .clean_names import clean_legal_name
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


def resolve_level1_input(explicit_path: Optional[Path] = None) -> Optional[Path]:
    if explicit_path is not None and explicit_path.exists():
        return explicit_path
    if SETTINGS.gleif_level1_input is not None and SETTINGS.gleif_level1_input.exists():
        return SETTINGS.gleif_level1_input
    return find_latest_by_patterns(
        SETTINGS.raw_gleif_dir,
        [
            "*lei2*.xml",
            "*lei2*.xml.*",
            "*lei2*.zip",
            "*lei*.xml",
            "*lei*.xml.*",
            "*lei*.zip",
        ],
    )


def preprocess_level1(input_path: Optional[Path] = None) -> Dict[str, object]:
    ensure_dirs([SETTINGS.raw_gleif_dir, SETTINGS.processed_dir])

    resolved = resolve_level1_input(input_path)
    if resolved is None:
        raise FileNotFoundError("Could not resolve GLEIF Level 1 input file.")

    columns = [
        "row_id",
        "lei",
        "legal_name",
        "legal_name_clean",
        "country",
        "entity_status",
        "registration_authority",
        "legal_address_country",
        "headquarters_country",
        "source_snapshot_date",
        "source_file",
    ]

    header = extract_header_fields(resolved, "LEIHeader")
    source_snapshot_date = header.get("ContentDate")

    writer = ChunkedWriter(SETTINGS.level1_csv_path, SETTINGS.level1_parquet_path, columns)
    chunk: List[Dict[str, object]] = []

    rows_seen = 0
    rows_written = 0
    skipped_rows = 0
    parse_errors: List[str] = []

    try:
        for rec in iter_xml_records(resolved, "LEIRecord"):
            rows_seen += 1
            try:
                entity = get_child(rec, "Entity")
                row = {
                    "row_id": rows_seen,
                    "lei": get_text(rec, ["LEI"]),
                    "legal_name": get_text(entity, ["LegalName"]),
                    "legal_name_clean": clean_legal_name(get_text(entity, ["LegalName"]) or ""),
                    "country": get_text(entity, ["HeadquartersAddress", "Country"])
                    or get_text(entity, ["LegalAddress", "Country"]),
                    "entity_status": get_text(entity, ["EntityStatus"]),
                    "registration_authority": get_text(
                        entity,
                        ["RegistrationAuthority", "RegistrationAuthorityID"],
                    ),
                    "legal_address_country": get_text(entity, ["LegalAddress", "Country"]),
                    "headquarters_country": get_text(entity, ["HeadquartersAddress", "Country"]),
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
            pd.DataFrame(columns=columns).to_csv(SETTINGS.level1_csv_path, index=False)
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
            "csv": str(SETTINGS.level1_csv_path),
            "parquet": str(SETTINGS.level1_parquet_path),
        },
    }
