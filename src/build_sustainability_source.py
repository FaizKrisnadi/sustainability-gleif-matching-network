from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from .clean_names import clean_legal_name
from .config import SETTINGS
from .io_utils import ensure_dirs, write_json


def _std_col(name: str) -> str:
    value = str(name).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def _normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    mapping = {col: _std_col(col) for col in df.columns}
    out = df.rename(columns=mapping).copy()
    return out, mapping


def _pick_column(columns: Iterable[str], preferred: List[str], keywords: List[str]) -> Optional[str]:
    cols = list(columns)
    for pref in preferred:
        if pref in cols:
            return pref

    best_col = None
    best_score = 0
    for col in cols:
        score = sum(1 for kw in keywords if kw in col)
        if score > best_score:
            best_score = score
            best_col = col
    return best_col if best_score > 0 else None


@dataclass
class SourceFrame:
    frame: pd.DataFrame
    diagnostics: Dict[str, object]


def _build_source_frame(
    df: pd.DataFrame,
    source_label: str,
    default_initiative_flag: str,
    notes_prefix: str,
    partial_subset_note: Optional[str] = None,
) -> SourceFrame:
    raw_cols = [str(c) for c in df.columns]
    std_df, renaming = _normalize_columns(df)

    name_col = _pick_column(
        std_df.columns,
        preferred=["company_name", "member_name", "name"],
        keywords=["company", "member", "name", "organization"],
    )
    country_col = _pick_column(
        std_df.columns,
        preferred=["country", "location", "headquarters"],
        keywords=["country", "location", "headquarter", "hq"],
    )
    sector_col = _pick_column(
        std_df.columns,
        preferred=["sector", "industry"],
        keywords=["sector", "industry"],
    )
    url_col = _pick_column(
        std_df.columns,
        preferred=["website", "url", "source_url"],
        keywords=["website", "url", "link"],
    )

    if name_col is None:
        raise ValueError(f"Could not detect company-name column for {source_label}")

    out = pd.DataFrame()
    out["source_name_raw"] = std_df[name_col].fillna("").astype(str).str.strip()
    out["source_name"] = out["source_name_raw"]
    out["source_name_clean"] = out["source_name"].map(clean_legal_name)
    out["source_country"] = std_df[country_col].fillna("").astype(str).str.strip() if country_col else ""
    out["source_sector"] = std_df[sector_col].fillna("").astype(str).str.strip() if sector_col else ""
    out["source_url"] = std_df[url_col].fillna("").astype(str).str.strip() if url_col else ""
    out["source_accessed_date"] = datetime.now(timezone.utc).date().isoformat()

    notes = notes_prefix
    if partial_subset_note:
        notes = f"{notes_prefix}; {partial_subset_note}"
    out["source_notes"] = notes

    for flag in ["has_sbti", "has_re100", "has_ev100", "has_ep100"]:
        out[flag] = 0
    out[default_initiative_flag] = 1

    out = out[out["source_name_raw"].str.strip() != ""].copy()
    out["source_country_norm"] = out["source_country"].str.strip().str.lower()

    diag = {
        "raw_columns": raw_cols,
        "normalized_columns": list(std_df.columns),
        "column_renaming": renaming,
        "selected_columns": {
            "name_col": name_col,
            "country_col": country_col,
            "sector_col": sector_col,
            "url_col": url_col,
        },
        "input_rows": int(len(df)),
        "rows_with_nonblank_name": int(len(out)),
    }
    return SourceFrame(frame=out, diagnostics=diag)


def _dedupe(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    before = len(df)
    deduped = df.drop_duplicates(subset=["source_name_clean", "source_country_norm"], keep="first").copy()
    removed = before - len(deduped)
    return deduped, removed


def _merge_flags(unified: pd.DataFrame, incoming: pd.DataFrame, initiative_flag: str, initiative_name: str) -> Dict[str, object]:
    matched = 0
    added = 0

    for row in incoming.itertuples(index=False):
        clean = str(row.source_name_clean).strip()
        if not clean:
            continue

        country_norm = str(row.source_country_norm).strip()
        mask = unified["source_name_clean"] == clean
        if country_norm:
            strict = mask & (unified["source_country_norm"] == country_norm)
            if strict.any():
                mask = strict

        if mask.any():
            matched += 1
            unified.loc[mask, initiative_flag] = 1
        else:
            added += 1
            new_row = {
                "source_name_raw": row.source_name_raw,
                "source_name": row.source_name,
                "source_name_clean": clean,
                "source_country": row.source_country,
                "source_country_norm": country_norm,
                "source_sector": row.source_sector,
                "source_notes": f"Added from {initiative_name}; not found in SBTi base. {row.source_notes}",
                "has_sbti": 0,
                "has_re100": 0,
                "has_ev100": 0,
                "has_ep100": 0,
                "source_url": row.source_url,
                "source_accessed_date": row.source_accessed_date,
            }
            new_row[initiative_flag] = 1
            unified = pd.concat([unified, pd.DataFrame([new_row])], ignore_index=True)

    return {
        "matched_to_existing_rows": matched,
        "added_outside_sbti": added,
        "rows_after_merge": int(len(unified)),
        "dataframe": unified,
    }


def _fallback_initiative_path(filename: str) -> Path:
    return SETTINGS.raw_dir / "initiative_lists" / filename


def build_sustainability_source(
    sbti_excel_path: Optional[Path] = None,
    re100_csv_path: Optional[Path] = None,
    ev100_csv_path: Optional[Path] = None,
    ep100_csv_path: Optional[Path] = None,
    output_csv_path: Optional[Path] = None,
    diagnostics_json_path: Optional[Path] = None,
) -> Dict[str, object]:
    ensure_dirs([SETTINGS.raw_dir, SETTINGS.processed_dir])

    sbti_path = sbti_excel_path or SETTINGS.sbti_excel_input
    re100_path = re100_csv_path or SETTINGS.re100_csv_input or _fallback_initiative_path("re100_members_first100_from_paste.csv")
    ev100_path = ev100_csv_path or SETTINGS.ev100_csv_input or _fallback_initiative_path("ev100_members_from_paste.csv")
    ep100_path = ep100_csv_path or SETTINGS.ep100_csv_input or _fallback_initiative_path("ep100_members_from_paste.csv")

    if sbti_path is None:
        raise ValueError("SBTi Excel path is required via config or CLI argument.")

    output_csv = output_csv_path or SETTINGS.sustainability_output_path
    diagnostics_json = diagnostics_json_path or (SETTINGS.processed_dir / "sustainability_source_build_diagnostics.json")

    sbti_raw = pd.read_excel(sbti_path, sheet_name=0, dtype=str).fillna("")
    re100_raw = pd.read_csv(re100_path, dtype=str).fillna("")
    ev100_raw = pd.read_csv(ev100_path, dtype=str).fillna("")
    ep100_raw = pd.read_csv(ep100_path, dtype=str).fillna("")

    sbti = _build_source_frame(
        sbti_raw,
        source_label="SBTi",
        default_initiative_flag="has_sbti",
        notes_prefix="Primary source: SBTi company dataset",
    )
    re100 = _build_source_frame(
        re100_raw,
        source_label="RE100",
        default_initiative_flag="has_re100",
        notes_prefix="Manual initiative source: RE100",
        partial_subset_note="RE100 file indicates a partial first100 subset",
    )
    ev100 = _build_source_frame(
        ev100_raw,
        source_label="EV100",
        default_initiative_flag="has_ev100",
        notes_prefix="Manual initiative source: EV100",
    )
    ep100 = _build_source_frame(
        ep100_raw,
        source_label="EP100",
        default_initiative_flag="has_ep100",
        notes_prefix="Manual initiative source: EP100",
    )

    sbti_before_dedupe = len(sbti.frame)
    unified, sbti_dedup_removed = _dedupe(sbti.frame.copy())

    re100_dedup, _ = _dedupe(re100.frame.copy())
    ev100_dedup, _ = _dedupe(ev100.frame.copy())
    ep100_dedup, _ = _dedupe(ep100.frame.copy())

    merge_re100 = _merge_flags(unified, re100_dedup, "has_re100", "RE100 manual list")
    unified = merge_re100.pop("dataframe")
    merge_ev100 = _merge_flags(unified, ev100_dedup, "has_ev100", "EV100 manual list")
    unified = merge_ev100.pop("dataframe")
    merge_ep100 = _merge_flags(unified, ep100_dedup, "has_ep100", "EP100 manual list")
    unified = merge_ep100.pop("dataframe")

    final_before_dedupe = len(unified)
    unified, final_dup_removed = _dedupe(unified)

    unified = unified.reset_index(drop=True)
    unified["source_id"] = [f"SUS{idx:07d}" for idx in range(1, len(unified) + 1)]

    out_cols = [
        "source_id",
        "source_name",
        "source_country",
        "source_sector",
        "source_notes",
        "has_sbti",
        "has_re100",
        "has_ev100",
        "has_ep100",
        "source_url",
        "source_accessed_date",
    ]
    for col in out_cols:
        if col not in unified.columns:
            unified[col] = ""

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    unified[out_cols].to_csv(output_csv, index=False)

    diagnostics = {
        "input_files": {
            "sbti_excel_path": str(sbti_path),
            "re100_csv_path": str(re100_path),
            "ev100_csv_path": str(ev100_path),
            "ep100_csv_path": str(ep100_path),
        },
        "detected_input_columns": {
            "sbti": sbti.diagnostics,
            "re100": re100.diagnostics,
            "ev100": ev100.diagnostics,
            "ep100": ep100.diagnostics,
        },
        "row_counts": {
            "sbti_raw_rows": int(len(sbti_raw)),
            "sbti_rows_before_dedupe": int(sbti_before_dedupe),
            "sbti_dedup_removed": int(sbti_dedup_removed),
            "re100_raw_rows": int(len(re100_raw)),
            "ev100_raw_rows": int(len(ev100_raw)),
            "ep100_raw_rows": int(len(ep100_raw)),
            "after_re100_merge": int(merge_re100["rows_after_merge"]),
            "after_ev100_merge": int(merge_ev100["rows_after_merge"]),
            "after_ep100_merge": int(merge_ep100["rows_after_merge"]),
            "final_before_dedupe": int(final_before_dedupe),
            "final_dedup_removed": int(final_dup_removed),
            "final_rows": int(len(unified)),
        },
        "outputs": {
            "sustainability_source_csv": str(output_csv),
        },
    }

    write_json(diagnostics_json, diagnostics)
    return diagnostics
