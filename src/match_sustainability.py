from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

try:
    from rapidfuzz import fuzz, process
except Exception:  # pragma: no cover
    fuzz = None
    process = None

from .clean_names import clean_legal_name
from .config import SETTINGS
from .io_utils import write_json


@dataclass(frozen=True)
class MatchThresholds:
    auto: float = SETTINGS.auto_match_threshold
    review: float = SETTINGS.review_match_threshold
    unmatched: float = SETTINGS.unmatched_threshold


def classify_match_status(score: Optional[float], thresholds: MatchThresholds = MatchThresholds()) -> str:
    if score is None:
        return "unmatched"
    if score >= thresholds.auto:
        return "auto"
    if score >= thresholds.review:
        return "review"
    return "unmatched"


def ensure_sustainability_template(template_path: Path) -> None:
    if template_path.exists():
        return
    template_path.parent.mkdir(parents=True, exist_ok=True)
    template = pd.DataFrame(
        columns=[
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
    )
    template.to_csv(template_path, index=False)


def _read_entities_for_matching() -> pd.DataFrame:
    cols = ["lei", "legal_name", "legal_name_clean", "country"]

    if SETTINGS.level1_parquet_path.exists():
        df = pd.read_parquet(SETTINGS.level1_parquet_path, columns=cols)
    else:
        df = pd.read_csv(SETTINGS.level1_csv_path, usecols=cols, dtype=str)

    for c in cols:
        df[c] = df[c].fillna("").astype(str)

    df = df[df["lei"].str.strip() != ""].copy().reset_index(drop=True)
    df["country_norm"] = df["country"].str.upper().str.strip()
    df["first_char"] = df["legal_name_clean"].str[:1]
    df["first_token"] = df["legal_name_clean"].str.split().str[0].fillna("")
    return df


def _compute_scores(source_name_clean: str, candidate_name_clean: str) -> Tuple[float, float, float]:
    if fuzz is None:
        raise ImportError("rapidfuzz is required for fuzzy matching. Install project dependencies.")
    s_sort = float(fuzz.token_sort_ratio(source_name_clean, candidate_name_clean))
    s_set = float(fuzz.token_set_ratio(source_name_clean, candidate_name_clean))
    combined = max(s_sort, s_set)
    return s_sort, s_set, combined


def _build_index_map(df: pd.DataFrame, key_cols: Sequence[str]) -> Dict[Tuple[str, ...], List[int]]:
    index_map: Dict[Tuple[str, ...], List[int]] = {}
    for idx, row in enumerate(df[list(key_cols)].itertuples(index=False, name=None)):
        key = tuple(str(v) for v in row)
        index_map.setdefault(key, []).append(idx)
    return index_map


def _top_candidates_from_indices(
    source_clean: str,
    idx_candidates: List[int],
    clean_names: List[str],
    top_k: int,
) -> List[Tuple[float, float, float, int]]:
    if process is None or fuzz is None or not idx_candidates:
        return []

    unique_ids = list(dict.fromkeys(idx_candidates))
    choices = {idx: clean_names[idx] for idx in unique_ids if clean_names[idx]}
    if not choices:
        return []

    shortlist_limit = max(top_k * 2, 6)
    sort_hits = process.extract(source_clean, choices, scorer=fuzz.token_sort_ratio, limit=shortlist_limit)
    set_hits = process.extract(source_clean, choices, scorer=fuzz.token_set_ratio, limit=shortlist_limit)

    shortlisted_ids: Set[int] = set(int(hit[2]) for hit in sort_hits)
    shortlisted_ids.update(int(hit[2]) for hit in set_hits)

    scored: List[Tuple[float, float, float, int]] = []
    for idx in shortlisted_ids:
        s_sort, s_set, combined = _compute_scores(source_clean, clean_names[idx])
        scored.append((combined, s_sort, s_set, idx))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:top_k]


def run_matching(source_input: Optional[Path] = None) -> Dict[str, object]:
    template_path = SETTINGS.sustainability_template_path
    ensure_sustainability_template(template_path)

    crosswalk_out = SETTINGS.match_crosswalk_path
    review_out = SETTINGS.review_candidates_path
    diagnostics_out = SETTINGS.match_diagnostics_path

    source_path = source_input or SETTINGS.sustainability_source_input or SETTINGS.sustainability_output_path

    if not source_path.exists():
        diagnostics = {
            "n_source": 0,
            "n_auto": 0,
            "n_review": 0,
            "n_unmatched": 0,
            "status": "source_input_missing",
            "expected_input": str(source_path),
            "template_created": str(template_path),
        }
        pd.DataFrame(
            columns=[
                "source_id",
                "source_name_raw",
                "source_name_clean",
                "matched_lei",
                "matched_legal_name",
                "matched_legal_name_clean",
                "match_score",
                "score_type",
                "match_method",
                "match_status",
                "blocking_keys",
                "notes",
            ]
        ).to_csv(crosswalk_out, index=False)
        pd.DataFrame(
            columns=[
                "source_id",
                "source_name_raw",
                "candidate_lei",
                "candidate_legal_name",
                "candidate_legal_name_clean",
                "token_sort_ratio",
                "token_set_ratio",
                "combined_score",
                "blocking_keys",
            ]
        ).to_csv(review_out, index=False)
        write_json(diagnostics_out, diagnostics)
        return diagnostics

    src = pd.read_csv(source_path, dtype=str).fillna("")
    required_cols = {"source_id", "source_name", "source_country", "source_sector", "source_notes"}
    missing = sorted(required_cols - set(src.columns))
    if missing:
        raise ValueError(f"Source file missing required columns: {missing}")

    src["source_name_clean"] = src["source_name"].map(clean_legal_name)
    src["source_country_norm"] = src["source_country"].str.upper().str.strip()
    src["first_char"] = src["source_name_clean"].str[:1]
    src["first_token"] = src["source_name_clean"].str.split().str[0].fillna("")

    entities = _read_entities_for_matching()

    leis = entities["lei"].tolist()
    legal_names = entities["legal_name"].tolist()
    clean_names = entities["legal_name_clean"].tolist()

    idx_country_token = _build_index_map(entities, ["country_norm", "first_token"])
    idx_country_char = _build_index_map(entities, ["country_norm", "first_char"])
    idx_token = _build_index_map(entities, ["first_token"])
    idx_char = _build_index_map(entities, ["first_char"])
    idx_country = _build_index_map(entities, ["country_norm"])
    all_indices = list(range(len(entities)))

    max_candidates = SETTINGS.max_block_candidates
    top_k = SETTINGS.top_k_candidates

    crosswalk_rows: List[Dict[str, object]] = []
    review_rows: List[Dict[str, object]] = []

    for row in src.itertuples(index=False):
        source_clean = str(row.source_name_clean).strip()
        source_country = str(row.source_country_norm).strip()
        first_token = str(row.first_token).strip()
        first_char = str(row.first_char).strip()

        blocking_keys = f"country={source_country or 'NA'}|first_char={first_char}|first_token={first_token}"

        if not source_clean:
            crosswalk_rows.append(
                {
                    "source_id": row.source_id,
                    "source_name_raw": row.source_name,
                    "source_name_clean": source_clean,
                    "matched_lei": "",
                    "matched_legal_name": "",
                    "matched_legal_name_clean": "",
                    "match_score": "",
                    "score_type": "",
                    "match_method": "none",
                    "match_status": "unmatched",
                    "blocking_keys": blocking_keys,
                    "notes": "Empty cleaned source name",
                }
            )
            continue

        idx_candidates: List[int] = []

        if SETTINGS.block_on_country and SETTINGS.block_on_first_token and source_country and first_token:
            idx_candidates = idx_country_token.get((source_country, first_token), [])
        if not idx_candidates and SETTINGS.block_on_country and SETTINGS.block_on_first_char and source_country and first_char:
            idx_candidates = idx_country_char.get((source_country, first_char), [])
        if not idx_candidates and SETTINGS.block_on_first_token and first_token:
            idx_candidates = idx_token.get((first_token,), [])
        if not idx_candidates and SETTINGS.block_on_first_char and first_char:
            idx_candidates = idx_char.get((first_char,), [])
        if not idx_candidates and SETTINGS.block_on_country and source_country:
            idx_candidates = idx_country.get((source_country,), [])
        if not idx_candidates:
            idx_candidates = all_indices

        if len(idx_candidates) > max_candidates:
            idx_candidates = idx_candidates[:max_candidates]

        exact_idx = None
        for idx in idx_candidates:
            if clean_names[idx] == source_clean:
                exact_idx = idx
                break

        if exact_idx is not None:
            crosswalk_rows.append(
                {
                    "source_id": row.source_id,
                    "source_name_raw": row.source_name,
                    "source_name_clean": source_clean,
                    "matched_lei": leis[exact_idx],
                    "matched_legal_name": legal_names[exact_idx],
                    "matched_legal_name_clean": clean_names[exact_idx],
                    "match_score": 100.0,
                    "score_type": "exact",
                    "match_method": "exact",
                    "match_status": "auto",
                    "blocking_keys": blocking_keys,
                    "notes": "Exact match on cleaned names",
                }
            )
            continue

        top = _top_candidates_from_indices(
            source_clean=source_clean,
            idx_candidates=idx_candidates,
            clean_names=clean_names,
            top_k=top_k,
        )

        if top:
            best_combined, best_sort, best_set, best_idx = top[0]
            status = classify_match_status(best_combined)
            crosswalk_rows.append(
                {
                    "source_id": row.source_id,
                    "source_name_raw": row.source_name,
                    "source_name_clean": source_clean,
                    "matched_lei": leis[best_idx] if status != "unmatched" else "",
                    "matched_legal_name": legal_names[best_idx] if status != "unmatched" else "",
                    "matched_legal_name_clean": clean_names[best_idx] if status != "unmatched" else "",
                    "match_score": best_combined,
                    "score_type": "combined_max_token_sort_token_set",
                    "match_method": "fuzzy",
                    "match_status": status,
                    "blocking_keys": blocking_keys,
                    "notes": "Top candidate by max(token_sort_ratio, token_set_ratio)",
                }
            )

            if status == "review":
                for combined, s_sort, s_set, idx in top:
                    review_rows.append(
                        {
                            "source_id": row.source_id,
                            "source_name_raw": row.source_name,
                            "candidate_lei": leis[idx],
                            "candidate_legal_name": legal_names[idx],
                            "candidate_legal_name_clean": clean_names[idx],
                            "token_sort_ratio": s_sort,
                            "token_set_ratio": s_set,
                            "combined_score": combined,
                            "blocking_keys": blocking_keys,
                        }
                    )
        else:
            crosswalk_rows.append(
                {
                    "source_id": row.source_id,
                    "source_name_raw": row.source_name,
                    "source_name_clean": source_clean,
                    "matched_lei": "",
                    "matched_legal_name": "",
                    "matched_legal_name_clean": "",
                    "match_score": "",
                    "score_type": "",
                    "match_method": "none",
                    "match_status": "unmatched",
                    "blocking_keys": blocking_keys,
                    "notes": "No candidates after blocking",
                }
            )

    crosswalk = pd.DataFrame(crosswalk_rows)
    review = pd.DataFrame(review_rows)

    crosswalk_out.parent.mkdir(parents=True, exist_ok=True)
    crosswalk.to_csv(crosswalk_out, index=False)
    review.to_csv(review_out, index=False)

    n_source = int(len(crosswalk))
    n_auto = int((crosswalk["match_status"] == "auto").sum()) if n_source else 0
    n_review = int((crosswalk["match_status"] == "review").sum()) if n_source else 0
    n_unmatched = int((crosswalk["match_status"] == "unmatched").sum()) if n_source else 0

    score_series = pd.to_numeric(crosswalk["match_score"], errors="coerce")

    diagnostics = {
        "n_source": n_source,
        "n_auto": n_auto,
        "n_review": n_review,
        "n_unmatched": n_unmatched,
        "overall_auto_rate": float(n_auto / n_source) if n_source else 0.0,
        "score_distribution": {
            "min": float(score_series.min()) if not score_series.dropna().empty else None,
            "p25": float(score_series.quantile(0.25)) if not score_series.dropna().empty else None,
            "median": float(score_series.median()) if not score_series.dropna().empty else None,
            "p75": float(score_series.quantile(0.75)) if not score_series.dropna().empty else None,
            "max": float(score_series.max()) if not score_series.dropna().empty else None,
        },
        "blocking": {
            "use_country": SETTINGS.block_on_country,
            "use_first_token": SETTINGS.block_on_first_token,
            "use_first_char": SETTINGS.block_on_first_char,
            "max_block_candidates": SETTINGS.max_block_candidates,
            "top_k_candidates": SETTINGS.top_k_candidates,
        },
        "thresholds": {
            "auto": SETTINGS.auto_match_threshold,
            "review": SETTINGS.review_match_threshold,
            "unmatched": SETTINGS.unmatched_threshold,
        },
        "outputs": {
            "crosswalk_csv": str(crosswalk_out),
            "review_csv": str(review_out),
        },
    }

    write_json(diagnostics_out, diagnostics)
    return diagnostics
