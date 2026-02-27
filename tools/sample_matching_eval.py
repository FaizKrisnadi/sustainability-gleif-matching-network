from __future__ import annotations

from pathlib import Path

import pandas as pd

SEED = 42
TARGET_N = 100

INPUT_PATH = Path("data/processed/match_crosswalk.csv")
SAMPLES_DIR = Path("data/samples")

GROUP_TO_FILE = {
    "auto": "matching_eval_sample_auto.csv",
    "review": "matching_eval_sample_review.csv",
    "unmatched": "matching_eval_sample_unmatched.csv",
}


def _build_group_sample(df: pd.DataFrame, status: str) -> pd.DataFrame:
    subset = df[df["match_status"].astype(str).str.lower() == status].copy()
    if len(subset) <= TARGET_N:
        return subset
    return subset.sample(n=TARGET_N, random_state=SEED)


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing required input: {INPUT_PATH}")

    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)
    required = {
        "source_id",
        "source_name_raw",
        "source_name_clean",
        "matched_lei",
        "matched_legal_name",
        "match_score",
        "score_type",
        "match_status",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"match_crosswalk.csv is missing required columns: {missing}")

    sample_columns = [
        "source_id",
        "source_name_raw",
        "source_name_clean",
        "matched_lei",
        "matched_legal_name",
        "match_score",
        "score_type",
        "match_status",
    ]
    if "blocking_keys" in df.columns:
        sample_columns.append("blocking_keys")

    sampled_by_group: dict[str, pd.DataFrame] = {}
    for group, filename in GROUP_TO_FILE.items():
        sample_df = _build_group_sample(df, group)
        sampled_by_group[group] = sample_df
        out_df = sample_df.reindex(columns=sample_columns)
        out_df.to_csv(SAMPLES_DIR / filename, index=False)

    label_frames: list[pd.DataFrame] = []
    for group, sample_df in sampled_by_group.items():
        frame = sample_df.reindex(
            columns=[
                "source_id",
                "source_name_raw",
                "matched_lei",
                "matched_legal_name",
                "match_score",
            ]
        ).copy()
        frame.insert(0, "sample_group", group)
        frame["label"] = ""
        frame["notes"] = ""
        label_frames.append(frame)

    labels_template = pd.concat(label_frames, ignore_index=True)
    labels_template.to_csv(SAMPLES_DIR / "matching_eval_labels_template.csv", index=False)

    print("matching evaluation samples created")
    print(f"seed={SEED} target_n={TARGET_N}")
    for group in GROUP_TO_FILE:
        original_n = int((df["match_status"].astype(str).str.lower() == group).sum())
        sampled_n = len(sampled_by_group[group])
        print(f"{group}: sampled={sampled_n} available={original_n}")
    print(f"labels_template={SAMPLES_DIR / 'matching_eval_labels_template.csv'}")


if __name__ == "__main__":
    main()
