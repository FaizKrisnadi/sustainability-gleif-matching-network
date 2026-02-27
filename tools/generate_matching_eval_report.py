from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SEED = 42
TARGET_N = 100

LABELS_PATH = Path("data/samples/matching_eval_labels.csv")
TEMPLATE_PATH = Path("data/samples/matching_eval_labels_template.csv")
REPORT_PATH = Path("docs/matching_eval.md")
METRICS_PATH = Path("data/samples/matching_eval_metrics.json")

GROUPS = ["auto", "review", "unmatched"]
ALLOWED_LABELS = {"correct", "incorrect", "uncertain"}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def main() -> None:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if LABELS_PATH.exists():
        labels_source = LABELS_PATH
    else:
        labels_source = TEMPLATE_PATH

    if not labels_source.exists():
        raise FileNotFoundError(
            "Missing labels input. Run tools/sample_matching_eval.py first to create matching_eval_labels_template.csv."
        )

    labels = pd.read_csv(labels_source, dtype=str).fillna("")
    required_cols = [
        "sample_group",
        "source_id",
        "source_name_raw",
        "matched_lei",
        "matched_legal_name",
        "match_score",
        "label",
        "notes",
    ]
    missing = [col for col in required_cols if col not in labels.columns]
    if missing:
        raise ValueError(f"Labels file missing required columns: {missing}")

    labels["sample_group"] = labels["sample_group"].map(_clean_text).str.lower()
    labels["label"] = labels["label"].map(_clean_text).str.lower()
    labels["notes"] = labels["notes"].map(_clean_text)

    non_blank = labels["label"] != ""
    valid_labeled = labels[labels["label"].isin(ALLOWED_LABELS)]
    has_manual_labels = bool(non_blank.any() and len(valid_labeled) > 0 and LABELS_PATH.exists())

    pending_manual_labels = not has_manual_labels

    counts_by_group: dict[str, dict[str, int]] = {}
    for group in GROUPS:
        group_df = labels[labels["sample_group"] == group]
        label_counts = group_df["label"].value_counts().to_dict()
        counts_by_group[group] = {
            "rows_total": int(len(group_df)),
            "correct": int(label_counts.get("correct", 0)),
            "incorrect": int(label_counts.get("incorrect", 0)),
            "uncertain": int(label_counts.get("uncertain", 0)),
            "blank": int(label_counts.get("", 0)),
        }

    auto_df = labels[labels["sample_group"] == "auto"]
    auto_scored = auto_df[auto_df["label"].isin(["correct", "incorrect"])]
    auto_correct = int((auto_scored["label"] == "correct").sum())
    auto_incorrect = int((auto_scored["label"] == "incorrect").sum())
    auto_denom = auto_correct + auto_incorrect

    auto_precision: float | None
    if pending_manual_labels or auto_denom == 0:
        auto_precision = None
    else:
        auto_precision = auto_correct / auto_denom

    note_rows = labels[(labels["notes"] != "") & labels["label"].isin(["incorrect", "uncertain"])]
    note_counter = Counter(note_rows["notes"].tolist())
    top_failure_notes = [
        {"note": note, "count": int(count)}
        for note, count in note_counter.most_common(10)
    ]

    metrics = {
        "generated_at_utc": timestamp,
        "seed": SEED,
        "target_sample_size_per_group": TARGET_N,
        "labels_source": str(labels_source).replace("\\", "/"),
        "pending_manual_labels": pending_manual_labels,
        "counts_by_group": counts_by_group,
        "auto_precision": auto_precision,
        "auto_precision_numerator": auto_correct,
        "auto_precision_denominator": auto_denom,
        "failure_notes_top": top_failure_notes,
    }

    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    status_line = "PENDING MANUAL LABELS" if pending_manual_labels else "READY"
    precision_text = (
        "PENDING (no valid manual labels for auto group)"
        if auto_precision is None
        else f"{auto_precision:.4f} ({auto_correct}/{auto_denom})"
    )

    lines: list[str] = []
    lines.append("# Matching Evaluation")
    lines.append("")
    lines.append(f"**Status:** {status_line}")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- Generated at (UTC): `{timestamp}`")
    lines.append(f"- Labels source: `{labels_source.as_posix()}`")
    lines.append(f"- Sampling design: seed=`{SEED}`, target size per group=`{TARGET_N}`")
    lines.append("")
    lines.append("## Label Counts By Group")
    lines.append("| Group | Total | Correct | Incorrect | Uncertain | Blank |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for group in GROUPS:
        row = counts_by_group[group]
        lines.append(
            f"| {group} | {row['rows_total']} | {row['correct']} | {row['incorrect']} | {row['uncertain']} | {row['blank']} |"
        )
    lines.append("")
    lines.append("## Precision Estimate (Auto Group)")
    lines.append(
        "- Definition: `correct / (correct + incorrect)` using only manually labeled auto samples; `uncertain` is ignored."
    )
    lines.append(f"- Estimate: {precision_text}")
    lines.append("")
    lines.append("## Common Failure Patterns")
    if top_failure_notes:
        lines.append("Patterns summarized from `notes` on `incorrect` and `uncertain` labels:")
        for item in top_failure_notes:
            lines.append(f"- {item['note']}: {item['count']}")
    else:
        lines.append("PENDING: no failure-pattern notes captured yet.")
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"report={REPORT_PATH}")
    print(f"metrics={METRICS_PATH}")
    print(f"status={status_line}")


if __name__ == "__main__":
    main()
