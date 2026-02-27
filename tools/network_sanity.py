from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

EDGES_PATH = Path("data/processed/edges.csv")
NODES_PATH = Path("data/processed/nodes.csv")
NODES_IN_NETWORK_PATH = Path("data/processed/nodes_in_network.csv")
NODES_IN_NETWORK_SAMPLE_PATH = Path("data/samples/nodes_in_network_sample.csv")
STATS_PATH = Path("data/samples/network_stats.json")
REPORT_PATH = Path("docs/network_sanity.md")


def _top_rows(df: pd.DataFrame, value_col: str, key_col: str, n: int = 20) -> list[dict[str, object]]:
    if df.empty:
        return []
    top = df.sort_values(value_col, ascending=False).head(n)
    return [
        {key_col: row[key_col], value_col: int(row[value_col])}
        for _, row in top.iterrows()
    ]


def main() -> None:
    if not EDGES_PATH.exists():
        raise FileNotFoundError(f"Missing required input: {EDGES_PATH}")
    if not NODES_PATH.exists():
        raise FileNotFoundError(f"Missing required input: {NODES_PATH}")

    edges = pd.read_csv(EDGES_PATH, dtype=str)
    nodes = pd.read_csv(NODES_PATH, dtype=str)

    required_edge_cols = {"firm_i", "firm_j"}
    missing_edge_cols = sorted(required_edge_cols - set(edges.columns))
    if missing_edge_cols:
        raise ValueError(f"edges.csv missing required columns: {missing_edge_cols}")
    if "lei" not in nodes.columns:
        raise ValueError("nodes.csv missing required column: lei")

    edges = edges.fillna("")
    nodes = nodes.fillna("")

    edges_dedup = edges.drop_duplicates()
    pair_df = edges_dedup[["firm_i", "firm_j"]].copy()
    pair_df = pair_df[(pair_df["firm_i"] != "") & (pair_df["firm_j"] != "")]

    edge_leis = set(pair_df["firm_i"]).union(set(pair_df["firm_j"]))
    nodes_lei_set = set(nodes["lei"])
    missing_node_info = sorted(edge_leis - nodes_lei_set)

    created_nodes_in_network = False
    if not NODES_IN_NETWORK_PATH.exists():
        nodes_in_network = nodes[nodes["lei"].isin(edge_leis)].copy()
        NODES_IN_NETWORK_PATH.parent.mkdir(parents=True, exist_ok=True)
        nodes_in_network.to_csv(NODES_IN_NETWORK_PATH, index=False)
        created_nodes_in_network = True
    else:
        nodes_in_network = pd.read_csv(NODES_IN_NETWORK_PATH, dtype=str).fillna("")

    NODES_IN_NETWORK_SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    nodes_in_network.head(200).to_csv(NODES_IN_NETWORK_SAMPLE_PATH, index=False)

    # Directed interpretation for sanity checks is explicitly fixed by requirement:
    # treat firm_i as parent and firm_j as child.
    directed_pairs = pair_df.drop_duplicates(subset=["firm_i", "firm_j"])
    out_degree_df = (
        directed_pairs.groupby("firm_i", as_index=False)["firm_j"]
        .nunique()
        .rename(columns={"firm_i": "parent_lei", "firm_j": "out_degree"})
    )
    in_degree_df = (
        directed_pairs.groupby("firm_j", as_index=False)["firm_i"]
        .nunique()
        .rename(columns={"firm_j": "child_lei", "firm_i": "in_degree"})
    )

    nonself = directed_pairs[directed_pairs["firm_i"] != directed_pairs["firm_j"]].copy()
    if len(nonself) > 0:
        undirected_u = nonself[["firm_i", "firm_j"]].copy()
        undirected_u["u"] = undirected_u[["firm_i", "firm_j"]].min(axis=1)
        undirected_u["v"] = undirected_u[["firm_i", "firm_j"]].max(axis=1)
        undirected_u = undirected_u[["u", "v"]].drop_duplicates()

        neighbors = pd.concat(
            [
                undirected_u.rename(columns={"u": "node", "v": "neighbor"}),
                undirected_u.rename(columns={"v": "node", "u": "neighbor"}),
            ],
            ignore_index=True,
        )
        degree_series = neighbors.groupby("node")["neighbor"].nunique()
    else:
        degree_series = pd.Series(dtype="int64")

    degree_full = pd.Series(0, index=sorted(edge_leis), dtype="int64")
    if len(degree_series) > 0:
        degree_full.loc[degree_series.index] = degree_series.astype("int64")

    degree_summary = {
        "min": int(degree_full.min()) if len(degree_full) > 0 else 0,
        "median": float(degree_full.median()) if len(degree_full) > 0 else 0.0,
        "max": int(degree_full.max()) if len(degree_full) > 0 else 0,
    }

    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stats = {
        "generated_at_utc": timestamp,
        "n_edges": int(len(edges_dedup)),
        "n_nodes_total": int(len(nodes)),
        "n_nodes_in_network": int(len(edge_leis)),
        "n_self_loops": int((pair_df["firm_i"] == pair_df["firm_j"]).sum()),
        "missing_node_info_count": int(len(missing_node_info)),
        "missing_node_info_example_leis": missing_node_info[:20],
        "top_20_out_degree": _top_rows(out_degree_df, "out_degree", "parent_lei", n=20),
        "top_20_in_degree": _top_rows(in_degree_df, "in_degree", "child_lei", n=20),
        "undirected_degree_summary": degree_summary,
        "nodes_in_network_file_created": created_nodes_in_network,
    }

    STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATS_PATH.write_text(json.dumps(stats, indent=2), encoding="utf-8")

    lines: list[str] = []
    lines.append("# Network Sanity Checks")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- Generated at (UTC): `{timestamp}`")
    lines.append("- Inputs: `data/processed/edges.csv`, `data/processed/nodes.csv`")
    lines.append("- Directed assumption for sanity stats: treat `firm_i` as parent and `firm_j` as child.")
    lines.append("")
    lines.append("## Summary Metrics")
    lines.append(f"- n_edges (exact duplicates dropped): `{stats['n_edges']}`")
    lines.append(f"- n_nodes_total (`nodes.csv`): `{stats['n_nodes_total']}`")
    lines.append(f"- n_nodes_in_network (unique LEIs in edges): `{stats['n_nodes_in_network']}`")
    lines.append(f"- n_self_loops (`firm_i == firm_j`): `{stats['n_self_loops']}`")
    lines.append(f"- missing_node_info_count (edge LEIs absent from `nodes.csv`): `{stats['missing_node_info_count']}`")
    lines.append(
        f"- nodes_in_network_file_created (this run): `{str(stats['nodes_in_network_file_created']).lower()}`"
    )
    lines.append("")
    lines.append("## Undirected Degree Summary")
    lines.append(f"- min: `{degree_summary['min']}`")
    lines.append(f"- median: `{degree_summary['median']}`")
    lines.append(f"- max: `{degree_summary['max']}`")
    lines.append("")
    lines.append("## Top 20 Out-Degree (firm_i as parent)")
    lines.append("| parent_lei | out_degree |")
    lines.append("|---|---:|")
    for row in stats["top_20_out_degree"]:
        lines.append(f"| {row['parent_lei']} | {row['out_degree']} |")
    lines.append("")
    lines.append("## Top 20 In-Degree (firm_j as child)")
    lines.append("| child_lei | in_degree |")
    lines.append("|---|---:|")
    for row in stats["top_20_in_degree"]:
        lines.append(f"| {row['child_lei']} | {row['in_degree']} |")
    lines.append("")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"report={REPORT_PATH}")
    print(f"stats={STATS_PATH}")
    print(f"nodes_in_network_sample={NODES_IN_NETWORK_SAMPLE_PATH}")


if __name__ == "__main__":
    main()
