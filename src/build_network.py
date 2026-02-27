from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

from .config import SETTINGS
from .io_utils import parse_year


def _read_entities() -> pd.DataFrame:
    if SETTINGS.level1_parquet_path.exists():
        return pd.read_parquet(SETTINGS.level1_parquet_path)
    return pd.read_csv(SETTINGS.level1_csv_path, dtype=str)


def _read_relationships() -> pd.DataFrame:
    if SETTINGS.level2_parquet_path.exists():
        return pd.read_parquet(SETTINGS.level2_parquet_path)
    return pd.read_csv(SETTINGS.level2_csv_path, dtype=str)


def build_network_outputs() -> Dict[str, object]:
    entities = _read_entities()
    relationships = _read_relationships()

    for col in ["lei", "legal_name", "legal_name_clean", "country"]:
        if col in entities.columns:
            entities[col] = entities[col].fillna("").astype(str)

    for col in ["parent_lei", "child_lei", "relationship_type", "start_date"]:
        if col in relationships.columns:
            relationships[col] = relationships[col].fillna("").astype(str)

    edges = pd.DataFrame(
        {
            "firm_i": relationships.get("child_lei", ""),
            "firm_j": relationships.get("parent_lei", ""),
            "relation_type": relationships.get("relationship_type", ""),
            "relation_direction": "child_to_parent",
            "year": relationships.get("start_date", "").map(parse_year),
            "weight": 1,
        }
    )

    in_rel = set(edges["firm_i"].dropna().tolist()) | set(edges["firm_j"].dropna().tolist())

    nodes = entities[["lei", "legal_name", "legal_name_clean", "country"]].copy()
    nodes["in_relationships"] = nodes["lei"].isin(in_rel)

    SETTINGS.edges_csv_path.parent.mkdir(parents=True, exist_ok=True)
    edges.to_csv(SETTINGS.edges_csv_path, index=False)
    nodes.to_csv(SETTINGS.nodes_csv_path, index=False)

    return {
        "entities_rows": int(len(entities)),
        "relationships_rows": int(len(relationships)),
        "nodes_rows": int(len(nodes)),
        "edges_rows": int(len(edges)),
        "outputs": {
            "nodes_csv": str(SETTINGS.nodes_csv_path),
            "edges_csv": str(SETTINGS.edges_csv_path),
        },
    }
