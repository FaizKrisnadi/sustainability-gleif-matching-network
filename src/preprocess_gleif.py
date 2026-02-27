from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .gleif_level1 import preprocess_level1, resolve_level1_input
from .gleif_level2 import parse_repex, preprocess_level2, resolve_level2_input, resolve_repex_input
from .io_utils import write_json
from .config import SETTINGS


def resolve_input_paths(
    lei_path: Optional[Path] = None,
    rr_path: Optional[Path] = None,
    repex_path: Optional[Path] = None,
) -> Dict[str, Optional[Path]]:
    return {
        "lei": resolve_level1_input(lei_path),
        "rr": resolve_level2_input(rr_path),
        "repex": resolve_repex_input(repex_path),
    }


def parse_reporting_exceptions(repex_path: Path) -> Dict[str, object]:
    return parse_repex(repex_path)


def preprocess_gleif(
    lei_path: Optional[Path] = None,
    rr_path: Optional[Path] = None,
    repex_path: Optional[Path] = None,
    parse_repex_flag: bool = True,
) -> Dict[str, object]:
    stats: Dict[str, object] = {
        "inputs": {
            "lei": str(resolve_level1_input(lei_path)) if resolve_level1_input(lei_path) else None,
            "rr": str(resolve_level2_input(rr_path)) if resolve_level2_input(rr_path) else None,
            "repex": str(resolve_repex_input(repex_path)) if resolve_repex_input(repex_path) else None,
        },
        "level1": preprocess_level1(lei_path),
        "level2": preprocess_level2(rr_path),
    }
    if parse_repex_flag:
        stats["repex"] = parse_repex(repex_path)
    else:
        stats["repex"] = {"status": "skipped", "reason": "parse_repex_flag=False"}

    write_json(SETTINGS.preprocess_stats_path, stats)
    return stats
