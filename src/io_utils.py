from __future__ import annotations

import gzip
import json
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
import xml.etree.ElementTree as ET


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def local_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def find_latest_by_patterns(base_dir: Path, patterns: Iterable[str]) -> Optional[Path]:
    candidates: List[Path] = []
    for pattern in patterns:
        candidates.extend(base_dir.glob(pattern))
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def detect_file_type(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "size_bytes": 0,
            "detected_type": "missing",
        }

    with path.open("rb") as fh:
        magic = fh.read(16)

    if magic.startswith(b"PK\x03\x04"):
        ftype = "zip"
    elif magic.startswith(b"\x1f\x8b"):
        ftype = "gzip"
    elif magic.startswith(b"<?xml") or b"<" in magic:
        ftype = "xml"
    else:
        ftype = "unknown"

    return {
        "path": str(path),
        "exists": True,
        "size_bytes": int(path.stat().st_size),
        "detected_type": ftype,
    }


def _choose_zip_member(zf: zipfile.ZipFile) -> str:
    members = [i for i in zf.infolist() if not i.is_dir()]
    if not members:
        raise ValueError("Zip file has no file entries")

    xml_members = [m for m in members if m.filename.lower().endswith(".xml")]
    ranked = xml_members if xml_members else members
    ranked = sorted(ranked, key=lambda m: m.file_size, reverse=True)
    return ranked[0].filename


@contextmanager
def open_xml_binary(path: Path):
    info = detect_file_type(path)
    detected = info["detected_type"]

    if detected == "zip":
        zf = zipfile.ZipFile(path)
        inner = _choose_zip_member(zf)
        fh = zf.open(inner)
        try:
            yield fh, inner
        finally:
            fh.close()
            zf.close()
        return

    if detected == "gzip":
        fh = gzip.open(path, "rb")
        try:
            yield fh, path.name
        finally:
            fh.close()
        return

    fh = path.open("rb")
    try:
        yield fh, path.name
    finally:
        fh.close()


def extract_header_fields(path: Path, header_tag_local: str) -> Dict[str, Optional[str]]:
    with open_xml_binary(path) as (fh, _):
        for event, elem in ET.iterparse(fh, events=("end",)):
            if local_name(elem.tag) == header_tag_local:
                out: Dict[str, Optional[str]] = {}
                for child in list(elem):
                    if len(child) == 0:
                        text = (child.text or "").strip()
                        out[local_name(child.tag)] = text or None
                return out
    return {}


def iter_xml_records(path: Path, record_tag_local: str) -> Iterator[ET.Element]:
    with open_xml_binary(path) as (fh, _):
        context = ET.iterparse(fh, events=("start", "end"))
        root = None
        for event, elem in context:
            if root is None and event == "start":
                root = elem
            if event == "end" and local_name(elem.tag) == record_tag_local:
                yield elem
                elem.clear()
                if root is not None:
                    root.clear()


def get_child(elem: Optional[ET.Element], child_local_name: str) -> Optional[ET.Element]:
    if elem is None:
        return None
    for child in elem:
        if local_name(child.tag) == child_local_name:
            return child
    return None


def get_text(elem: Optional[ET.Element], path: Iterable[str]) -> Optional[str]:
    cur = elem
    for key in path:
        cur = get_child(cur, key)
        if cur is None:
            return None
    text = (cur.text or "").strip() if cur is not None else ""
    return text or None


def parse_year(date_like: Optional[str]) -> Optional[int]:
    if date_like is None:
        return None
    text = str(date_like).strip()
    if text in {"", "None", "nan", "<NA>"}:
        return None
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)


def append_markdown_lines(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(f"{line}\n")


def format_kv_lines(payload: Dict[str, object]) -> List[str]:
    lines: List[str] = []
    for key in sorted(payload.keys()):
        value = payload[key]
        if isinstance(value, float):
            lines.append(f"- {key}: {value:.6f}" if value < 1 else f"- {key}: {value}")
        else:
            lines.append(f"- {key}: {value}")
    return lines
