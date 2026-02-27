from __future__ import annotations

import re
import unicodedata
from typing import List, Sequence, Tuple


LEGAL_SUFFIX_PATTERNS: List[Tuple[str, ...]] = [
    ("inc",),
    ("incorporated",),
    ("corp",),
    ("corporation",),
    ("co",),
    ("company",),
    ("ltd",),
    ("limited",),
    ("llc",),
    ("plc",),
    ("lp",),
    ("llp",),
    ("gmbh",),
    ("ag",),
    ("nv",),
    ("bv",),
    ("sa",),
    ("spa",),
    ("pte",),
    ("pt",),
    ("tbk",),
    ("s", "a"),
    ("s", "p", "a"),
]


WHITESPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")


def normalize_unicode(text: str) -> str:
    # NFKD allows accent stripping while preserving base characters.
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_name(text: str) -> str:
    if text is None:
        return ""
    value = normalize_unicode(str(text)).lower().replace("&", " and ")
    value = NON_ALNUM_RE.sub(" ", value)
    value = WHITESPACE_RE.sub(" ", value).strip()
    return value


def strip_legal_suffixes(clean_text: str) -> str:
    if not clean_text:
        return ""

    tokens = clean_text.split()
    changed = True

    # Some jurisdictions place legal form markers as a leading token (for example, "PT"). 
    # Remove only a small, explicit set to avoid over-stripping.
    while tokens and tokens[0] in {"pt"}:
        tokens = tokens[1:]

    # Repeatedly remove recognized legal suffix token patterns at the end.
    while changed and tokens:
        changed = False
        for suffix in sorted(LEGAL_SUFFIX_PATTERNS, key=len, reverse=True):
            s_len = len(suffix)
            if s_len <= len(tokens) and tuple(tokens[-s_len:]) == suffix:
                tokens = tokens[:-s_len]
                changed = True
                break

    return " ".join(tokens).strip()


def clean_legal_name(text: str) -> str:
    normalized = normalize_name(text)
    stripped = strip_legal_suffixes(normalized)
    # If suffix-stripping empties the value, keep normalized form to avoid blank key.
    return stripped if stripped else normalized
