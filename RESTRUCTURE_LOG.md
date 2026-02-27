# Restructure Log

## Scope
Portfolio-focused repository for:
- GLEIF Level 1 parsing to entity master.
- GLEIF Level 2 parsing to ownership relationships.
- Sustainability source assembly from local SBTi + RE100/EV100/EP100.
- Fuzzy matching crosswalk + diagnostics.
- Auditable run artifacts and logging.

## Key decisions
- Keep old workspace intact and create/maintain this clean sibling repo.
- Use config-driven local input paths (`config.yaml`) and avoid hardcoded paths in source code.
- Detect GLEIF file type by signature (zip/gzip/xml), not extension.
- Exclude raw source files from git; keep templates/samples only.

## Migrated/implemented components
- Added/refined modules under `src/`:
  - `gleif_level1.py`, `gleif_level2.py`, `build_sustainability_source.py`, `match_sustainability.py`, `build_network.py`, `diagnostics.py`, `cli.py`, `config.py`, `io_utils.py`, `clean_names.py`
- Added docs:
  - `docs/methodology.md`, `docs/data_dictionary.md`, `docs/data_provenance.md`, `docs/matching_rules.md`
- Added config template:
  - `config.example.yaml`
- Added git-safe samples:
  - `data/samples/match_crosswalk_sample.csv`
  - `data/samples/edges_sample.csv`
  - `data/samples/nodes_sample.csv`
  - `data/samples/run_manifest_sample.json`

## Run evidence
- `data/processed/run_manifest.json`
- `logs/RUN_SUMMARY.md`
- `logs/COMMAND_LOG.md`
- `logs/ERROR_LOG.md`

## Metrics snapshot (2026-02-27)
- Level 1 rows read/written: `3219530 / 3219530`
- Level 2 rows read/written: `634561 / 634561`
- Nodes/edges: `3219530 / 634561`
- Matching: `n_source=14180`, `n_auto=7641`, `n_review=396`, `n_unmatched=6143`

## Old workspace move record
Detailed discovery and ARCHIVE moves are recorded in:
- `<ARCHIVE_WORKSPACE>/RESTRUCTURE_LOG.md`
