# Data Provenance

## Inputs
- GLEIF Level 1 LEI-CDF file: local path in `config.yaml` (`inputs.gleif_level1`).
- GLEIF Level 2 RR-CDF file: local path in `config.yaml` (`inputs.gleif_level2`).
- Optional GLEIF REPEX file: local path in `config.yaml` (`inputs.gleif_repex`).
- SBTi by-company Excel: local path in `config.yaml` (`inputs.sbti_excel`).
- Manual RE100/EV100/EP100 CSVs: local paths in `config.yaml`.

## Git policy
- Full raw GLEIF files and SBTi Excel are **not** committed to git.
- Repo keeps only templates and small sample outputs.

## Traceability artifacts
- `data/processed/run_manifest.json`: timestamp, input file sizes/types, Level 1/2 row counts, output locations.
- `logs/COMMAND_LOG.md`: exact CLI command history.
- `logs/RUN_SUMMARY.md`: per-step row counts and metrics.
- `logs/ERROR_LOG.md`: errors and stack traces.
