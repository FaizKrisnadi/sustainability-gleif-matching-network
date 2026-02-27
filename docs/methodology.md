# Methodology

## 1) Input inspection
- Detect GLEIF file container type by signature (ZIP/GZIP/XML), not extension.
- Verify local sustainability source inputs and infer columns from observed headers.

## 2) Level 1 parsing (LEI-CDF)
- Stream-parse `LEIRecord` rows.
- Build entity master fields including cleaned legal names.
- Track `rows_seen`, `rows_written`, and `skipped_rows`.

## 3) Level 2 parsing (RR-CDF)
- Stream-parse `RelationshipRecord` rows.
- Extract `child_lei` (StartNode) and `parent_lei` (EndNode).
- Track `rows_seen`, `rows_written`, and `skipped_rows`.

## 4) Sustainability source assembly
- Use SBTi by-company data as base.
- Merge manual RE100/EV100/EP100 lists.
- Preserve initiative flags and generate a unified `sustainability_source.csv`.

## 5) Matching
- Clean names consistently between source and GLEIF entity master.
- Block candidates by configured keys (country/first token/first char).
- Score with RapidFuzz and classify into `auto` / `review` / `unmatched`.

## 6) Network build
- Convert Level 2 relationships into `edges.csv`.
- Build `nodes.csv` from Level 1 entities with relationship membership flags.

## 7) Audit artifacts
- `logs/COMMAND_LOG.md`: command-level trace.
- `logs/RUN_SUMMARY.md`: metrics by step.
- `logs/ERROR_LOG.md`: failures with traceback.
- `data/processed/run_manifest.json`: input sizes/types + row counts + outputs.
