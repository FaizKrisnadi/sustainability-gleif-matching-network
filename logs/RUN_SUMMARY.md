# Run Summary

## [2026-02-27T08:44:21Z] inspect-inputs
- inputs_present: 7
- inputs_total: 8
- inspect_file: <ARCHIVE_REPO_ROOT>/processed/inspect_inputs.json

## [2026-02-27T15:21:26Z] inspect-inputs
- inputs_present: 7
- inputs_total: 8
- inspect_file: <REPO_ROOT>/data/processed/inspect_inputs.json

## [2026-02-27T15:54:33Z] inspect-inputs
- inputs_present: 7
- inputs_total: 8
- inspect_file: <REPO_ROOT>/data/processed/inspect_inputs.json

## [2026-02-27T16:15:46Z] preprocess-gleif-level1
- input_path: <DOWNLOADS>/20260219-gleif-concatenated-file-lei2.xml.6996d6943c89b.zip
- manifest: <REPO_ROOT>/data/processed/run_manifest.json
- manifest_timestamp: 2026-02-27T16:15:46Z
- output_csv: <REPO_ROOT>/data/processed/gleif_entities_clean.csv
- rows_read: 3219530
- rows_written: 3219530
- skipped_rows: 0

## [2026-02-27T16:50:54Z] preprocess-gleif-level2
- input_path: <DOWNLOADS>/20260219-gleif-concatenated-file-rr.xml.6996d3f429e09.zip
- manifest: <REPO_ROOT>/data/processed/run_manifest.json
- manifest_timestamp: 2026-02-27T16:50:54Z
- output_csv: <REPO_ROOT>/data/processed/gleif_relationships_clean.csv
- repex_status: skipped
- rows_read: 634561
- rows_written: 634561
- skipped_rows: 0

## [2026-02-27T16:53:06Z] build-network
- duplicate_edges: 453
- edges_rows: 634561
- nodes_rows: 3219530
- output_edges: <REPO_ROOT>/data/processed/edges.csv
- output_nodes: <REPO_ROOT>/data/processed/nodes.csv

## [2026-02-27T16:54:10Z] build-sustainability-source
- output_csv: <REPO_ROOT>/data/processed/sustainability_source.csv
- source_rows_final: 14180
- source_rows_sbti_raw: 14034

## [2026-02-27T17:11:13Z] match-sustainability
- n_auto: 7641
- n_review: 396
- n_source: 14180
- n_unmatched: 6143
- source_path: <REPO_ROOT>/data/processed/sustainability_source.csv

## Final Validation Summary (2026-02-27)
- Level 1 rows read/written: `3219530 / 3219530`
- Level 2 rows read/written: `634561 / 634561`
- Nodes/edges counts: `3219530 / 634561`
- Matching: `n_source=14180`, `n_auto=7641`, `n_review=396`, `n_unmatched=6143`
- Outputs location: `<REPO_ROOT>/data/processed`
- Scale evidence manifest: `<REPO_ROOT>/data/processed/run_manifest.json`

## [2026-02-27T17:15:40Z] inspect-inputs
- inputs_present: 8
- inputs_total: 8
- inspect_file: <REPO_ROOT>/data/processed/inspect_inputs.json

## [2026-02-27T17:16:20Z] inspect-inputs
- inputs_present: 8
- inputs_total: 8
- inspect_file: <REPO_ROOT>/data/processed/inspect_inputs.json
