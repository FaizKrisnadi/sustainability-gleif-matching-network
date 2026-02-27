# sustainability-gleif-matching-network

Portfolio pipeline for GLEIF entity parsing, sustainability-name matching, and ownership-network construction with auditable logs and sample artifacts.

## Latest run evidence (from logs)
- Level 1 rows read/written: 3,219,530 / 3,219,530
- Level 2 rows read/written: 634,561 / 634,561
- Matching: n_source=14,180, n_auto=7,641, n_review=396, n_unmatched=6,143

## Outputs
- `data/processed/gleif_entities_clean.parquet` (not committed)
- `data/processed/edges.csv` (not committed)
- `data/processed/nodes.csv` (not committed)
- `data/processed/nodes_in_network.csv` (not committed)
- `data/processed/match_crosswalk.csv` (not committed)
- `data/samples/match_crosswalk_sample.csv` (committed)
- `data/samples/edges_sample.csv` (committed)
- `data/samples/nodes_sample.csv` (committed)
- `data/samples/nodes_in_network_sample.csv` (committed)
- `data/samples/run_manifest_sample.json` (committed)

## How to run
1. Copy `config.example.yaml` to `config.yaml` and set local input paths.
2. Install dependencies:
   ```bash
   make setup
   ```
3. Run steps via Makefile targets:
   ```bash
   make inspect
   make preprocess-level1
   make preprocess-level2
   make build-source
   make match
   make network
   make run-all
   ```
4. Equivalent CLI commands:
   ```bash
   python -m src.cli inspect-inputs
   python -m src.cli preprocess-gleif-level1
   python -m src.cli preprocess-gleif-level2 --parse-repex
   python -m src.cli build-sustainability-source
   python -m src.cli match-sustainability
   python -m src.cli build-network
   python -m src.cli run-all
   ```

## Limitations
- Raw GLEIF + SBTi files are local and not committed.
- Initiative lists may be partial (e.g., RE100 first100).
- Fuzzy matching produces review/unmatched cases and should not be treated as perfect ground truth.

## Quality & validation
- Matching evaluation workflow: `tools/sample_matching_eval.py` and `tools/generate_matching_eval_report.py`
- Matching quality report: `docs/matching_eval.md`
- Network sanity report: `docs/network_sanity.md`
- Matching precision is only computed after manual labels are added in `data/samples/matching_eval_labels.csv`; otherwise reports remain `PENDING MANUAL LABELS`.
