# Sustainability GLEIF Matching Network

Portfolio-grade entity resolution and ownership-network pipeline for tracing where sustainability pledges end up in the GLEIF graph, with a deployed explainer page and auditable matching artifacts.

[![Live Explainer](https://img.shields.io/badge/Live%20Explainer-pledges.faizkrisnadi.com-0A7B83?style=for-the-badge)](https://pledges.faizkrisnadi.com/)

Open the deployed page at [pledges.faizkrisnadi.com](https://pledges.faizkrisnadi.com/).

<p>
  <a href="https://pledges.faizkrisnadi.com/">
    <img src="docs/assets/pledges-preview.png" alt="Preview of the Sustainability GLEIF Matching Network explainer page" width="900">
  </a>
</p>

## What This Repo Does
- Parses and cleans GLEIF Level 1 and Level 2 entity data.
- Builds a sustainability source table from initiative lists and local inputs.
- Matches sustainability entities to GLEIF records with review and unmatched buckets.
- Constructs ownership-network outputs and sample artifacts for inspection and validation.
- Ships a public HTML explainer in `sustainability_funnel_v2.html`.

## Latest Run Evidence
- Level 1 rows read/written: `3,219,530 / 3,219,530`
- Level 2 rows read/written: `634,561 / 634,561`
- Matching: `n_source=14,180`, `n_auto=7,641`, `n_review=396`, `n_unmatched=6,143`

## Key Files
- `sustainability_funnel_v2.html`
- `src/cli.py`
- `src/preprocess_gleif.py`
- `src/build_sustainability_source.py`
- `src/match_sustainability.py`
- `src/build_network.py`
- `docs/methodology.md`
- `docs/data_dictionary.md`
- `docs/matching_eval.md`
- `docs/network_sanity.md`

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

## Quickstart
```bash
make setup
make inspect
make preprocess-level1
make preprocess-level2
make build-source
make match
make network
```

## Full Pipeline
```bash
make run-all
```

Equivalent CLI commands:

```bash
python -m src.cli inspect-inputs
python -m src.cli preprocess-gleif-level1
python -m src.cli preprocess-gleif-level2 --parse-repex
python -m src.cli build-sustainability-source
python -m src.cli match-sustainability
python -m src.cli build-network
python -m src.cli run-all
```

## Quality And Validation
- Matching evaluation workflow: `tools/sample_matching_eval.py` and `tools/generate_matching_eval_report.py`
- Matching quality report: `docs/matching_eval.md`
- Network sanity report: `docs/network_sanity.md`
- Matching precision is only computed after manual labels are added in `data/samples/matching_eval_labels.csv`; otherwise reports remain `PENDING MANUAL LABELS`

## Limitations
- Raw GLEIF and sustainability-source files are local and not committed.
- Initiative lists may be partial, for example `RE100 first100`.
- Fuzzy matching produces review and unmatched cases and should not be treated as ground truth without manual validation.
