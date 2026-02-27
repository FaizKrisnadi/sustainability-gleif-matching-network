# Decision Log

## [2026-02-27] Matching evaluation design
- Sampling seed fixed at `42` with target `100` rows for each group: `auto`, `review`, `unmatched`.
- If a group has fewer than 100 rows, all rows are included; no synthetic rows are created.
- Labeling schema in `data/samples/matching_eval_labels_template.csv`:
  - `label` allowed values: `correct`, `incorrect`, `uncertain` (blank allowed until manual annotation).
  - `notes` free text for failure reasons and reviewer comments.
- Fail-closed rule: if manual labels are missing/blank, precision is not computed and report status is `PENDING MANUAL LABELS`.

## [2026-02-27] Network sanity definitions
- Directed sanity interpretation is fixed for reporting: treat `firm_i` as parent and `firm_j` as child.
- `out_degree`: number of unique children (`firm_j`) per parent (`firm_i`) using deduplicated directed pairs.
- `in_degree`: number of unique parents (`firm_i`) per child (`firm_j`) using deduplicated directed pairs.
- Undirected degree summary uses unique neighbor counts from deduplicated unordered node pairs.
- `missing_node_info_count` is the count of unique LEIs present in edges but absent from `nodes.csv`.
