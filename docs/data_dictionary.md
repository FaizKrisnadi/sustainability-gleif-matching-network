# Data Dictionary

## `gleif_entities_clean.csv`
- `row_id`: parser row counter.
- `lei`: legal entity identifier.
- `legal_name`: original legal name.
- `legal_name_clean`: normalized name used for matching.
- `country`: fallback country field.
- `entity_status`: GLEIF entity status.
- `registration_authority`: registration authority code.
- `legal_address_country`: legal address country.
- `headquarters_country`: HQ country.
- `source_snapshot_date`: source content date.
- `source_file`: source file name.

## `gleif_relationships_clean.csv`
- `row_id`: parser row counter.
- `parent_lei`: consolidating parent LEI.
- `child_lei`: reporting child LEI.
- `relationship_type`: relationship type.
- `relationship_status`: relationship status.
- `start_date`: relationship period start.
- `end_date`: relationship period end.
- `period_type`: period type.
- `source_snapshot_date`: source content date.
- `source_file`: source file name.

## `sustainability_source.csv`
- `source_id`: generated source identifier.
- `source_name`: source company name.
- `source_country`: source country.
- `source_sector`: source sector.
- `source_notes`: provenance notes.
- `has_sbti`, `has_re100`, `has_ev100`, `has_ep100`: initiative membership flags.
- `source_url`: optional source URL.
- `source_accessed_date`: build date.

## `match_crosswalk.csv`
- `source_id`, `source_name_raw`, `source_name_clean`
- `matched_lei`, `matched_legal_name`, `matched_legal_name_clean`
- `match_score`, `score_type`, `match_method`, `match_status`
- `blocking_keys`, `notes`

## `edges.csv`
- `firm_i`: child LEI.
- `firm_j`: parent LEI.
- `relation_type`: relationship type.
- `relation_direction`: fixed `child_to_parent`.
- `year`: parsed year from relationship start date.
- `weight`: edge weight (default 1).

## `nodes.csv`
- `lei`: entity LEI.
- `legal_name`: entity legal name.
- `legal_name_clean`: cleaned name.
- `country`: country.
- `in_relationships`: whether node appears in any edge endpoint.

## `nodes_in_network.csv`
- Subset of `nodes.csv` where `lei` appears in `edges.csv` (`firm_i` or `firm_j`).
- Uses the same columns as `nodes.csv`.
