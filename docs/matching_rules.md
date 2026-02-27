# Matching Rules

## Cleaning
- Lowercase and Unicode normalization.
- Replace `&` with `and`.
- Remove punctuation and normalize whitespace.
- Strip legal suffixes cautiously (e.g., Inc, Ltd, LLC, PT, Tbk).

## Blocking
Controlled by `config.yaml`:
- `use_country`
- `use_first_token`
- `use_first_char`
- `max_block_candidates`

## Scoring
- RapidFuzz `token_sort_ratio` and `token_set_ratio`.
- Combined score = `max(token_sort_ratio, token_set_ratio)`.

## Thresholds
Controlled by `config.yaml`:
- `auto_threshold` (default 95)
- `review_threshold` (default 85)
- Scores below review threshold are `unmatched`.

## Review band
- `review` rows are emitted to `review_candidates.csv` with top candidates.
- Manual adjudication is required before downstream use.
