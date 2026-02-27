from src.match_sustainability import MatchThresholds, classify_match_status


def test_thresholds_auto_review_unmatched() -> None:
    thr = MatchThresholds(auto=95.0, review=85.0)
    assert classify_match_status(97.0, thr) == "auto"
    assert classify_match_status(90.0, thr) == "review"
    assert classify_match_status(70.0, thr) == "unmatched"


def test_threshold_none_unmatched() -> None:
    assert classify_match_status(None) == "unmatched"
