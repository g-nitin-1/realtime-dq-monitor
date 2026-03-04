from realtime_monitor.quality.rules import validate_event


def test_validate_event_valid() -> None:
    event = {
        "event_id": "e1",
        "timestamp": "2026-03-02T00:00:00Z",
        "source": "web",
        "user_id": "u1",
        "status": "ok",
    }
    valid, errors = validate_event(event)
    assert valid
    assert errors == []


def test_validate_event_missing_and_type_errors() -> None:
    event = {"event_id": 1, "status": "oops"}
    valid, errors = validate_event(event)
    assert not valid
    assert "type:event_id" in errors
    assert "missing:timestamp" in errors
    assert "allowed:status" in errors
