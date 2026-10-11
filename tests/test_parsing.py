from src.fetch_flights import parse_state, is_valid


def test_parse_state_normal():
    state = ["4bc8c9", "PGT651  ", "Turkey", 1, 1, -5.3, 33.8, 9738.36,
             False, 250.0, 260.0, 0, None, 10218.42, "2323", False, 0]
    plane = parse_state(state)
    assert plane["callsign"] == "PGT651"
    assert plane["speed_kmh"] == 900
    assert is_valid(plane)


def test_parse_state_with_none():
    state = ["abc123", None, "Morocco", None, None, None, None, None,
             True, None, None, None, None, None, None, False, 0]
    plane = parse_state(state)
    assert plane["callsign"] == ""
    assert plane["speed_kmh"] is None
    assert not is_valid(plane)