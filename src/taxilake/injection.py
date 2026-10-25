"""Controlled failure injection for proving that task retries work."""


def should_fail(test_id: str, marker_exists: bool) -> bool:
    """Fail only when a test id is set and the first attempt has not already left its marker.

    The first attempt writes the marker and fails; the retry finds the marker and proceeds. With no test id
    (the default) this is always False, so normal runs are unaffected.
    """
    return bool(test_id) and not marker_exists
