"""Unit tests for the demo traffic generator's status state machine."""

import random

from generate_traffic import STATUS_FLOW, TERMINAL_STATUSES, next_status


def test_should_return_none_for_terminal_statuses():
    rng = random.Random(42)

    for status in TERMINAL_STATUSES:
        assert next_status(status, rng) is None


def test_should_only_transition_to_allowed_statuses():
    rng = random.Random(42)

    for status, transitions in STATUS_FLOW.items():
        allowed = {target for target, _ in transitions}
        observed = {next_status(status, rng) for _ in range(200)}
        assert observed <= allowed


def test_should_always_reach_a_terminal_status():
    rng = random.Random(42)

    for _ in range(50):
        status = "pending"
        for _step in range(10):  # longest path: pending→confirmed→shipped→delivered→completed
            nxt = next_status(status, rng)
            if nxt is None:
                break
            status = nxt
        assert status in TERMINAL_STATUSES
