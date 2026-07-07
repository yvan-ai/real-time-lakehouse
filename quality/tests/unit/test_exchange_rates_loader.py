"""Unit tests for the exchange-rates loader transform (ADR-0011).

Only the pure Polars function is covered — fetch (network) and produce
(Kafka) are exercised end-to-end on the cluster, not here.
"""

# Keeps the module importable on Python 3.8 (pytest fallback inside the
# Spark image), where dict[str, Any] is not subscriptable at runtime.
from __future__ import annotations

from typing import Any

import pytest

polars = pytest.importorskip("polars", reason="polars not installed (pipelines/ingestion)")

from exchange_rates_loader import rates_to_events  # noqa: E402

PAYLOAD: dict[str, Any] = {
    "amount": 1.0,
    "base": "EUR",
    "date": "2026-07-07",
    "rates": {"USD": 1.1734, "GBP": 0.8571, "JPY": 169.53, "SEK": 11.2049},
}


def test_one_event_per_selected_currency():
    events = rates_to_events(PAYLOAD, currencies=("USD", "GBP"))

    assert events.height == 2
    assert events["currency"].to_list() == ["GBP", "USD"]  # sorted


def test_empty_currency_filter_keeps_all():
    events = rates_to_events(PAYLOAD, currencies=())

    assert events.height == len(PAYLOAD["rates"])


def test_event_shape_and_values():
    events = rates_to_events(PAYLOAD, currencies=("USD",))
    row = events.to_dicts()[0]

    assert list(events.columns) == [
        "event_type",
        "base",
        "currency",
        "rate",
        "rate_date",
        "fetched_at",
    ]
    assert row["event_type"] == "exchange_rate"
    assert row["base"] == "EUR"
    assert row["rate"] == pytest.approx(1.1734)
    assert row["rate_date"] == "2026-07-07"
    assert row["fetched_at"]  # ISO timestamp set at transform time


def test_unknown_currency_yields_empty_frame():
    events = rates_to_events(PAYLOAD, currencies=("XXX",))

    assert events.is_empty()
