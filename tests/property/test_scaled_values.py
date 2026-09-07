from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from iot_energy_pipeline.contracts import MAX_INT64, MIN_INT64, to_scaled_int


@given(
    coefficient=st.integers(min_value=-(10**12), max_value=10**12),
    scale=st.integers(min_value=0, max_value=9),
)
def test_scaled_integer_round_trip_is_exact(coefficient: int, scale: int) -> None:
    source = Decimal(coefficient).scaleb(-scale)
    text = format(source, "f")

    encoded = to_scaled_int(text, scale)

    assert Decimal(encoded).scaleb(-scale) == source


@pytest.mark.parametrize("scale", [-1, 1.5, True, "2"])
def test_scale_must_be_a_non_negative_integer(scale: object) -> None:
    with pytest.raises(ValueError):
        to_scaled_int("1", scale)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "text,scale,expected",
    [
        (str(MIN_INT64), 0, MIN_INT64),
        (str(MAX_INT64), 0, MAX_INT64),
        ("1.2300", 4, 12300),
        ("-0.01", 2, -1),
    ],
)
def test_scaled_integer_boundaries(text: str, scale: int, expected: int) -> None:
    assert to_scaled_int(text, scale) == expected


@pytest.mark.parametrize(
    "text,scale",
    [
        (str(MIN_INT64 - 1), 0),
        (str(MAX_INT64 + 1), 0),
        ("1.001", 2),
        ("NaN", 0),
        ("Infinity", 0),
    ],
)
def test_unrepresentable_values_fail(text: str, scale: int) -> None:
    with pytest.raises((ValueError, OverflowError)):
        to_scaled_int(text, scale)
