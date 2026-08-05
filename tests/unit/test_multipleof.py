"""Regression tests for the multipleOf false-negative under IEEE-754 division.

jsonschema's default multipleOf divides two floats and checks the quotient is
integral, which wrongly rejects ordinary decimals against a tiny divisor
(e.g. 4281.23 vs multipleOf 1e-10). Schema.validate now checks multipleOf with
exact Decimal arithmetic; these tests pin the behavior in both directions.
"""
from tap_rest_api.schema import Schema


NUM = {"type": ["null", "number"], "multipleOf": 1e-10}
NESTED = {
    "type": "object",
    "properties": {
        "id": {"type": ["null", "integer"]},
        "line_items": {
            "type": ["null", "array"],
            "items": {
                "type": ["null", "object"],
                "properties": {
                    "home_amount": NUM,
                    "local_amount": NUM,
                    "quantity": NUM,
                },
            },
        },
    },
}


# --- previously-dropped decimals must now validate ---------------------------

def test_scalar_decimals_that_float_division_rejects_are_valid():
    schema = {"type": "number", "multipleOf": 1e-10}
    for value in (594.41, 4281.23, 25467.76, 100.10, 99.99, 12.34, -77.77, 0.0):
        ok, reason = Schema.validate(value, schema)
        assert ok, "expected %r valid, got: %s" % (value, reason)


def test_nested_line_item_amount_is_not_dropped():
    """The DATA-190 case: an invoice whose line-item amount is 4281.23."""
    record = {
        "id": 304114,
        "line_items": [{"home_amount": 4281.23, "local_amount": 4281.23, "quantity": 1.0}],
    }
    ok, reason = Schema.validate(record, NESTED)
    assert ok, reason


# --- genuine violations must still be caught ---------------------------------

def test_true_non_multiple_still_invalid():
    schema = {"type": "number", "multipleOf": 1e-10}
    for value in (1e-11, 1.5e-10, 3.14159265358979):
        ok, _ = Schema.validate(value, schema)
        assert not ok, "expected %r invalid" % value


def test_nested_true_violation_still_invalid():
    record = {"id": 1, "line_items": [{"home_amount": 1e-11, "local_amount": 1.0, "quantity": 1.0}]}
    ok, _ = Schema.validate(record, NESTED)
    assert not ok


# --- unrelated constraints still enforced ------------------------------------

def test_type_errors_still_reported():
    ok, _ = Schema.validate({"id": "not-an-int"}, {"type": "object",
                            "properties": {"id": {"type": "integer"}}})
    assert not ok
