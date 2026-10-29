from cardpulse_frameworks.common.pan import (
    REDACTION,
    contains_pan,
    is_pan,
    luhn_check_digit,
    luhn_valid,
    redact,
)
from cardpulse_synth.reference import TEST_PANS


def test_public_test_pans_are_luhn_valid():
    assert all(luhn_valid(p) for p in TEST_PANS)


def test_short_or_non_numeric_values_are_not_pans():
    assert not luhn_valid("424242424242")  # 12 digits
    assert not luhn_valid("4111a11111111111")
    assert not is_pan("")
    assert not is_pan(None)


def test_check_digit_round_trip():
    partial = "411111111111111"
    assert luhn_valid(partial + str(luhn_check_digit(partial)))


def test_separators_are_detected():
    assert is_pan("4111 1111 1111 1111")
    assert is_pan("4111-1111-1111-1111")
    assert contains_pan("PMT REF CARD 4111-1111-1111-1111 THANKS")


def test_free_text_without_pan_passes():
    assert not contains_pan("BLUE OAK MARKET #4521")
    assert not contains_pan("REF 7010000000000996")  # account-number-like, Luhn-invalid


def test_redact_removes_only_pans():
    text = "D|202703150000000001|4111111111111111|7010000000000996|ACME"
    out = redact(text)
    assert "4111111111111111" not in out and REDACTION in out
    assert "7010000000000996" in out
