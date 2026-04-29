"""Unit tests for VIB-3702: lending_loop scaffold input validation.

Edge frequently sends a `LENDING_ARBITRAGE` signal where the supply protocol
sits in `sdkSpec.protocol` and the borrow protocol is buried in `metadata`.
AlmanakCode then forces the signal into the SDK's single-protocol
`lending_loop` template and silently drops the borrow leg. The validator
emits structured warnings so the planner / CLI can surface the loss instead
of producing a supply-only "arb" strategy.
"""

from __future__ import annotations

from almanak.framework.cli.new_strategy import (
    LENDING_LOOP_CROSS_PROTOCOL,
    LENDING_LOOP_INCOMPLETE,
    validate_lending_loop_template,
)


def test_warns_when_borrow_protocol_unset() -> None:
    warnings = validate_lending_loop_template(supply_protocol="aave_v3", borrow_protocol=None)
    assert len(warnings) == 1
    msg = warnings[0]
    assert msg.startswith(LENDING_LOOP_INCOMPLETE)
    assert "supply_protocol=aave_v3" in msg
    assert "borrow_protocol=<unset>" in msg


def test_warns_when_borrow_protocol_empty_string() -> None:
    warnings = validate_lending_loop_template(supply_protocol="aave_v3", borrow_protocol="")
    assert len(warnings) == 1
    assert warnings[0].startswith(LENDING_LOOP_INCOMPLETE)


def test_warns_when_borrow_protocol_whitespace_only() -> None:
    warnings = validate_lending_loop_template(supply_protocol="aave_v3", borrow_protocol="   ")
    assert len(warnings) == 1
    assert warnings[0].startswith(LENDING_LOOP_INCOMPLETE)


def test_warns_on_cross_protocol_pair() -> None:
    """Edge S1 case: aave_v3 supply + morpho-blue borrow can't fit lending_loop."""
    warnings = validate_lending_loop_template(
        supply_protocol="aave_v3", borrow_protocol="morpho-blue"
    )
    assert len(warnings) == 1
    msg = warnings[0]
    assert msg.startswith(LENDING_LOOP_CROSS_PROTOCOL)
    assert "supply_protocol=aave_v3" in msg
    assert "borrow_protocol=morpho-blue" in msg
    assert "multi_step" in msg, "Should suggest the multi_step template as the right alternative"


def test_no_warning_for_matching_protocols() -> None:
    """Single-protocol leverage loop is the canonical use of lending_loop."""
    assert validate_lending_loop_template("aave_v3", "aave_v3") == []
    assert validate_lending_loop_template("morpho_blue", "morpho_blue") == []


def test_protocol_match_is_whitespace_tolerant() -> None:
    """Edge inputs sometimes carry stray whitespace; that shouldn't trip the cross-protocol check."""
    assert validate_lending_loop_template(" aave_v3 ", "aave_v3") == []
    assert validate_lending_loop_template("aave_v3", " aave_v3 ") == []


def test_protocol_match_is_case_insensitive() -> None:
    """AlmanakCode and Edge sometimes capitalize differently (AAVE_V3 vs aave_v3);
    that mismatch alone must not trigger the LENDING_LOOP_CROSS_PROTOCOL warning."""
    assert validate_lending_loop_template("AAVE_V3", "aave_v3") == []
    assert validate_lending_loop_template("aave_v3", "AAVE_V3") == []
    assert validate_lending_loop_template("Morpho_Blue", "morpho_blue") == []


def test_warns_when_supply_protocol_empty() -> None:
    warnings = validate_lending_loop_template(supply_protocol="", borrow_protocol="aave_v3")
    assert len(warnings) == 1
    assert warnings[0].startswith(LENDING_LOOP_INCOMPLETE)


def test_warning_codes_are_stable_strings() -> None:
    """The structured prefixes are part of the AlmanakCode planner contract."""
    assert LENDING_LOOP_INCOMPLETE == "LENDING_LOOP_INCOMPLETE"
    assert LENDING_LOOP_CROSS_PROTOCOL == "LENDING_LOOP_CROSS_PROTOCOL"
