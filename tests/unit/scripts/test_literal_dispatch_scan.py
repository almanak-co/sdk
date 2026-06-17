"""Unit tests for scripts/ci/literal_dispatch_scan.py detectors.

Tests use inline fixture strings parsed with ast.parse — no live files are read.
Each test covers exactly one detector pattern or one non-hit exclusion rule.
"""

from __future__ import annotations

import ast

import pytest

from scripts.ci.literal_dispatch_scan import (
    _KNOWN_CHAINS,
    _KNOWN_PROTOCOLS,
    _iter_hits,
)


def _parse(source: str) -> ast.AST:
    return ast.parse(source.strip())


_FAKE_REL = "almanak/framework/test_fixture.py"


def _fps(source: str) -> list[str]:
    """Return sorted list of fingerprints from the fixture source."""
    return sorted(fp for _, _, fp in _iter_hits(_parse(source), _FAKE_REL))


# ---------------------------------------------------------------------------
# Known-names sanity
# ---------------------------------------------------------------------------


def test_known_chains_contains_expected() -> None:
    """Chain registry must contain the calibration chain names."""
    for name in ("arbitrum", "ethereum", "optimism", "base", "polygon", "bsc"):
        assert name in _KNOWN_CHAINS, f"Expected {name!r} in _KNOWN_CHAINS"


def test_known_protocols_contains_expected() -> None:
    """Connector registry must contain the calibration protocol names."""
    for name in (
        "uniswap_v3",
        "aave_v3",
        "gmx_v2",
        "hyperliquid",
        "aerodrome_slipstream",
        "morpho",
    ):
        assert name in _KNOWN_PROTOCOLS, f"Expected {name!r} in _KNOWN_PROTOCOLS"


# ---------------------------------------------------------------------------
# Compare detector -- true positives
# ---------------------------------------------------------------------------


def test_compare_eq_protocol_name() -> None:
    """``if protocol == "uniswap_v3"`` is flagged (Eq, protocol keyword)."""
    fps = _fps('if protocol == "uniswap_v3": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:protocol:uniswap_v3"


def test_compare_neq_chain_name() -> None:
    """``if chain != "arbitrum"`` is flagged (NotEq, chain keyword)."""
    fps = _fps('if chain != "arbitrum": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:chain:arbitrum"


def test_compare_in_chain_tuple() -> None:
    """``if chain in ("optimism", "base")`` is flagged (In, chain keyword, tuple literal)."""
    fps = _fps('if chain in ("optimism", "base"): pass')
    assert len(fps) == 1
    assert fps[0] == "compare:chain:base,optimism"


def test_compare_not_in_protocol_list() -> None:
    """``if protocol not in ["aave_v3", "morpho"]`` is flagged (NotIn, protocol keyword)."""
    fps = _fps('if protocol not in ["aave_v3", "morpho"]: pass')
    assert len(fps) == 1
    assert fps[0] == "compare:protocol:aave_v3,morpho"


def test_compare_uppercase_chain() -> None:
    """``if chain_upper == "BSC"`` is flagged (BSC is an alias upper-variant)."""
    fps = _fps('if chain_upper == "BSC": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:chain_upper:BSC"


def test_compare_protocol_lower() -> None:
    """``elif protocol_lower == "hyperliquid"`` is flagged (compound var name)."""
    fps = _fps('x = 1\nelif protocol_lower == "hyperliquid": pass' if False else 'if protocol_lower == "hyperliquid": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:protocol_lower:hyperliquid"


def test_compare_venue_keyword() -> None:
    """``if venue == "uniswap_v3"`` is flagged (venue keyword)."""
    fps = _fps('if venue == "uniswap_v3": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:venue:uniswap_v3"


def test_compare_connector_keyword() -> None:
    """``if connector == "aave_v3"`` is flagged (connector keyword)."""
    fps = _fps('if connector == "aave_v3": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:connector:aave_v3"


def test_compare_dex_keyword() -> None:
    """``if dex == "uniswap_v3"`` is flagged (dex keyword, word-boundary check)."""
    fps = _fps('if dex == "uniswap_v3": pass')
    assert len(fps) == 1
    assert fps[0] == "compare:dex:uniswap_v3"


# ---------------------------------------------------------------------------
# Compare detector -- NON-HITS (exclusions)
# ---------------------------------------------------------------------------


def test_no_flag_parameter_default() -> None:
    """Function parameter default ``protocol: str = "uniswap_v3"`` must NOT be flagged."""
    source = 'def f(protocol: str = "uniswap_v3"): pass'
    assert _fps(source) == []


def test_no_flag_keyword_argument() -> None:
    """Keyword argument ``f(protocol="uniswap_v3")`` must NOT be flagged."""
    source = 'f(protocol="uniswap_v3")'
    assert _fps(source) == []


def test_no_flag_unknown_literal() -> None:
    """``if protocol == "unknown_proto"`` is NOT flagged (not in known names)."""
    fps = _fps('if protocol == "unknown_proto": pass')
    assert fps == []


def test_no_flag_index_var_name() -> None:
    """``if index == "arbitrum"`` must NOT be flagged (``dex`` not at word boundary in ``index``)."""
    fps = _fps('if index == "arbitrum": pass')
    assert fps == []


def test_no_flag_non_domain_var() -> None:
    """``if position_type == "uniswap_v3"`` is NOT flagged (no domain keyword in name)."""
    fps = _fps('if position_type == "uniswap_v3": pass')
    assert fps == []


# ---------------------------------------------------------------------------
# Dict detector -- true positives
# ---------------------------------------------------------------------------


def test_dict_chain_keys() -> None:
    """Dict with >= 2 chain keys is flagged."""
    source = '''
x = {
    "arbitrum": [1, 2],
    "ethereum": [3, 4],
    "optimism": [5, 6],
}
'''
    fps = _fps(source)
    assert len(fps) == 1
    assert fps[0].startswith("dict:chain:")
    assert "arbitrum" in fps[0]
    assert "ethereum" in fps[0]


def test_dict_protocol_keys() -> None:
    """Dict with >= 2 protocol keys is flagged."""
    source = '''
x = {
    "aave_v3": "lend",
    "uniswap_v3": "lp",
}
'''
    fps = _fps(source)
    assert len(fps) == 1
    assert fps[0].startswith("dict:protocol:")
    assert "aave_v3" in fps[0]
    assert "uniswap_v3" in fps[0]


# ---------------------------------------------------------------------------
# Dict detector -- NON-HIT (single known key -- not enough for dispatch)
# ---------------------------------------------------------------------------


def test_dict_single_chain_key_not_flagged() -> None:
    """Dict with only ONE chain key is NOT flagged (not a dispatch pattern)."""
    source = '''
x = {"arbitrum": "https://rpc.example.com", "some_other": "https://other.com"}
'''
    # "some_other" is not a known chain or protocol name
    fps = _fps(source)
    assert fps == []


def test_dict_single_protocol_key_not_flagged() -> None:
    """Dict with only ONE protocol key is NOT flagged."""
    source = '''
x = {"aave_v3": "lending", "other": "stuff"}
'''
    fps = _fps(source)
    assert fps == []


# ---------------------------------------------------------------------------
# Compare detector -- casing-method unwrap (protocol.lower() == "x")
# ---------------------------------------------------------------------------


def test_compare_method_call_lower() -> None:
    """``if protocol.lower() == "morpho_blue"`` unwraps to the receiver name."""
    fps = _fps('if protocol.lower() == "morpho_blue": pass')
    assert fps == ["compare:protocol:morpho_blue"]


def test_compare_method_call_chained_in() -> None:
    """``venue.strip().upper()`` unwraps through chained casing calls."""
    fps = _fps('if venue.strip().casefold() in ("aave_v3", "morpho"): pass')
    assert fps == ["compare:venue:aave_v3,morpho"]


def test_compare_non_casing_method_not_unwrapped() -> None:
    """A non-casing method call (``protocol.encode()``) is NOT a domain operand."""
    fps = _fps('if protocol.encode() == "aave_v3": pass')
    assert fps == []


# ---------------------------------------------------------------------------
# Collection detector -- protocol enumerations
# ---------------------------------------------------------------------------


def test_collection_set_protocol_names() -> None:
    """Bare set literal with >= 2 protocol names is flagged."""
    fps = _fps('PROTOCOLS = {"aave_v3", "morpho", "uniswap_v3"}')
    assert fps == ["collection:protocol:aave_v3,morpho,uniswap_v3"]


def test_collection_frozenset_inner_set() -> None:
    """``frozenset({...})`` is caught via its inner set literal."""
    fps = _fps('PROTOCOLS = frozenset({"aave_v3", "morpho"})')
    assert fps == ["collection:protocol:aave_v3,morpho"]


def test_collection_list_protocol_names() -> None:
    """Bare list literal with >= 2 protocol names is flagged."""
    fps = _fps('v3 = ["uniswap_v3", "pancakeswap_v3", "sushiswap_v3"]')
    assert fps == ["collection:protocol:pancakeswap_v3,sushiswap_v3,uniswap_v3"]


def test_collection_single_protocol_not_flagged() -> None:
    """A collection with only ONE known protocol name is NOT flagged."""
    fps = _fps('x = ["aave_v3", "something_else"]')
    assert fps == []


def test_collection_chain_names_not_flagged() -> None:
    """Collection detector is protocol-scoped: bare chain lists are NOT flagged.

    Chain abbreviations collide with token symbols, so chain-collections are
    intentionally out of scope (see the scanner module docstring).
    """
    fps = _fps('x = ["arbitrum", "ethereum", "optimism"]')
    assert fps == []


def test_collection_in_compare_not_double_counted() -> None:
    """A collection that is a Compare operand is counted once (by compare)."""
    fps = _fps('if protocol in {"aave_v3", "morpho"}: pass')
    assert fps == ["compare:protocol:aave_v3,morpho"]


def test_collection_parameter_default_not_flagged() -> None:
    """A protocol collection in a parameter default is excluded."""
    fps = _fps('def f(p=("aave_v3", "morpho")): pass')
    assert fps == []


# ---------------------------------------------------------------------------
# Docstring non-hit (string constants in expression context)
# ---------------------------------------------------------------------------


def test_no_flag_module_docstring() -> None:
    """Module-level docstring with chain names is not flagged."""
    source = '"""Support chains: arbitrum, ethereum, optimism."""\nx = 1'
    assert _fps(source) == []


def test_no_flag_function_docstring() -> None:
    """Function docstring with protocol names is not flagged."""
    source = '''
def f():
    """Uses protocol aave_v3 or uniswap_v3."""
    pass
'''
    assert _fps(source) == []
