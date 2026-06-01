"""Tests for :func:`canonicalize_token_identity` (W1-4 / TA-8).

Read-side token-identity helper. See
``almanak/framework/data/tokens/identity.py`` for the design and the audit
references (``docs/internal/AccountingLastFixesMay22.md`` §C.3 TA-8 + §5
Wave 1 row W1-4).

Test scope:
    1. Symbol vs address equivalence (the core inventory-matching contract).
    2. Multi-chain robustness — 5 EVM chains + Solana — prove the helper
       scales beyond the single-chain bug-fix shape.
    3. Inventory-matching fixture: mixed-form rows collapse to one bucket.
    4. Negative cases: unknown symbols, malformed addresses, cross-family
       address shapes, blank inputs, unknown chains.
    5. Cross-chain isolation: same symbol on different chains stays
       distinct.
"""

from __future__ import annotations

import pytest

from almanak.framework.data.tokens import canonicalize_token_identity
from almanak.framework.data.tokens.exceptions import (
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
)

# ---------------------------------------------------------------------------
# Canonical reference addresses (lowercased hex / case-preserved base58)
# ---------------------------------------------------------------------------
# These are the addresses the helper MUST emit on the right-hand side of the
# canonical tuple. Encoded here so a regression on the resolver's static
# registry surfaces as a test failure on this file too, not only on the
# resolver's own tests.

USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
USDC_OPTIMISM = "0x0b2c639c533813f4aa9d7837caf62653d097ff85"
USDC_POLYGON = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"

WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
WETH_BASE = "0x4200000000000000000000000000000000000006"
WETH_OPTIMISM = "0x4200000000000000000000000000000000000006"
WETH_POLYGON = "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619"

USDC_SOLANA = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# ---------------------------------------------------------------------------
# 1. Symbol-vs-address equivalence (the core contract)
# ---------------------------------------------------------------------------


class TestSymbolAddressEquivalence:
    """A symbol input and an address input for the same token on the same
    chain MUST resolve to the same canonical tuple. Otherwise an inventory
    reader that groups by tuple gets two buckets for one token — the exact
    bug the helper exists to fix."""

    def test_usdc_arbitrum_symbol_matches_lowercase_address(self) -> None:
        sym = canonicalize_token_identity("USDC", "arbitrum")
        addr_lower = canonicalize_token_identity(USDC_ARBITRUM, "arbitrum")
        assert sym == addr_lower == ("arbitrum", USDC_ARBITRUM)

    def test_usdc_arbitrum_symbol_matches_uppercase_address(self) -> None:
        sym = canonicalize_token_identity("USDC", "arbitrum")
        addr_upper = canonicalize_token_identity(USDC_ARBITRUM.upper().replace("0X", "0x"), "arbitrum")
        assert sym == addr_upper

    def test_usdc_arbitrum_symbol_matches_mixed_case_address(self) -> None:
        sym = canonicalize_token_identity("USDC", "arbitrum")
        mixed = canonicalize_token_identity("0xAF88D065E77C8CC2239327C5EDb3A432268e5831", "arbitrum")
        assert sym == mixed

    def test_symbol_is_case_insensitive(self) -> None:
        # The resolver already case-normalizes symbol input; verify the
        # helper preserves that behaviour.
        a = canonicalize_token_identity("USDC", "arbitrum")
        b = canonicalize_token_identity("usdc", "arbitrum")
        c = canonicalize_token_identity("UsDc", "arbitrum")
        assert a == b == c

    def test_whitespace_in_symbol_is_stripped(self) -> None:
        a = canonicalize_token_identity("USDC", "arbitrum")
        b = canonicalize_token_identity("  USDC  ", "arbitrum")
        c = canonicalize_token_identity("\tUSDC\n", "arbitrum")
        assert a == b == c


# ---------------------------------------------------------------------------
# 2. Multi-chain robustness (5 EVM chains + Solana)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chain,expected_address",
    [
        ("arbitrum", USDC_ARBITRUM),
        ("ethereum", USDC_ETHEREUM),
        ("base", USDC_BASE),
        ("optimism", USDC_OPTIMISM),
        ("polygon", USDC_POLYGON),
    ],
)
def test_usdc_canonicalizes_on_each_evm_chain(chain: str, expected_address: str) -> None:
    """USDC resolves to the chain-specific native USDC address on every
    supported EVM chain. The audit-doc's TA-8 scenario plays out across
    all of these chains in production strategies."""
    sym_out = canonicalize_token_identity("USDC", chain)
    addr_out = canonicalize_token_identity(expected_address, chain)
    assert sym_out == addr_out == (chain, expected_address)


@pytest.mark.parametrize(
    "chain,expected_address",
    [
        ("arbitrum", WETH_ARBITRUM),
        ("ethereum", WETH_ETHEREUM),
        ("base", WETH_BASE),
        ("optimism", WETH_OPTIMISM),
        ("polygon", WETH_POLYGON),
    ],
)
def test_weth_canonicalizes_on_each_evm_chain(chain: str, expected_address: str) -> None:
    """WETH resolves to the chain-specific wrapped-ETH address. Base and
    Optimism share `0x4200…0006` because they're both OP-Stack chains —
    that's a real on-chain fact, not a test bug."""
    sym_out = canonicalize_token_identity("WETH", chain)
    addr_out = canonicalize_token_identity(expected_address, chain)
    assert sym_out == addr_out == (chain, expected_address)


def test_usdc_solana_canonicalizes_with_case_preserved() -> None:
    """Solana is the non-EVM chain that proves the case-preserving branch
    of ``_normalize_address_for_chain`` is wired up. Base58 is
    case-sensitive — lowercasing it would break the address."""
    out = canonicalize_token_identity("USDC", "solana")
    assert out == ("solana", USDC_SOLANA)
    # Symbol and address forms agree:
    addr_out = canonicalize_token_identity(USDC_SOLANA, "solana")
    assert addr_out == out
    # The address is NOT lowercased (Solana base58 has mixed case):
    assert out[1] != out[1].lower()
    assert out[1] == USDC_SOLANA


def test_chain_alias_normalization() -> None:
    """The helper accepts the same chain aliases that
    ``resolve_chain_name`` accepts (``eth`` -> ``ethereum``)."""
    eth_alias = canonicalize_token_identity("USDC", "eth")
    eth_canonical = canonicalize_token_identity("USDC", "ethereum")
    assert eth_alias == eth_canonical


def test_chain_enum_accepted() -> None:
    """The helper accepts :class:`Chain` enum instances on the chain
    parameter, matching ``TokenResolver.resolve`` and ``_normalize_chain``
    upstream. Callers that already type their chains as the enum get
    correct static type-checking without having to ``.value`` first."""
    from almanak.core.enums import Chain

    enum_out = canonicalize_token_identity("USDC", Chain.ARBITRUM)
    str_out = canonicalize_token_identity("USDC", "arbitrum")
    assert enum_out == str_out == ("arbitrum", USDC_ARBITRUM)


# ---------------------------------------------------------------------------
# 3. Inventory-matching fixture (the user-facing scenario)
# ---------------------------------------------------------------------------


class TestInventoryMatching:
    """Two SWAP-style rows written by different code paths (one symbol-form,
    one address-form, or one mixed-case) must collapse to ONE inventory
    bucket when grouped by the helper's output tuple. This is the read-side
    contract that unblocks dashboards before the Wave 3 writer fix lands."""

    def test_symbol_form_and_address_form_collapse_to_one_bucket(self) -> None:
        # Simulate two SWAP accounting rows persisted by different writers:
        # RSI / BTD strategies persist symbols; Enso / MACD persist
        # canonical lowercase addresses (audit doc §C.3 TA-8).
        rsi_row = {"token": "USDC", "chain": "arbitrum", "amount_usd": 100.00}
        macd_row = {"token": USDC_ARBITRUM, "chain": "arbitrum", "amount_usd": 50.00}

        buckets: dict[tuple[str, str], list[dict[str, object]]] = {}
        for row in (rsi_row, macd_row):
            key = canonicalize_token_identity(str(row["token"]), str(row["chain"]))
            buckets.setdefault(key, []).append(row)

        assert len(buckets) == 1, f"expected one bucket, got {len(buckets)}: {buckets}"
        only_bucket = next(iter(buckets.values()))
        assert {r["amount_usd"] for r in only_bucket} == {100.00, 50.00}

    def test_mixed_case_addresses_collapse_to_one_bucket(self) -> None:
        # Same token, persisted by two writers with different hex casing.
        # Without canonicalization a naive `GROUP BY token_address` SQL
        # would split this into two rows.
        upper_row = {"token": USDC_ARBITRUM.upper().replace("0X", "0x"), "chain": "arbitrum"}
        lower_row = {"token": USDC_ARBITRUM, "chain": "arbitrum"}
        mixed_row = {"token": "0xAF88d065e77c8CC2239327C5EDb3A432268E5831", "chain": "arbitrum"}

        keys = {
            canonicalize_token_identity(str(r["token"]), str(r["chain"])) for r in (upper_row, lower_row, mixed_row)
        }
        assert len(keys) == 1

    def test_aliased_chain_collapses_with_canonical_chain(self) -> None:
        # If one writer recorded ``"eth"`` and another recorded ``"ethereum"``
        # but the underlying token is the same, the helper collapses them
        # via the chain alias map.
        a = canonicalize_token_identity("USDC", "eth")
        b = canonicalize_token_identity(USDC_ETHEREUM, "ethereum")
        assert a == b


# ---------------------------------------------------------------------------
# 4. Cross-chain isolation (same symbol, different chain == different bucket)
# ---------------------------------------------------------------------------


class TestCrossChainIsolation:
    """The same symbol on different chains is a DIFFERENT token. Collapsing
    them into one bucket would corrupt inventory accounting."""

    def test_usdc_arbitrum_and_ethereum_are_distinct(self) -> None:
        arb = canonicalize_token_identity("USDC", "arbitrum")
        eth = canonicalize_token_identity("USDC", "ethereum")
        assert arb != eth
        assert arb == ("arbitrum", USDC_ARBITRUM)
        assert eth == ("ethereum", USDC_ETHEREUM)

    def test_all_evm_chains_yield_distinct_usdc_tuples(self) -> None:
        chains_and_addrs = [
            ("arbitrum", USDC_ARBITRUM),
            ("ethereum", USDC_ETHEREUM),
            ("base", USDC_BASE),
            ("optimism", USDC_OPTIMISM),
            ("polygon", USDC_POLYGON),
        ]
        keys = {canonicalize_token_identity("USDC", chain) for chain, _ in chains_and_addrs}
        # Five chains -> five distinct tuples. (Base and Optimism share a
        # WETH address but their USDC addresses are still distinct.)
        assert len(keys) == 5

    def test_solana_usdc_distinct_from_evm_usdc(self) -> None:
        sol = canonicalize_token_identity("USDC", "solana")
        arb = canonicalize_token_identity("USDC", "arbitrum")
        assert sol != arb


# ---------------------------------------------------------------------------
# 5. Idempotence (helper output cycles back through unchanged)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token,chain",
    [
        ("USDC", "arbitrum"),
        ("WETH", "ethereum"),
        ("USDC", "base"),
        ("USDC", "solana"),
        ("USDC", "polygon"),
        (USDC_ARBITRUM.upper().replace("0X", "0x"), "arbitrum"),
    ],
)
def test_idempotent(token: str, chain: str) -> None:
    """Calling the helper twice — first with the user's input, then with
    the helper's own output — yields the same tuple. Critical for read-side
    pipelines that may canonicalize at multiple layers."""
    first = canonicalize_token_identity(token, chain)
    second = canonicalize_token_identity(first[1], first[0])
    assert first == second


# ---------------------------------------------------------------------------
# 6. Negative cases
# ---------------------------------------------------------------------------


class TestNegativeCases:
    """Helper never silently defaults. Every failure mode raises a typed
    exception from the existing resolver hierarchy."""

    def test_unknown_symbol_raises_token_not_found(self) -> None:
        with pytest.raises(TokenNotFoundError):
            canonicalize_token_identity("DEFINITELY_NOT_A_TOKEN_XYZ", "arbitrum")

    def test_malformed_hex_address_raises_invalid_address(self) -> None:
        # 42 chars but non-hex characters
        with pytest.raises(InvalidTokenAddressError):
            canonicalize_token_identity(
                "0xGHIJKL0000000000000000000000000000000000",
                "arbitrum",
            )

    def test_solana_base58_on_evm_chain_raises_invalid_address(self) -> None:
        # A Solana mint sent to an EVM chain is a wrong-family error, not
        # an unknown symbol — the helper raises InvalidTokenAddressError to
        # make that intent clear to callers.
        with pytest.raises(InvalidTokenAddressError):
            canonicalize_token_identity(USDC_SOLANA, "arbitrum")

    def test_evm_hex_on_solana_raises_invalid_address(self) -> None:
        with pytest.raises(InvalidTokenAddressError):
            canonicalize_token_identity(USDC_ARBITRUM, "solana")

    def test_malformed_solana_address_raises_invalid_address(self) -> None:
        """A 32-44 char input on Solana that contains invalid base58
        characters (``0``, ``O``, ``I``, ``l``) is almost certainly a
        typo'd mint address, not a symbol. Surface that as
        ``InvalidTokenAddressError`` so the caller's failure message
        points at the malformed shape — not a confusing
        ``TokenNotFoundError`` from the symbol path."""
        # Replace the final base58 char with ``0`` (a base58-excluded
        # character) so length stays in [32, 44] but the string is no
        # longer a valid base58 mint.
        malformed = USDC_SOLANA[:-1] + "0"
        assert 32 <= len(malformed) <= 44  # guards against future drift
        with pytest.raises(InvalidTokenAddressError):
            canonicalize_token_identity(malformed, "solana")

    def test_empty_token_raises_resolution_error(self) -> None:
        with pytest.raises(TokenResolutionError):
            canonicalize_token_identity("", "arbitrum")

    def test_whitespace_only_token_raises_resolution_error(self) -> None:
        with pytest.raises(TokenResolutionError):
            canonicalize_token_identity("   ", "arbitrum")

    def test_unknown_chain_raises_resolution_error(self) -> None:
        with pytest.raises(TokenResolutionError):
            canonicalize_token_identity("USDC", "totally-not-a-chain")

    def test_non_string_token_raises_resolution_error(self) -> None:
        with pytest.raises(TokenResolutionError):
            canonicalize_token_identity(None, "arbitrum")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 7. Output-shape invariants (defensive checks shared by all branches)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "token,chain",
    [
        ("USDC", "arbitrum"),
        ("WETH", "base"),
        (USDC_ARBITRUM.upper().replace("0X", "0x"), "arbitrum"),
        ("USDC", "solana"),
    ],
)
def test_output_tuple_shape(token: str, chain: str) -> None:
    out = canonicalize_token_identity(token, chain)
    assert isinstance(out, tuple)
    assert len(out) == 2
    assert isinstance(out[0], str) and out[0] == out[0].lower()
    assert isinstance(out[1], str) and out[1]


def test_evm_address_output_is_lowercased() -> None:
    out = canonicalize_token_identity("USDC", "arbitrum")
    assert out[1] == out[1].lower()
    assert out[1].startswith("0x")
    assert len(out[1]) == 42


def test_solana_address_output_preserves_case() -> None:
    out = canonicalize_token_identity("USDC", "solana")
    # Solana base58 mints always contain at least one uppercase character;
    # the canonical USDC mint certainly does.
    assert any(c.isupper() for c in out[1])
    assert out[1] == USDC_SOLANA
