"""VIB-4909 — ``wallet_total_value_usd`` must not double-count TOKEN positions.

Production behavior: SWAP-class strategies (e.g. ``UniswapRSISweepStrategy``)
report wallet tokens as ``PositionType.TOKEN`` "positions" via
``get_open_positions()``. Pre-fix, ``PortfolioValuer.value()`` set
``wallet_total_value_usd = wallet_value + position_value``, which counted the
WETH wallet entry once in ``wallet_value`` AND once in ``position_value``
(via the TOKEN pseudo-position). On the 2026-05-28 RSI Arbitrum mainnet
trace this produced a $32.27 wallet_total against an actual on-chain
$25.80 wallet — and silently triggered the reconciler's "framework_won_
large_divergence" warning on every snapshot.

Post-fix: TOKEN-class positions are excluded from the formula so the field
reflects "wallet + real protocol positions (LP / SUPPLY / BORROW / PERP /
VAULT / STAKE / PREDICTION / CEX)" without overlap. See
``PositionType.TOKEN`` docstring for the contract.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
)
from almanak.framework.portfolio.models import PositionValue, TokenBalance
from almanak.framework.valuation.portfolio_valuer import (
    PortfolioValuer,
    _is_wallet_pseudo_position,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_strategy(
    *,
    deployment_id: str = "test-strat",
    chain: str = "arbitrum",
    wallet_address: str = "0x1234567890123456789012345678901234567890",
    tracked_tokens: list[str] | None = None,
    positions: list[PositionInfo] | None = None,
) -> MagicMock:
    """Build a fresh ``StrategyLike`` mock per test.

    CodeRabbit (PR #2530) flagged the legacy ``type(mock).attr = PropertyMock(...)``
    pattern as a class-level mutation that can leak across tests when the
    test runner re-uses a MagicMock class. We set instance attributes
    directly so each call is fully isolated.
    """
    strategy = MagicMock()
    strategy.deployment_id = deployment_id
    strategy.chain = chain
    strategy.wallet_address = wallet_address
    strategy._get_tracked_tokens.return_value = (
        tracked_tokens if tracked_tokens is not None else ["ETH", "USDC"]
    )
    strategy.get_open_positions.return_value = TeardownPositionSummary(
        deployment_id=deployment_id,
        timestamp=datetime.now(UTC),
        positions=positions or [],
    )
    return strategy


def _make_market(
    *,
    prices: dict[str, Decimal] | None = None,
    balances: dict[str, Decimal] | None = None,
) -> MagicMock:
    market = MagicMock()
    _prices = prices or {}
    _balances = balances or {}

    def mock_price(token: str, quote: str = "USD") -> Decimal:
        if token in _prices:
            return _prices[token]
        raise ValueError(f"No price for {token}")

    def mock_balance(token: str) -> MagicMock:
        if token in _balances:
            result = MagicMock()
            result.balance = _balances[token]
            return result
        raise ValueError(f"No balance for {token}")

    market.price = mock_price
    market.balance = mock_balance
    return market


# ---------------------------------------------------------------------------
# Core: the bug repro and its fix
# ---------------------------------------------------------------------------


class TestTokenPositionDoubleCountRegression:
    """VIB-4909 — TOKEN positions must NOT be counted in wallet_total_value_usd.

    Reproduces the RSI mainnet trace pattern: wallet holds WETH, and the
    strategy ALSO reports WETH as a ``TOKEN`` position. Pre-fix the field
    was $32.27 (= $25.80 wallet + $6.47 WETH-as-TOKEN); post-fix it is
    $25.80 (wallet only — TOKEN excluded).
    """

    def test_rsi_mainnet_double_count_repro(self):
        """Match the exact $25.80 / $6.47 numbers from /tmp/rsi_mainnet_test."""
        # WETH @ $3,500: 0.00185... ETH → ~$6.47 — mirror the trace's order of
        # magnitude with cleaner decimals for assertability.
        weth_position_value = Decimal("6.4656194400")
        token_position = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="rsi-weth",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=weth_position_value,
            details={"asset": "WETH"},
        )
        strategy = _make_strategy(
            tracked_tokens=["WETH", "USDC", "ETH"],
            positions=[token_position],
        )
        market = _make_market(
            prices={
                "WETH": Decimal("3500"),
                "USDC": Decimal("1"),
                "ETH": Decimal("3500"),
            },
            balances={
                "WETH": Decimal("0.001847"),   # ≈ $6.46
                "USDC": Decimal("13.18"),
                "ETH": Decimal("0.001760"),    # native gas ≈ $6.16
            },
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # total_value_usd is strategy-scoped (positive position values only,
        # per VIB-3614) — unaffected by this fix.
        assert snapshot.total_value_usd == weth_position_value

        # available_cash_usd is the wallet-only sum — unaffected.
        # WETH 0.001847 * 3500 + USDC 13.18 + ETH 0.001760 * 3500 = 25.8245
        expected_wallet = Decimal("0.001847") * Decimal("3500") + Decimal("13.18") + Decimal("0.001760") * Decimal("3500")
        assert snapshot.available_cash_usd == expected_wallet

        # VIB-4909 — the bug: wallet_total_value_usd MUST equal wallet_value
        # (no double-count). It is NOT wallet_value + WETH-as-TOKEN.
        assert snapshot.wallet_total_value_usd == expected_wallet
        assert snapshot.wallet_total_value_usd != expected_wallet + weth_position_value

    def test_pure_swap_class_with_one_token_position(self):
        """Single TOKEN position whose token is in the tracked wallet."""
        token_pos = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="weth-pseudo",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=Decimal("3500"),
            details={"asset": "WETH"},
        )
        strategy = _make_strategy(
            tracked_tokens=["WETH", "USDC"],
            positions=[token_pos],
        )
        market = _make_market(
            prices={"WETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"WETH": Decimal("1"), "USDC": Decimal("500")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet_value = 1*3500 + 500*1 = $4000
        # non-TOKEN positions = $0
        # wallet_total_value_usd = $4000 (NOT $7500)
        assert snapshot.wallet_total_value_usd == Decimal("4000")

    def test_multiple_token_positions_all_excluded(self):
        """All TOKEN-class positions are excluded — wallet stands alone."""
        positions = [
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="weth-tok",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("3500"),
                details={"asset": "WETH"},
            ),
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="arb-tok",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("100"),
                details={"asset": "ARB"},
            ),
        ]
        strategy = _make_strategy(
            tracked_tokens=["WETH", "USDC", "ARB"],
            positions=positions,
        )
        market = _make_market(
            prices={"WETH": Decimal("3500"), "USDC": Decimal("1"), "ARB": Decimal("1")},
            balances={"WETH": Decimal("1"), "USDC": Decimal("1000"), "ARB": Decimal("100")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet_value = 3500 + 1000 + 100 = $4600
        # wallet_total_value_usd MUST be wallet only — both TOKEN positions
        # excluded — no double-count.
        assert snapshot.wallet_total_value_usd == Decimal("4600")


# ---------------------------------------------------------------------------
# Non-regression: real protocol positions remain ADDED
# ---------------------------------------------------------------------------


class TestRealProtocolPositionsStillAdded:
    """The fix excludes ONLY ``PositionType.TOKEN``; every other type still
    contributes to ``wallet_total_value_usd``. Existing LP / SUPPLY tests in
    ``test_valuation.py`` continue to pass — these cases pin the symmetry.
    """

    @pytest.mark.parametrize(
        ("position_type", "value_usd"),
        [
            (PositionType.LP, Decimal("10000")),
            (PositionType.SUPPLY, Decimal("5000")),
            (PositionType.VAULT, Decimal("7500")),
            (PositionType.STAKE, Decimal("2500")),
            (PositionType.PERP, Decimal("1500")),
            (PositionType.PREDICTION, Decimal("400")),
            (PositionType.CEX, Decimal("800")),
        ],
    )
    def test_non_token_position_added_to_wallet_total(self, position_type, value_usd):
        """Every non-TOKEN position type is summed into wallet_total."""
        position = PositionInfo(
            position_type=position_type,
            position_id=f"pos-{position_type.value.lower()}",
            chain="arbitrum",
            protocol="generic",
            value_usd=value_usd,
            details={},
        )
        strategy = _make_strategy(tracked_tokens=["USDC"], positions=[position])
        market = _make_market(
            prices={"USDC": Decimal("1")},
            balances={"USDC": Decimal("1000")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet_value = $1000 (USDC); non-TOKEN position adds value_usd.
        assert snapshot.wallet_total_value_usd == Decimal("1000") + value_usd

    def test_borrow_position_added_with_sign(self):
        """BORROW positions carry negative ``value_usd`` (a liability); the
        formula must add the signed value so a borrow REDUCES the wallet
        total — matching how a Zerion external comparator nets debt against
        equity.
        """
        borrow = PositionInfo(
            position_type=PositionType.BORROW,
            position_id="aave-usdc-borrow",
            chain="arbitrum",
            protocol="aave_v3",
            value_usd=Decimal("-300"),
            details={},
        )
        strategy = _make_strategy(tracked_tokens=["USDC"], positions=[borrow])
        market = _make_market(
            prices={"USDC": Decimal("1")},
            balances={"USDC": Decimal("1000")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet $1000 + signed borrow -$300 = $700
        assert snapshot.wallet_total_value_usd == Decimal("700")

    def test_mixed_token_and_lp(self):
        """SWAP-class strategy that ALSO holds an LP — only TOKEN drops out."""
        positions = [
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="weth-tok",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("500"),
                details={"asset": "WETH"},
            ),
            PositionInfo(
                position_type=PositionType.LP,
                position_id="lp-weth-usdc",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("2000"),
                details={"tokens": ["WETH", "USDC"]},
            ),
        ]
        strategy = _make_strategy(
            tracked_tokens=["WETH", "USDC"],
            positions=positions,
        )
        market = _make_market(
            prices={"WETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"WETH": Decimal("0.142857142"), "USDC": Decimal("500")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet ≈ 0.142857142 * 3500 + 500 = ~$1000
        # + LP $2000 (TOKEN $500 dropped)
        # wallet_total ≈ $3000 (NOT $3500)
        expected_wallet = Decimal("0.142857142") * Decimal("3500") + Decimal("500")
        assert snapshot.wallet_total_value_usd == expected_wallet + Decimal("2000")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_no_positions_wallet_total_equals_wallet_value(self):
        """No positions at all — wallet_total ≡ wallet_value."""
        strategy = _make_strategy(tracked_tokens=["USDC"], positions=[])
        market = _make_market(
            prices={"USDC": Decimal("1")},
            balances={"USDC": Decimal("1000")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # Confidence policy is orthogonal — the contract under test is the
        # formula: no positions → wallet_total ≡ wallet_value.
        assert snapshot.wallet_total_value_usd == Decimal("1000")
        assert snapshot.wallet_total_value_usd == snapshot.available_cash_usd

    def test_empty_wallet_token_position_no_overlap_is_included(self):
        """Empty wallet (the WETH balance is zero so ``value_tokens`` drops
        the entry) with a TOKEN position whose asset is "WETH" → no
        overlap → position is treated as a deployed holding and contributes
        to ``wallet_total_value_usd``. Codex finding fix: never silently
        drop a TOKEN position that doesn't overlap the wallet.
        """
        token_pos = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="offwallet-weth",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=Decimal("3500"),
            details={"asset": "WETH"},
        )
        strategy = _make_strategy(
            tracked_tokens=["WETH"],
            positions=[token_pos],
        )
        market = _make_market(
            prices={"WETH": Decimal("3500")},
            balances={"WETH": Decimal("0")},  # value_tokens drops zero balances
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # Empty wallet → no overlap → TOKEN counted as deployed holding.
        assert snapshot.wallet_total_value_usd == Decimal("3500")
        assert snapshot.total_value_usd == Decimal("3500")

    def test_metamorpho_vault_shares_token_without_overlap_included(self):
        """Codex P2 — ``metamorpho_eth_yield`` reports vault shares as a
        TOKEN position with ``details = {"vault_address": ..., "deposit_token": "USDC", ...}``
        (NO ``asset`` key). The wallet tracks USDC only (the deposit
        token). The blanket-exclude rule would have silently dropped the
        deployed vault value from ``wallet_total_value_usd``; the
        per-overlap check correctly classifies it as a non-wallet holding
        and includes it.
        """
        # After deposit, USDC wallet is partial; the vault shares carry the
        # remaining deployed value.
        token_pos = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="metamorpho-vault-shares",
            chain="ethereum",
            protocol="metamorpho",
            value_usd=Decimal("950"),
            details={
                "vault_address": "0x1234567890123456789012345678901234567890",
                "deposit_token": "USDC",
                "shares": "1000000000",
            },
        )
        strategy = _make_strategy(
            chain="ethereum",
            tracked_tokens=["USDC"],
            positions=[token_pos],
        )
        market = _make_market(
            prices={"USDC": Decimal("1")},
            balances={"USDC": Decimal("50")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # wallet $50 + vault shares $950 = $1000. No double-count: USDC is
        # not the share token, so no overlap.
        assert snapshot.wallet_total_value_usd == Decimal("1000")
        assert snapshot.total_value_usd == Decimal("950")

    def test_token_position_matcher_is_case_insensitive(self):
        """Overlap match is case-insensitive — ``weth`` (position) matches
        ``WETH`` (wallet) and vice-versa. Prevents the silent-bug class
        where one producer lowercases and another doesn't."""
        token_pos = PositionInfo(
            position_type=PositionType.TOKEN,
            position_id="weth-lc",
            chain="arbitrum",
            protocol="uniswap_v3",
            value_usd=Decimal("3500"),
            details={"asset": "weth"},  # lowercase
        )
        strategy = _make_strategy(
            tracked_tokens=["WETH"],  # uppercase
            positions=[token_pos],
        )
        market = _make_market(
            prices={"WETH": Decimal("3500")},
            balances={"WETH": Decimal("1")},  # wallet has WETH
        )

        snapshot = PortfolioValuer().value(strategy, market)

        # Wallet $3500; TOKEN $3500; case-insensitive overlap → TOKEN excluded.
        assert snapshot.wallet_total_value_usd == Decimal("3500")

    def test_token_position_with_lending_supply_and_borrow(self):
        """Realistic looper: TOKEN pseudo-position + SUPPLY + BORROW. Only
        SUPPLY and BORROW contribute beyond wallet."""
        positions = [
            PositionInfo(
                position_type=PositionType.TOKEN,
                position_id="weth-tok",
                chain="arbitrum",
                protocol="uniswap_v3",
                value_usd=Decimal("500"),
                details={"asset": "WETH"},
            ),
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="aave-weth-sup",
                chain="arbitrum",
                protocol="aave_v3",
                value_usd=Decimal("10000"),
                details={},
            ),
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id="aave-usdc-bor",
                chain="arbitrum",
                protocol="aave_v3",
                value_usd=Decimal("-3000"),
                details={},
            ),
        ]
        strategy = _make_strategy(
            tracked_tokens=["WETH", "USDC"],
            positions=positions,
        )
        market = _make_market(
            prices={"WETH": Decimal("3500"), "USDC": Decimal("1")},
            balances={"WETH": Decimal("0.142857142"), "USDC": Decimal("500")},
        )

        snapshot = PortfolioValuer().value(strategy, market)

        expected_wallet = Decimal("0.142857142") * Decimal("3500") + Decimal("500")
        # wallet + SUPPLY $10k + BORROW -$3k (TOKEN $500 excluded)
        assert snapshot.wallet_total_value_usd == expected_wallet + Decimal("10000") + Decimal("-3000")


# ---------------------------------------------------------------------------
# Helper unit tests — pin the matcher contract directly
# ---------------------------------------------------------------------------


def _pos(
    *,
    position_type: PositionType = PositionType.TOKEN,
    value_usd: Decimal = Decimal("100"),
    details: dict | None = None,
) -> PositionValue:
    return PositionValue(
        position_type=position_type,
        protocol="test",
        chain="arbitrum",
        value_usd=value_usd,
        label="test",
        details=details or {},
    )


class TestIsWalletPseudoPositionHelper:
    """Direct unit tests for ``_is_wallet_pseudo_position``.

    Going through the full PortfolioValuer pipeline doesn't exercise the
    address matcher (``value_tokens`` doesn't populate ``TokenBalance.address``
    from a mocked ``market.balance()`` shape). These tests exercise the
    matcher in isolation.
    """

    def test_non_token_position_is_never_pseudo(self):
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        lp = _pos(position_type=PositionType.LP, details={"asset": "WETH"})
        assert _is_wallet_pseudo_position(lp, wallet) is False

    def test_token_with_asset_symbol_overlap_is_pseudo(self):
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        token = _pos(details={"asset": "WETH"})
        assert _is_wallet_pseudo_position(token, wallet) is True

    def test_token_with_asset_symbol_no_overlap_is_not_pseudo(self):
        wallet = [TokenBalance(symbol="USDC", balance=Decimal("1000"), value_usd=Decimal("1000"))]
        # Vault shares with asset="msUSDC" while wallet only has USDC.
        token = _pos(details={"asset": "msUSDC"})
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_token_with_asset_symbol_case_insensitive(self):
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        for variant in ("weth", "WETH", "wEtH"):
            token = _pos(details={"asset": variant})
            assert _is_wallet_pseudo_position(token, wallet) is True, variant

    def test_token_with_address_overlap_is_pseudo(self):
        weth_addr = "0x82AF49447D8a07E3bD95BD0d56f35241523fBaB1"  # arbitrum WETH
        wallet = [
            TokenBalance(
                symbol="WETH",
                balance=Decimal("1"),
                value_usd=Decimal("3500"),
                address=weth_addr.lower(),
            )
        ]
        token = _pos(details={"address": weth_addr})  # mixed-case
        assert _is_wallet_pseudo_position(token, wallet) is True

    def test_token_with_address_no_overlap_is_not_pseudo(self):
        wallet = [
            TokenBalance(
                symbol="USDC",
                balance=Decimal("1000"),
                value_usd=Decimal("1000"),
                address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            )
        ]
        token = _pos(details={"address": "0x82AF49447D8a07E3bD95BD0d56f35241523fBaB1"})  # WETH
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_token_without_asset_or_address_is_not_pseudo(self):
        """Metamorpho-shape: vault shares with ``vault_address``/
        ``deposit_token``/``shares`` but no ``asset``/``address`` key.
        Defensive include — never silently drop a deployed holding."""
        wallet = [TokenBalance(symbol="USDC", balance=Decimal("50"), value_usd=Decimal("50"))]
        token = _pos(details={
            "vault_address": "0x1234567890123456789012345678901234567890",
            "deposit_token": "USDC",
            "shares": "1000",
        })
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_token_with_empty_details_is_not_pseudo(self):
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        token = _pos(details={})
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_token_with_empty_wallet_is_not_pseudo(self):
        token = _pos(details={"asset": "WETH"})
        assert _is_wallet_pseudo_position(token, []) is False

    def test_token_with_non_string_asset_field_is_not_pseudo(self):
        """Defensive: a malformed details payload (asset=int) must not crash."""
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        token = _pos(details={"asset": 1234})
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_token_matches_first_eligible_overlap(self):
        """Asset symbol is checked before address. Either match returns True."""
        wallet = [
            TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3500")),
            TokenBalance(
                symbol="USDC",
                balance=Decimal("1000"),
                value_usd=Decimal("1000"),
                address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            ),
        ]
        # Symbol match wins for asset=WETH (address absent for that wallet entry).
        token1 = _pos(details={"asset": "WETH"})
        assert _is_wallet_pseudo_position(token1, wallet) is True
        # Address match wins for the USDC entry.
        token2 = _pos(details={"address": "0xAF88D065E77C8CC2239327C5EDB3A432268E5831"})
        assert _is_wallet_pseudo_position(token2, wallet) is True


class TestNonEvmAddressMatcher:
    """Potential #4 from PR #2530 audit — the matcher must not case-fold
    Solana / non-EVM addresses where case is semantically significant.
    """

    def test_solana_base58_address_exact_match(self):
        """A Solana base58 address (case-significant) matches exactly."""
        addr = "So11111111111111111111111111111111111111112"  # wrapped SOL
        wallet = [
            TokenBalance(
                symbol="WSOL",
                balance=Decimal("1"),
                value_usd=Decimal("150"),
                address=addr,
            )
        ]
        token = _pos(details={"address": addr})
        assert _is_wallet_pseudo_position(token, wallet) is True

    def test_solana_base58_case_mismatch_does_not_match(self):
        """Distinct base58 addresses that case-fold to the same string MUST
        remain distinct in the wallet-overlap index. Solana's ``Ab`` and
        ``aB`` are different accounts; treating them as equal would cause
        silent under-counting (false-positive overlap → excluded position
        that wasn't actually in the wallet)."""
        wallet = [
            TokenBalance(
                symbol="WSOL",
                balance=Decimal("1"),
                value_usd=Decimal("150"),
                address="ABCdef0123456789abcdef0123456789abcdef0123XX",  # case-significant
            )
        ]
        # The position carries the same address with one byte flipped to
        # lowercase — a DIFFERENT base58 string in Solana's address space.
        token = _pos(details={"address": "abcdef0123456789abcdef0123456789abcdef0123XX"})
        assert _is_wallet_pseudo_position(token, wallet) is False

    def test_evm_checksum_case_variant_still_matches(self):
        """EIP-55 checksum is display-only — equivalent EVM addresses with
        different case casings MUST still match."""
        weth_checksum = "0x82aF49447D8a07e3bd95BD0d56f35241523fBaB1"
        weth_lower = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        wallet = [
            TokenBalance(
                symbol="WETH",
                balance=Decimal("1"),
                value_usd=Decimal("3500"),
                address=weth_lower,
            )
        ]
        token = _pos(details={"address": weth_checksum})  # different case
        assert _is_wallet_pseudo_position(token, wallet) is True

    def test_non_evm_address_does_not_match_case_folded_evm(self):
        """A non-EVM address (e.g. ``So11111...``) shouldn't accidentally
        match a wallet's EVM-case-folded entries (different shape → wrong
        index → safely returns False)."""
        wallet = [
            TokenBalance(
                symbol="WETH",
                balance=Decimal("1"),
                value_usd=Decimal("3500"),
                address="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            )
        ]
        token = _pos(details={"address": "So11111111111111111111111111111111111111112"})
        assert _is_wallet_pseudo_position(token, wallet) is False


class TestRealStrategyShapesRegression:
    """Important #2 from PR #2530 audit — pin the matcher's behaviour
    against the actual ``details`` payloads emitted by production demo
    strategies. If a future change drifts a demo's ``get_open_positions()``
    payload, these tests fail before the dashboard does.

    Shapes captured 2026-06-01 from the source files at:
    - ``almanak/demo_strategies/uniswap_rsi_sweep/strategy.py:186``
    - ``almanak/demo_strategies/metamorpho_eth_yield/strategy.py:266``
    - ``almanak/demo_strategies/lido_staker/strategy.py:391-396``
    - ``almanak/demo_strategies/pendle_basics/strategy.py:295``
    """

    def test_uniswap_rsi_sweep_shape_is_pseudo_with_weth_wallet(self):
        """uniswap_rsi_sweep emits {"asset": "WETH", ...} after a BUY.
        Wallet that tracks WETH → matcher correctly classifies as
        pseudo-position → excluded from wallet_total_value_usd."""
        wallet = [TokenBalance(symbol="WETH", balance=Decimal("0.01"), value_usd=Decimal("35"))]
        pos = _pos(details={"asset": "WETH", "balance": "0.01"})
        assert _is_wallet_pseudo_position(pos, wallet) is True

    def test_metamorpho_eth_yield_shape_is_deployed_holding(self):
        """metamorpho_eth_yield emits vault shares with details containing
        ``vault_address`` / ``deposit_token`` / ``shares`` (NO ``asset``).
        Wallet tracks USDC (the deposit token). Matcher MUST classify as
        deployed holding (no overlap) so the vault value is preserved."""
        wallet = [TokenBalance(symbol="USDC", balance=Decimal("100"), value_usd=Decimal("100"))]
        pos = _pos(
            details={
                "vault_address": "0x1234567890123456789012345678901234567890",
                "deposit_token": "USDC",
                "shares": "1000000000",
            },
        )
        assert _is_wallet_pseudo_position(pos, wallet) is False

    def test_lido_staker_shape_pseudo_when_steth_tracked(self):
        """lido_staker emits {"asset": "stETH", "source": "lido_stake"}.
        If the operator overrides ``_get_tracked_tokens`` to include
        stETH → wallet has stETH → matcher classifies as pseudo-position.
        """
        wallet = [TokenBalance(symbol="stETH", balance=Decimal("1"), value_usd=Decimal("3500"))]
        pos = _pos(details={"asset": "stETH", "source": "lido_stake"})
        assert _is_wallet_pseudo_position(pos, wallet) is True

    def test_lido_staker_shape_deployed_holding_when_steth_untracked(self):
        """Same lido_staker shape, but the default framework
        ``_get_tracked_tokens()`` returns ``["USDC", "WETH"]`` and stETH
        is NOT in wallet_balances. Matcher MUST classify as deployed
        holding so the staking value is preserved in wallet_total."""
        wallet = [
            TokenBalance(symbol="USDC", balance=Decimal("10"), value_usd=Decimal("10")),
            TokenBalance(symbol="WETH", balance=Decimal("0"), value_usd=Decimal("0")),  # filtered
        ]
        pos = _pos(details={"asset": "stETH", "source": "lido_stake"})
        assert _is_wallet_pseudo_position(pos, wallet) is False

    def test_pendle_basics_shape_has_no_asset_key(self):
        """pendle_basics emits {"market": ..., "pt_token": ..., "base_token": ...}.
        No ``asset`` or ``address`` key → matcher uses the defensive
        default (deployed holding). Documents the call to add the
        convention key as a follow-up sweep across demos."""
        wallet = [TokenBalance(symbol="USDC", balance=Decimal("1000"), value_usd=Decimal("1000"))]
        pos = _pos(
            details={
                "market": "0xaaaaa...",
                "pt_token": "PT-stETH-DEC2026",
                "base_token": "stETH",
            },
        )
        assert _is_wallet_pseudo_position(pos, wallet) is False
