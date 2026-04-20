"""Tests for GMX V2 (market, collateral) validation in the PERP_OPEN compile path.

GMX V2 silently burns keeper execution fees when ``PERP_OPEN`` orders are
submitted with collateral tokens that are not the market's ``longToken`` or
``shortToken``. The compiler must validate this pair BEFORE emitting any
transactions. This test suite exercises the three relevant behaviours:

1. Valid ``(market, collateral)`` pairs compile successfully.
2. Invalid pairs produce a ``FAILED`` compilation result whose error lists the
   allowed collaterals for the market.
3. Unknown / unregistered markets fall through to the permissive path (the
   compiler may still succeed) — this is the escape hatch for markets that
   ship in the SDK's market list but haven't been registered with the
   ``market_rules`` module yet.

The rule table lives in
``almanak.framework.connectors.gmx_v2.market_rules`` and is the single
source of truth across the SDK.
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.gmx_v2.market_rules import (
    get_allowed_collaterals,
    is_market_registered,
    registered_markets,
    validate_collateral,
)
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError
from almanak.framework.intents.vocabulary import PerpOpenIntent


# =============================================================================
# Fixtures / Helpers
# =============================================================================


def _make_mock_compiler(chain: str = "arbitrum") -> IntentCompiler:
    """Create a compiler with minimal mocking for PERP_OPEN testing.

    Mirrors the helper in test_gmx_v2_perp_approval.py to keep test style
    consistent across the GMX V2 compiler test suite.
    """
    compiler = IntentCompiler.__new__(IntentCompiler)
    compiler.chain = chain
    compiler.wallet_address = "0x" + "1" * 40
    compiler.rpc_url = "http://localhost:8545"
    compiler._approve_cache = {}
    compiler._gateway_client = None
    return compiler


def _make_perp_open_intent(
    collateral_token: str = "USDC",
    collateral_amount: Decimal = Decimal("100"),
    market: str = "ETH/USD",
    size_usd: Decimal = Decimal("1000"),
    is_long: bool = True,
) -> PerpOpenIntent:
    """Create a minimal PerpOpenIntent for the compile path."""
    return PerpOpenIntent(
        market=market,
        collateral_token=collateral_token,
        collateral_amount=collateral_amount,
        size_usd=size_usd,
        is_long=is_long,
        leverage=Decimal("10"),
        protocol="gmx_v2",
    )


def _patch_happy_path():
    """Build mock downstream objects so the compile path can run end-to-end.

    Returns:
        A ``(mock_adapter_result, mock_sdk)`` tuple. Callers are expected to
        apply the returned mocks using ``unittest.mock.patch`` context
        managers at the call site.
    """
    mock_adapter_result = MagicMock()
    mock_adapter_result.success = True
    mock_adapter_result.order_key = "0xabc123"

    mock_sdk = MagicMock()
    mock_sdk.EXCHANGE_ROUTER_ADDRESS = "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41"
    mock_sdk.ROUTER_ADDRESS = "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6"
    mock_sdk.get_execution_fee.return_value = 100000000000000  # 0.0001 ETH
    mock_tx_data = MagicMock()
    mock_tx_data.to = mock_sdk.EXCHANGE_ROUTER_ADDRESS
    mock_tx_data.value = 100000000000000
    mock_tx_data.data = "0xmulticall"
    mock_tx_data.gas_estimate = 500000
    mock_sdk.build_increase_order_multicall.return_value = mock_tx_data

    return mock_adapter_result, mock_sdk


# =============================================================================
# Pure-function tests — the rules module itself
# =============================================================================


class TestMarketRulesPureFunctions:
    """The rule table is the single source of truth; make sure lookups work."""

    def test_eth_usd_arbitrum_is_registered(self):
        assert is_market_registered("arbitrum", "ETH/USD") is True

    def test_unknown_market_is_not_registered(self):
        assert is_market_registered("arbitrum", "FOO/USD") is False

    def test_unknown_chain_is_not_registered(self):
        assert is_market_registered("ethereum", "ETH/USD") is False

    def test_sol_usd_arbitrum_allows_sol_and_usdc(self):
        allowed = get_allowed_collaterals("arbitrum", "SOL/USD")
        assert set(allowed) == {"SOL", "USDC"}

    def test_eth_usd_arbitrum_allows_weth_and_usdc(self):
        allowed = get_allowed_collaterals("arbitrum", "ETH/USD")
        assert set(allowed) == {"WETH", "USDC"}

    def test_avax_usd_avalanche_allows_wavax_and_usdc(self):
        allowed = get_allowed_collaterals("avalanche", "AVAX/USD")
        assert set(allowed) == {"WAVAX", "USDC"}

    def test_get_allowed_raises_for_unknown_market(self):
        with pytest.raises(KeyError):
            get_allowed_collaterals("arbitrum", "FOO/USD")

    def test_registered_markets_sorted(self):
        markets = list(registered_markets("arbitrum"))
        assert markets == sorted(markets)
        assert "ETH/USD" in markets
        assert "SOL/USD" in markets

    def test_validate_accepts_valid_pair(self):
        # Should not raise
        validate_collateral("arbitrum", "ETH/USD", "USDC")
        validate_collateral("arbitrum", "ETH/USD", "WETH")
        validate_collateral("arbitrum", "SOL/USD", "USDC")
        validate_collateral("arbitrum", "SOL/USD", "SOL")

    def test_validate_is_case_insensitive_on_symbol(self):
        validate_collateral("arbitrum", "ETH/USD", "usdc")
        validate_collateral("arbitrum", "ETH/USD", "Usdc")
        validate_collateral("arbitrum", "ETH/USD", "WETH")

    def test_validate_rejects_invalid_collateral_for_sol_usd(self):
        with pytest.raises(InvalidCollateralForMarketError) as exc_info:
            validate_collateral("arbitrum", "SOL/USD", "WETH")
        err = exc_info.value
        assert err.market == "SOL/USD"
        assert err.collateral == "WETH"
        assert err.chain == "arbitrum"
        assert err.protocol == "gmx_v2"
        assert set(err.allowed_collaterals) == {"SOL", "USDC"}
        # The human-readable message must include the allowed set.
        msg = str(err)
        assert "SOL" in msg
        assert "USDC" in msg
        assert "WETH" in msg
        assert "SOL/USD" in msg

    def test_validate_unknown_market_is_permissive(self, caplog):
        """Unknown markets log a warning but do not raise."""
        import logging as _logging

        with caplog.at_level(_logging.WARNING):
            validate_collateral("arbitrum", "FOO/USD", "USDC")
        assert any("FOO/USD" in rec.message for rec in caplog.records)

    def test_validate_address_collateral_is_permissive(self):
        """0x-prefixed addresses are skipped by the symbol validator."""
        # An address for WETH (a token that is NOT allowed for SOL/USD).
        # The symbol-based validator must NOT reject it — the compiler's
        # address resolution path handles address collaterals.
        validate_collateral(
            "arbitrum",
            "SOL/USD",
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        )

    def test_validate_uppercase_0x_address_is_permissive(self):
        """Uppercase 0X-prefixed addresses are also skipped by the symbol validator.

        Some wallets / libraries emit addresses with an uppercase ``0X`` prefix
        (e.g. checksum tooling variants). The validator must treat them the
        same as lowercase ``0x`` addresses — otherwise a caller could be
        falsely rejected because the string starts with ``0X``, is not a
        symbol in the allowed set, and hits the symbol-mismatch raise path.
        """
        validate_collateral(
            "arbitrum",
            "SOL/USD",
            "0X82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        )

    def test_is_market_registered_is_case_insensitive(self):
        """Chain and market inputs normalise to their registry keys."""
        assert is_market_registered("Arbitrum", "ETH/USD") is True
        assert is_market_registered("ARBITRUM", "eth/usd") is True
        assert is_market_registered("arbitrum", " ETH/USD ") is True
        assert is_market_registered("Avalanche", "avax/usd") is True

    def test_get_allowed_collaterals_is_case_insensitive(self):
        """Chain and market inputs normalise before lookup."""
        assert set(get_allowed_collaterals("ARBITRUM", "eth/usd")) == {"WETH", "USDC"}
        assert set(get_allowed_collaterals("Avalanche", " AVAX/USD ")) == {"WAVAX", "USDC"}

    def test_registered_markets_is_case_insensitive(self):
        """Chain input normalises to its registry key."""
        markets_mixed = list(registered_markets("ARBITRUM"))
        markets_lower = list(registered_markets("arbitrum"))
        assert markets_mixed == markets_lower
        assert "ETH/USD" in markets_mixed

    def test_validate_rejects_mixed_case_market_with_bad_collateral(self):
        """Mixed-case market input still triggers the reject path for a bad symbol."""
        with pytest.raises(InvalidCollateralForMarketError):
            validate_collateral("Arbitrum", "sol/usd", "WETH")


# =============================================================================
# Integration with the compiler — the core acceptance criteria
# =============================================================================


class TestPerpOpenCompilerCollateralValidation:
    """End-to-end checks on `_compile_perp_open` for the validation gate."""

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_valid_sol_usd_with_usdc_compiles(self, mock_web3_cls):
        """Valid (SOL/USD, USDC) on GMX V2 Arbitrum compiles successfully."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 100_000_000

        compiler = _make_mock_compiler()
        compiler._build_approve_tx = lambda token_address, spender, amount: []
        compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

        mock_adapter_result, mock_sdk = _patch_happy_path()
        intent = _make_perp_open_intent(
            market="SOL/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("100"),
            is_long=True,
        )

        with (
            patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
            patch("almanak.framework.connectors.GMXv2Config"),
            patch("almanak.framework.connectors.gmx_v2.GMXV2SDK", return_value=mock_sdk),
            patch(
                "almanak.framework.connectors.gmx_v2.GMX_V2_MARKETS",
                {"arbitrum": {"SOL/USD": "0xmarket"}},
            ),
            patch(
                "almanak.framework.connectors.gmx_v2.GMX_V2_TOKENS",
                {"arbitrum": {"USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"}},
            ),
        ):
            mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result
            result = compiler._compile_perp_open(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

    def test_invalid_sol_usd_with_weth_rejects_before_emitting_tx(self):
        """(SOL/USD, WETH) on GMX V2 Arbitrum must fail compilation.

        This is the exact failure mode that burns keeper fees in production
        (ticket source: PRD-StratPositions §Collateral Validation).
        """
        compiler = _make_mock_compiler()
        # These two should never be called if the gate is correctly placed.
        adapter_calls = []
        compiler._build_approve_tx = lambda *args, **kwargs: adapter_calls.append("approve") or []
        compiler._get_chain_rpc_url = lambda: adapter_calls.append("rpc") or "http://localhost:8545"

        intent = _make_perp_open_intent(
            market="SOL/USD",
            collateral_token="WETH",
            collateral_amount=Decimal("0.5"),
            is_long=True,
        )

        # Do NOT patch GMXV2SDK / adapter — we want to prove the compiler
        # short-circuits before they are touched.
        with patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls:
            result = compiler._compile_perp_open(intent)
            assert mock_adapter_cls.called is False, (
                "Collateral validation must happen BEFORE GMXv2Adapter is instantiated; "
                "otherwise strategies burn gas on a doomed order."
            )

        # Belt-and-braces: the pre-TX helpers must not have been called either.
        # If they are, the gate ran AFTER preflight work and we would have
        # wasted RPC/approve setup on a doomed order.
        assert adapter_calls == [], (
            f"Pre-validation helpers were invoked before collateral gate fired: {adapter_calls}"
        )
        assert result.status.value == "FAILED"
        assert result.error is not None
        # Error message should list the allowed collaterals for SOL/USD.
        assert "SOL/USD" in result.error
        assert "SOL" in result.error
        assert "USDC" in result.error
        assert "WETH" in result.error

    @patch("almanak.framework.connectors.gmx_v2.sdk.Web3")
    def test_valid_eth_usd_with_weth_long_compiles(self, mock_web3_cls):
        """Valid (ETH/USD, WETH) on GMX V2 Arbitrum still compiles."""
        mock_web3 = MagicMock()
        mock_web3_cls.return_value = mock_web3
        mock_web3.eth.gas_price = 100_000_000

        compiler = _make_mock_compiler()
        compiler._build_approve_tx = lambda *a, **kw: []
        compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

        mock_adapter_result, mock_sdk = _patch_happy_path()
        intent = _make_perp_open_intent(
            market="ETH/USD",
            collateral_token="WETH",
            collateral_amount=Decimal("0.5"),
            is_long=True,
        )

        with (
            patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
            patch("almanak.framework.connectors.GMXv2Config"),
            patch("almanak.framework.connectors.gmx_v2.GMXV2SDK", return_value=mock_sdk),
            patch(
                "almanak.framework.connectors.gmx_v2.GMX_V2_MARKETS",
                {"arbitrum": {"ETH/USD": "0xmarket"}},
            ),
            patch(
                "almanak.framework.connectors.gmx_v2.GMX_V2_TOKENS",
                {"arbitrum": {"WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"}},
            ),
        ):
            mock_adapter_cls.return_value.open_position.return_value = mock_adapter_result
            result = compiler._compile_perp_open(intent)

        assert result.status.value == "SUCCESS", f"Compilation failed: {result.error}"

    def test_invalid_eth_usd_with_link_rejects(self):
        """(ETH/USD, LINK) must fail — LINK is not long/short token for ETH/USD.

        Downstream adapter / SDK collaborators are patched so the test stays
        unit-scoped even if the validation gate is moved in future refactors:
        if the short-circuit regresses, the mocks stop us making a real RPC /
        adapter call and the final assertions still fail loudly.
        """
        compiler = _make_mock_compiler()
        compiler._build_approve_tx = lambda *a, **kw: []
        compiler._get_chain_rpc_url = lambda: "http://localhost:8545"

        intent = _make_perp_open_intent(
            market="ETH/USD",
            collateral_token="LINK",
            collateral_amount=Decimal("10"),
            is_long=True,
        )

        with (
            patch("almanak.framework.connectors.GMXv2Adapter") as mock_adapter_cls,
            patch("almanak.framework.connectors.GMXv2Config"),
            patch("almanak.framework.connectors.gmx_v2.sdk.Web3"),
        ):
            result = compiler._compile_perp_open(intent)
            assert mock_adapter_cls.called is False, (
                "Collateral validation must short-circuit before adapter construction."
            )

        assert result.status.value == "FAILED"
        assert "LINK" in result.error
        assert "ETH/USD" in result.error

    def test_error_structure_exposes_allowed_collaterals(self):
        """Business contract: callers can programmatically read the allowed set."""
        err = InvalidCollateralForMarketError(
            market="SOL/USD",
            collateral="WETH",
            allowed_collaterals=["SOL", "USDC"],
            chain="arbitrum",
            protocol="gmx_v2",
        )
        assert err.market == "SOL/USD"
        assert err.collateral == "WETH"
        assert err.allowed_collaterals == ["SOL", "USDC"]
        assert err.chain == "arbitrum"
        assert err.protocol == "gmx_v2"
