"""Characterization tests for ``IntentCompiler._compile_lp_close``.

Phase 6B backlog gate: unit-level characterization tests for
``_compile_lp_close`` (compiler.py ~line 3653, CC 39, 245 LOC) BEFORE the
shared-helper extraction. These tests pin current observable behaviour with
mocked SDK / adapter / on-chain seams so a regression during refactor is
caught in seconds instead of ~30 minutes of Anvil-fork intent tests.

Scope:
    - Protocol dispatch (Uniswap V4, TraderJoe V2, Aerodrome, Aerodrome
      Slipstream, Pendle, Curve, Fluid, Solana variants).
    - Generic Uniswap V3-style body (uniswap_v3, pancakeswap_v3, etc.).
    - Position state edge cases (zero liquidity, no tokens owed, unknown
      tokens owed, closed position, unknown liquidity).
    - Error paths (invalid position id, unknown position manager,
      unsupported protocol on Solana).

Non-scope (by construction):
    - No production code changes.
    - No Web3 / RPC actually constructed (SDK classes / on-chain queries
      are patched at the compiler module seam).
    - Not a replacement for ``tests/intents/`` end-to-end coverage.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent

# ---------------------------------------------------------------------------
# Module-level patch targets. ``_compile_lp_close`` reaches for these
# symbols via compiler.py's own namespace, so patching *here* is what
# intercepts the real call.
# ---------------------------------------------------------------------------

LP_ADAPTER_CLS = "almanak.framework.intents.compiler.UniswapV3LPAdapter"


# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


_DEFAULT_PRICES: dict[str, Decimal] = {
    "ETH": Decimal("2000"),
    "WETH": Decimal("2000"),
    "USDC": Decimal("1"),
    "USDT": Decimal("1"),
    "WBTC": Decimal("60000"),
    "WBNB": Decimal("600"),
    "DAI": Decimal("1"),
    "AVAX": Decimal("30"),
    "SOL": Decimal("150"),
}


def _make_compiler(chain: str = "arbitrum") -> IntentCompiler:
    """Build a compiler without constructing a real gateway / RPC setup.

    ``_compile_lp_close`` doesn't use the oracle directly, but the
    constructor requires one (or ``allow_placeholder_prices=True``).
    """
    config = IntentCompilerConfig()
    return IntentCompiler(
        chain=chain,
        wallet_address="0x1111111111111111111111111111111111111111",
        config=config,
        price_oracle=_DEFAULT_PRICES,
    )


def _make_mock_lp_adapter(
    *,
    position_manager: str = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88",
    decrease_calldata: bytes = b"\xde\xc1",
    collect_calldata: bytes = b"\xc0\x11",
    burn_calldata: bytes = b"\xbb\xbb",
) -> MagicMock:
    """Mock ``UniswapV3LPAdapter`` with deterministic calldata output."""

    adapter = MagicMock(name="MockLPAdapter")
    adapter.get_position_manager_address.return_value = position_manager
    adapter.get_decrease_liquidity_calldata.return_value = decrease_calldata
    adapter.get_collect_calldata.return_value = collect_calldata
    adapter.get_burn_calldata.return_value = burn_calldata
    return adapter


def _make_lp_close_intent(
    *,
    position_id: str = "12345",
    pool: str | None = "USDC/WETH/3000",
    collect_fees: bool = True,
    protocol: str = "uniswap_v3",
    protocol_params: dict | None = None,
) -> LPCloseIntent:
    """Small builder that keeps intent construction noise out of the tests."""
    return Intent.lp_close(
        position_id=position_id,
        pool=pool,
        collect_fees=collect_fees,
        protocol=protocol,
        protocol_params=protocol_params,
    )


# ---------------------------------------------------------------------------
# Per-protocol happy paths (generic V3 body)
# ---------------------------------------------------------------------------


class TestCompileLPCloseV3HappyPaths:
    """Per-protocol happy paths through the generic Uniswap-V3-style body."""

    @pytest.mark.parametrize(
        "protocol",
        ["uniswap_v3", "pancakeswap_v3", "sushiswap_v3"],
    )
    @patch(LP_ADAPTER_CLS)
    def test_happy_path_per_protocol_builds_decrease_collect_burn(
        self, mock_adapter_cls: MagicMock, protocol: str
    ) -> None:
        """Position with liquidity + owed fees => decrease + collect + burn."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=1_000_000),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(10, 20)),
        ):
            intent = _make_lp_close_intent(protocol=protocol)
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["protocol"] == protocol
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["lp_decrease_liquidity", "lp_collect", "lp_burn"]
        # Metadata pins
        assert result.action_bundle.metadata["position_id"] == "12345"
        assert result.action_bundle.metadata["token_id"] == 12345
        assert result.action_bundle.metadata["collect_fees"] is True

    @patch(LP_ADAPTER_CLS)
    def test_collect_fees_false_skips_collect(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """collect_fees=False drops the lp_collect tx and surfaces a warning."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=500),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(1, 2)),
        ):
            intent = _make_lp_close_intent(collect_fees=False)
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        # Pin exact ordering: decrease -> burn (no collect when collect_fees=False).
        assert tx_types == ["lp_decrease_liquidity", "lp_burn"]
        assert any("collect_fees=False" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Position state edge cases
# ---------------------------------------------------------------------------


class TestCompileLPClosePositionStates:
    """Edge cases around on-chain liquidity / tokens-owed query results."""

    @patch(LP_ADAPTER_CLS)
    def test_zero_liquidity_with_tokens_owed_skips_decrease_keeps_collect_burn(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """0 liquidity but owed tokens => skip decrease, still collect+burn."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=0),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(5, 0)),
        ):
            result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        # Pin exact ordering: collect -> burn (decrease skipped on 0 liquidity).
        assert tx_types == ["lp_collect", "lp_burn"]
        assert any("0 liquidity" in w for w in result.warnings)

    @patch(LP_ADAPTER_CLS)
    def test_already_closed_position_skips_everything(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """0 liquidity AND 0 owed => decrease skipped, collect skipped, burn skipped."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=0),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(0, 0)),
        ):
            result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        assert result.transactions == []
        assert any("already closed" in w for w in result.warnings)

    @patch(LP_ADAPTER_CLS)
    def test_unknown_tokens_owed_treats_as_activity(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """tokens_owed=(None, None) => assume activity, emit warning, still collect."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=0),
            patch.object(
                compiler, "_query_position_tokens_owed", return_value=(None, None)
            ),
        ):
            result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        # Pin exact ordering: collect -> burn (decrease skipped on 0 liquidity,
        # unknown owed still treated as activity so collect + burn fire).
        assert tx_types == ["lp_collect", "lp_burn"]
        assert any("Could not query tokens owed" in w for w in result.warnings)

    @patch(LP_ADAPTER_CLS)
    def test_tokens_owed_zero_with_liquidity_still_warns(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """Liquidity > 0, tokens_owed == (0, 0) => pre-decrease warning, full chain."""
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with (
            patch.object(compiler, "_query_position_liquidity", return_value=100),
            patch.object(compiler, "_query_position_tokens_owed", return_value=(0, 0)),
        ):
            result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.SUCCESS, result.error
        tx_types = [tx.tx_type for tx in result.transactions]
        assert tx_types == ["lp_decrease_liquidity", "lp_collect", "lp_burn"]
        assert any("no tokens owed pre-decrease" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestCompileLPCloseErrorPaths:
    """FAILED return paths: invalid inputs, unknown protocol config, query failures."""

    @patch(LP_ADAPTER_CLS)
    def test_invalid_position_id_fails(self, mock_adapter_cls: MagicMock) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        result = compiler.compile(_make_lp_close_intent(position_id="not-an-int"))

        assert result.status == CompilationStatus.FAILED
        assert "Invalid position ID" in (result.error or "")

    @patch(LP_ADAPTER_CLS)
    def test_unknown_position_manager_fails(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter(
            position_manager="0x0000000000000000000000000000000000000000"
        )
        compiler = _make_compiler()

        result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Unknown position manager" in (result.error or "")

    @patch(LP_ADAPTER_CLS)
    def test_liquidity_query_failure_fails(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        mock_adapter_cls.return_value = _make_mock_lp_adapter()
        compiler = _make_compiler()

        with patch.object(compiler, "_query_position_liquidity", return_value=None):
            result = compiler.compile(_make_lp_close_intent())

        assert result.status == CompilationStatus.FAILED
        assert "Could not query liquidity" in (result.error or "")


# ---------------------------------------------------------------------------
# Protocol dispatch (each dedicated helper routed correctly)
# ---------------------------------------------------------------------------


class TestCompileLPCloseDispatch:
    """Verify ``_compile_lp_close`` routes each protocol to the right helper."""

    def test_uniswap_v4_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="uniswap-v4-result")
        with patch.object(
            compiler, "_compile_lp_close_uniswap_v4", return_value=sentinel
        ) as mock_v4:
            intent = _make_lp_close_intent(protocol="uniswap_v4")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_v4.assert_called_once()

    def test_traderjoe_v2_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="avalanche")
        sentinel = MagicMock(name="tj-result")
        with patch.object(
            compiler, "_compile_lp_close_traderjoe_v2", return_value=sentinel
        ) as mock_tj:
            intent = _make_lp_close_intent(protocol="traderjoe_v2", pool="USDC/AVAX/20")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_tj.assert_called_once()

    def test_aerodrome_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="base")
        sentinel = MagicMock(name="aerodrome-result")
        with patch.object(
            compiler, "_compile_lp_close_aerodrome", return_value=sentinel
        ) as mock_aero:
            intent = _make_lp_close_intent(
                protocol="aerodrome", pool="WETH/USDC/volatile"
            )
            result = compiler.compile(intent)

        assert result is sentinel
        mock_aero.assert_called_once()

    def test_aerodrome_slipstream_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="base")
        sentinel = MagicMock(name="slipstream-result")
        with patch.object(
            compiler,
            "_compile_lp_close_aerodrome_slipstream",
            return_value=sentinel,
        ) as mock_slipstream:
            intent = _make_lp_close_intent(
                protocol="aerodrome_slipstream", pool="USDC/WETH/500"
            )
            result = compiler.compile(intent)

        assert result is sentinel
        mock_slipstream.assert_called_once()

    def test_pendle_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="pendle-result")
        with patch.object(
            compiler, "_compile_pendle_lp_close", return_value=sentinel
        ) as mock_pendle:
            intent = _make_lp_close_intent(protocol="pendle")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_pendle.assert_called_once()

    def test_curve_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="curve-result")
        with patch.object(
            compiler, "_compile_lp_close_curve", return_value=sentinel
        ) as mock_curve:
            intent = _make_lp_close_intent(protocol="curve")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_curve.assert_called_once()

    def test_fluid_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="arbitrum")
        sentinel = MagicMock(name="fluid-result")
        with patch.object(
            compiler, "_compile_lp_close_fluid", return_value=sentinel
        ) as mock_fluid:
            intent = _make_lp_close_intent(protocol="fluid")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_fluid.assert_called_once()


# ---------------------------------------------------------------------------
# Solana dispatch
# ---------------------------------------------------------------------------


class TestCompileLPCloseSolana:
    """Solana-chain-only dispatch and error messaging."""

    def test_meteora_dlmm_on_solana_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="solana")
        sentinel = MagicMock(name="meteora-result")
        with (
            patch.object(compiler, "_is_solana_chain", return_value=True),
            patch.object(
                compiler, "_compile_meteora_lp_close", return_value=sentinel
            ) as mock_meteora,
        ):
            intent = _make_lp_close_intent(protocol="meteora_dlmm")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_meteora.assert_called_once()

    def test_meteora_dlmm_off_solana_fails(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        intent = _make_lp_close_intent(protocol="meteora_dlmm")
        with patch.object(compiler, "_is_solana_chain", return_value=False):
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Meteora DLMM is only supported on Solana" in (result.error or "")

    def test_orca_whirlpools_on_solana_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="solana")
        sentinel = MagicMock(name="orca-result")
        with (
            patch.object(compiler, "_is_solana_chain", return_value=True),
            patch.object(
                compiler, "_compile_orca_lp_close", return_value=sentinel
            ) as mock_orca,
        ):
            intent = _make_lp_close_intent(protocol="orca_whirlpools")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_orca.assert_called_once()

    def test_orca_whirlpools_off_solana_fails(self) -> None:
        """Orca Whirlpools on non-Solana chain must FAIL with explicit message."""
        compiler = _make_compiler(chain="ethereum")
        intent = _make_lp_close_intent(protocol="orca_whirlpools")
        with patch.object(compiler, "_is_solana_chain", return_value=False):
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Orca Whirlpools is only supported on Solana" in (result.error or "")

    def test_raydium_clmm_on_solana_dispatches_to_helper(self) -> None:
        compiler = _make_compiler(chain="solana")
        sentinel = MagicMock(name="raydium-result")
        with (
            patch.object(compiler, "_is_solana_chain", return_value=True),
            patch.object(
                compiler, "_compile_raydium_lp_close", return_value=sentinel
            ) as mock_raydium,
        ):
            intent = _make_lp_close_intent(protocol="raydium_clmm")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_raydium.assert_called_once()

    def test_raydium_clmm_off_solana_routes_through_raydium_handler(self) -> None:
        """protocol='raydium_clmm' is unconditionally routed to the Raydium helper.

        Pre-refactor behaviour: the raydium route fires on ``protocol ==
        'raydium_clmm'`` OR on Solana-chain-with-protocol=None. When we force
        a non-Solana chain but keep protocol='raydium_clmm', the Raydium
        helper still owns the call (and the helper itself raises the real
        non-Solana error). Pin this so a regression that adds an early
        off-Solana reject for raydium_clmm is caught.
        """
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="raydium-result")
        with (
            patch.object(compiler, "_is_solana_chain", return_value=False),
            patch.object(
                compiler, "_compile_raydium_lp_close", return_value=sentinel
            ) as mock_raydium,
        ):
            intent = _make_lp_close_intent(protocol="raydium_clmm")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_raydium.assert_called_once()

    def test_unsupported_protocol_on_solana_fails(self) -> None:
        """Solana chain + non-Solana protocol => FAILED with explicit message."""
        compiler = _make_compiler(chain="solana")
        intent = _make_lp_close_intent(protocol="uniswap_v3")
        with patch.object(compiler, "_is_solana_chain", return_value=True):
            result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not supported for LP_CLOSE on Solana" in (result.error or "")
