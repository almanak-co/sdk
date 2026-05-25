"""Characterization tests for ``IntentCompiler._compile_lp_close``.

Unit-level characterization tests for ``IntentCompiler._compile_lp_close``.
These tests pin current observable behaviour with mocked SDK / adapter /
on-chain seams so a regression during refactor is caught in seconds instead
of ~30 minutes of Anvil-fork intent tests.

Scope:
    - Protocol dispatch (Uniswap V4, TraderJoe V2, Aerodrome, Aerodrome
      Slipstream, Pendle, Curve, Fluid, Solana variants).
    - Connector-owned Uniswap V3-family compiler path.
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

from almanak.framework.connectors.base.compiler import BaseCompilerContext
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent

# ---------------------------------------------------------------------------
# Module-level patch target for connector-owned Uniswap V3-family LP close.
# ---------------------------------------------------------------------------

LP_ADAPTER_CLS = "almanak.framework.connectors.uniswap_v3.adapter.UniswapV3LPAdapter"


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
# Per-protocol happy paths (connector compiler)
# ---------------------------------------------------------------------------


class TestCompileLPCloseV3HappyPaths:
    """Per-protocol happy paths through the Uniswap V3-family connector compiler."""

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
    def test_collect_fees_false_still_collects_on_close(
        self, mock_adapter_cls: MagicMock
    ) -> None:
        """collect_fees=False cannot suppress collect on a close.

        decreaseLiquidity moves principal into tokensOwed and burn() reverts
        unless tokensOwed is zero, so collect is mandatory on an active-position
        close. The flag is honoured as informational: collect still fires and a
        warning records that collect_fees=False was overridden.
        """
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
        # Pin exact ordering: decrease -> collect -> burn (collect is required
        # to satisfy the burn precondition; collect_fees=False is overridden).
        assert tx_types == ["lp_decrease_liquidity", "lp_collect", "lp_burn"]
        assert any("collect_fees=False ignored" in w for w in result.warnings)


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
    """Verify ``_compile_lp_close`` routes folded protocols through the connector registry."""

    def _assert_connector_dispatch(self, *, protocol: str, chain: str, pool: str | None = None) -> None:
        compiler = _make_compiler(chain=chain)
        sentinel = MagicMock(name=f"{protocol}-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_close_intent(protocol=protocol, pool=pool)
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with(protocol)
        connector_compiler.compile.assert_called_once()
        ctx, routed_intent = connector_compiler.compile.call_args.args
        assert isinstance(ctx, BaseCompilerContext)
        assert routed_intent is intent

    def test_uniswap_v4_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        self._assert_connector_dispatch(protocol="uniswap_v4", chain=compiler.chain)

    def test_traderjoe_v2_dispatches_to_connector_compiler(self) -> None:
        self._assert_connector_dispatch(protocol="traderjoe_v2", chain="avalanche", pool="USDC/AVAX/20")

    def test_aerodrome_dispatches_to_connector_compiler(self) -> None:
        self._assert_connector_dispatch(protocol="aerodrome", chain="base", pool="WETH/USDC/volatile")

    def test_aerodrome_slipstream_dispatches_to_connector_compiler(self) -> None:
        self._assert_connector_dispatch(protocol="aerodrome_slipstream", chain="base", pool="USDC/WETH/500")

    def test_pendle_dispatches_to_connector_compiler(self) -> None:
        self._assert_connector_dispatch(protocol="pendle", chain="ethereum")

    def test_curve_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler(chain="ethereum")
        sentinel = MagicMock(name="curve-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_close_intent(protocol="curve")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("curve")
        connector_compiler.compile.assert_called_once()
        args, kwargs = connector_compiler.compile.call_args
        assert len(args) == 2
        ctx, dispatched_intent = args
        assert isinstance(ctx, BaseCompilerContext)
        assert dispatched_intent is intent
        assert kwargs == {}

    def test_fluid_dispatches_to_connector_compiler(self) -> None:
        compiler = _make_compiler(chain="arbitrum")
        sentinel = MagicMock(name="fluid-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_close_intent(protocol="fluid")
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with("fluid")
        connector_compiler.compile.assert_called_once()
        args, kwargs = connector_compiler.compile.call_args
        assert len(args) == 2
        ctx, dispatched_intent = args
        assert isinstance(ctx, BaseCompilerContext)
        assert dispatched_intent is intent
        assert kwargs == {}


# ---------------------------------------------------------------------------
# Solana dispatch
# ---------------------------------------------------------------------------


class TestCompileLPCloseSolana:
    """Solana-chain-only dispatch and error messaging.

    Post-fold: Meteora/Orca/Raydium LP compilers are connector-owned and
    dispatched via :data:`CompilerRegistry`. The dispatch site
    (``IntentCompiler._compile_lp_close`` via ``_resolve_lp_protocol``)
    normalises the Solana default protocol and gates non-Solana protocols
    BEFORE the registry lookup; per-protocol "wrong chain" errors come from
    the connector compilers themselves.
    """

    def _assert_solana_connector_dispatch(self, *, protocol: str) -> None:
        compiler = _make_compiler(chain="solana")
        sentinel = MagicMock(name=f"{protocol}-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_close_intent(protocol=protocol)
            result = compiler.compile(intent)

        assert result is sentinel
        mock_get_compiler.assert_called_once_with(protocol)
        connector_compiler.compile.assert_called_once()
        ctx, routed_intent = connector_compiler.compile.call_args.args
        assert isinstance(ctx, BaseCompilerContext)
        assert ctx.chain == "solana"
        assert routed_intent is intent

    def test_meteora_dlmm_on_solana_dispatches_to_connector_compiler(self) -> None:
        self._assert_solana_connector_dispatch(protocol="meteora_dlmm")

    def test_orca_whirlpools_on_solana_dispatches_to_connector_compiler(self) -> None:
        self._assert_solana_connector_dispatch(protocol="orca_whirlpools")

    def test_raydium_clmm_on_solana_dispatches_to_connector_compiler(self) -> None:
        self._assert_solana_connector_dispatch(protocol="raydium_clmm")

    def test_meteora_dlmm_off_solana_fails(self) -> None:
        """Meteora on a non-Solana chain fails via the connector's own chain check.

        The dispatch site no longer pre-rejects Meteora on EVM chains; the
        connector ``MeteoraCompiler`` enforces ``chain in {"solana"}`` and
        returns the canonical error message.
        """
        compiler = _make_compiler(chain="ethereum")
        intent = _make_lp_close_intent(protocol="meteora_dlmm")
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Meteora DLMM is only supported on Solana" in (result.error or "")

    def test_orca_whirlpools_off_solana_fails(self) -> None:
        """Orca Whirlpools on a non-Solana chain fails via the connector's own chain check."""
        compiler = _make_compiler(chain="ethereum")
        intent = _make_lp_close_intent(protocol="orca_whirlpools")
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Orca Whirlpools is only supported on Solana" in (result.error or "")

    def test_raydium_clmm_off_solana_fails(self) -> None:
        """Raydium on a non-Solana chain fails via the connector's own chain check.

        Post-fold the connector compiler enforces ``chain in {"solana"}`` in
        its own ``compile()``, replacing the pre-fold downstream Raydium
        adapter error with a clean "only supported on Solana" message.
        """
        compiler = _make_compiler(chain="ethereum")
        intent = _make_lp_close_intent(protocol="raydium_clmm")
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "Raydium CLMM is only supported on Solana" in (result.error or "")

    def test_unsupported_protocol_on_solana_fails(self) -> None:
        """Solana chain + non-Solana protocol => FAILED with the canonical message
        (raised by the dispatch site before the connector registry lookup)."""
        compiler = _make_compiler(chain="solana")
        intent = _make_lp_close_intent(protocol="uniswap_v3")
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "not supported for LP_CLOSE on Solana" in (result.error or "")

    def test_unsupported_protocol_on_solana_carries_intent_id(self) -> None:
        """LP_CLOSE FAILED result from Solana protocol gate stamps intent_id."""
        compiler = _make_compiler(chain="solana")
        intent = _make_lp_close_intent(protocol="uniswap_v3")
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert result.intent_id == intent.intent_id

    def test_solana_lp_close_protocol_alias_is_normalized(self) -> None:
        """Case- and hyphen-variant aliases (e.g. ``Meteora-DLMM``) route to the canonical connector."""
        compiler = _make_compiler(chain="solana")
        sentinel = MagicMock(name="meteora-aliased-result")
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.chains = frozenset({"solana"})
        connector_compiler.compile.return_value = sentinel
        with patch(
            "almanak.framework.intents.compiler.get_connector_compiler",
            return_value=connector_compiler,
        ) as mock_get_compiler:
            intent = _make_lp_close_intent(protocol="Meteora-DLMM")
            result = compiler.compile(intent)

        assert result is sentinel
        # Canonical key (lowercased, hyphens to underscores) is what we look
        # up in the registry, NOT the raw user-supplied alias.
        mock_get_compiler.assert_called_once_with("meteora_dlmm")


class TestBuildCompilerContextSolanaRPC:
    """RPC-URL pass-through for Solana-only connector compilers.

    Solana LP adapters (Meteora, Orca, Raydium) do not yet route through
    the gateway and need a direct ``rpc_url``. ``_get_chain_rpc_url`` returns
    ``None`` when a gateway client is connected, so the compiler must pass
    the raw ``self.rpc_url`` straight through for Solana-only connectors.
    """

    def _make_compiler_with_gateway(self) -> IntentCompiler:
        compiler = _make_compiler(chain="solana")
        gateway_client = MagicMock()
        gateway_client.is_connected = True
        compiler._gateway_client = gateway_client
        compiler.rpc_url = "https://api.mainnet-beta.solana.com"
        return compiler

    def test_solana_only_connector_receives_raw_rpc_url_with_connected_gateway(self) -> None:
        compiler = self._make_compiler_with_gateway()
        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.chains = frozenset({"solana"})

        ctx = compiler._build_compiler_context("meteora_dlmm", connector_compiler)

        assert ctx.rpc_url == "https://api.mainnet-beta.solana.com"

    def test_non_solana_connector_still_resolves_via_gateway_aware_path(self) -> None:
        """EVM connectors keep going through ``_get_chain_rpc_url`` (returns ``None``
        when the gateway is connected — the gateway handles the call)."""
        compiler = _make_compiler(chain="arbitrum")
        gateway_client = MagicMock()
        gateway_client.is_connected = True
        compiler._gateway_client = gateway_client
        compiler.rpc_url = "https://arb1.example.com"

        connector_compiler = MagicMock()
        connector_compiler.context_type = BaseCompilerContext
        connector_compiler.chains = frozenset({"arbitrum", "ethereum"})

        ctx = compiler._build_compiler_context("aave_v3", connector_compiler)

        assert ctx.rpc_url is None

    def test_solana_only_detector_rejects_unknown_chains_attribute(self) -> None:
        """Connectors that don't expose a real ``chains`` iterable fall back to
        the gateway-aware path (defensive — keeps test-double behaviour stable)."""
        connector_compiler = MagicMock()  # ``chains`` is a MagicMock, not iterable.
        assert IntentCompiler._is_solana_only_connector(connector_compiler) is False
