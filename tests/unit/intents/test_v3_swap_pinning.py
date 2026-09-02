"""Unit tests for per-intent V3 pool pinning — ``SwapIntent.swap_params``
``fee_tier`` / ``pool`` keys consumed by the Uniswap V3 fork compiler.

Covers three layers:

1. ``SwapIntent.swap_params`` schema — construct / validate / serialize for the
   two pinning keys, mirroring the VIB-5548 Aerodrome suite.
2. ``DefaultSwapAdapter.pin_fee_tier`` — per-intent fixed mode with
   ``source="intent_pinned"`` and single-tier quoting.
3. ``UniswapV3Compiler.compile_swap`` — a pinned swap either compiles against
   exactly the requested pool/tier or FAILS; it never degrades to auto tier
   selection.
"""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak import (
    DefaultSwapAdapter,
    IntentCompiler,
    IntentCompilerConfig,
    SwapIntent,
)
from almanak.connectors._strategy_base.v3_pool_validation import V3PoolBinding
from almanak.framework.intents.vocabulary import Intent

# Arbitrum mainnet addresses (same fixtures as test_swap_fee_selection.py).
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
# Canonical Arbitrum USDC/WETH 0.05% pool.
PINNED_POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"
# Base mainnet addresses.
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_BASE = "0x4200000000000000000000000000000000000006"


def _offline_compiler(chain: str = "arbitrum", **config_overrides) -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        config=IntentCompilerConfig(allow_placeholder_prices=True, **config_overrides),
    )


@pytest.fixture(autouse=True)
def offline_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``_offline_compiler`` actually offline.

    ``allow_placeholder_prices=True`` decouples PRICES from reality; it does NOT
    disable transport resolution. With no explicit ``rpc_url``,
    ``_get_chain_rpc_url`` still falls through to a managed-Anvil port and then
    to a free public RPC, so 9 of these tests were issuing real outbound HTTPS
    and passing only because the machine had internet — tests that fail in a
    network-restricted runner and whose green says nothing about the code under
    test.

    Pinning the resolver to ``None`` is the honest form of the name: no gateway,
    no RPC, so every ``eth_call`` returns ``None`` and the compile exercises the
    pin logic alone. Verified with ``socket.connect`` blocked to non-loopback
    addresses: the file passes in 0.07s instead of 22s.
    """
    monkeypatch.setattr(IntentCompiler, "_get_chain_rpc_url", lambda self: None)


def _swap_intent(**overrides) -> SwapIntent:
    params = {
        "from_token": USDC_ARB,
        "to_token": WETH_ARB,
        "amount": Decimal("100"),
        "max_slippage": Decimal("0.01"),
        "protocol": "uniswap_v3",
    }
    params.update(overrides)
    return SwapIntent(**params)


# ===========================================================================
# Schema: SwapIntent.swap_params pinning keys
# ===========================================================================


def test_swap_params_accepts_fee_tier_pin() -> None:
    intent = _swap_intent(swap_params={"fee_tier": 500})
    assert intent.swap_params == {"fee_tier": 500}


def test_swap_params_accepts_pool_pin() -> None:
    intent = _swap_intent(swap_params={"pool": PINNED_POOL})
    assert intent.swap_params == {"pool": PINNED_POOL}


def test_swap_params_pin_threaded_through_factory() -> None:
    intent = Intent.swap(USDC_ARB, WETH_ARB, amount=Decimal("1"), swap_params={"fee_tier": 100})
    assert intent.swap_params == {"fee_tier": 100}


def test_swap_params_pin_serialize_round_trip() -> None:
    intent = _swap_intent(swap_params={"fee_tier": 500, "pool": PINNED_POOL})
    data = intent.serialize()
    assert data["swap_params"] == {"fee_tier": 500, "pool": PINNED_POOL}
    restored = SwapIntent.deserialize(data)
    assert restored.swap_params == {"fee_tier": 500, "pool": PINNED_POOL}


@pytest.mark.parametrize(
    ("swap_params", "needle"),
    [
        ({"fee_tier": 0}, "swap_params.fee_tier must be a positive integer"),
        ({"fee_tier": -500}, "swap_params.fee_tier must be a positive integer"),
        ({"fee_tier": True}, "swap_params.fee_tier must be a positive integer"),
        ({"fee_tier": "500"}, "swap_params.fee_tier must be a positive integer"),
        ({"pool": "0x123"}, "swap_params.pool must be a 0x-prefixed 20-byte hex address"),
        ({"pool": PINNED_POOL[2:]}, "swap_params.pool must be a 0x-prefixed 20-byte hex address"),
        ({"pool": PINNED_POOL + "ff"}, "swap_params.pool must be a 0x-prefixed 20-byte hex address"),
        ({"pool": 42}, "swap_params.pool must be a 0x-prefixed 20-byte hex address"),
    ],
)
def test_swap_params_rejects_bad_pin_shapes(swap_params: dict, needle: str) -> None:
    with pytest.raises(ValueError, match=needle):
        _swap_intent(swap_params=swap_params)


def test_cross_chain_swap_rejects_pinning() -> None:
    with pytest.raises(ValueError, match="not supported for cross-chain"):
        _swap_intent(
            protocol="enso",
            chain="base",
            destination_chain="arbitrum",
            swap_params={"fee_tier": 500},
        )


@pytest.mark.parametrize("bad_hash", ["A" * 64, "a" * 63, "0x" + "a" * 64, 1, True])
def test_quote_contract_rejects_malformed_venue_binding_hash(bad_hash: object) -> None:
    from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteRequest, SwapQuoteResult

    with pytest.raises(ValueError, match="venue_binding_hash"):
        SwapQuoteRequest(
            chain="arbitrum", protocol="uniswap_v3", token_in=USDC_ARB,
            token_out=WETH_ARB, amount_in=1, venue_binding_hash=bad_hash,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="venue_binding_hash"):
        SwapQuoteResult(amount_out=1, source="test", venue_binding_hash=bad_hash)  # type: ignore[arg-type]


# ===========================================================================
# Adapter: pin_fee_tier
# ===========================================================================


class TestAdapterPinFeeTier:
    def test_pin_flips_to_fixed_mode_with_intent_pinned_source(self) -> None:
        adapter = DefaultSwapAdapter(chain="arbitrum", protocol="uniswap_v3")
        adapter.pin_fee_tier(500)
        fee = adapter.select_fee_tier(USDC_ARB, WETH_ARB, 1_000_000)
        assert fee == 500
        assert adapter.last_fee_selection["mode"] == "fixed"
        assert adapter.last_fee_selection["source"] == "intent_pinned"
        assert adapter.last_fee_selection["selected_fee_tier"] == 500

    def test_pinned_invalid_tier_raises(self) -> None:
        adapter = DefaultSwapAdapter(chain="arbitrum", protocol="uniswap_v3")
        adapter.pin_fee_tier(1234)
        with pytest.raises(ValueError, match="Invalid fixed fee tier 1234"):
            adapter.select_fee_tier(USDC_ARB, WETH_ARB, 1_000_000)

    def test_config_fixed_mode_still_reports_fixed_config_source(self) -> None:
        """pin_fee_tier must not change how compiler-config fixed mode reports."""
        adapter = DefaultSwapAdapter(
            chain="arbitrum",
            protocol="uniswap_v3",
            pool_selection_mode="fixed",
            fixed_fee_tier=500,
        )
        adapter.select_fee_tier(USDC_ARB, WETH_ARB, 1_000_000)
        assert adapter.last_fee_selection["source"] == "fixed_config"

    def test_fixed_mode_quotes_only_the_pinned_tier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Fixed mode quotes its single tier for min-out accuracy but never
        re-selects the tier from the quote."""
        adapter = DefaultSwapAdapter(chain="arbitrum", protocol="uniswap_v3", rpc_url="http://localhost:1")
        adapter.pin_fee_tier(500)
        seen_candidates: list[tuple[int, ...]] = []

        def fake_quoter(from_token, to_token, amount_in, candidates):
            seen_candidates.append(tuple(candidates))
            adapter.last_quoted_amount_out = 999
            # A tier different from the pin must never win.
            return {"fee_tier": 3000, "quoted_candidates": [{"fee_tier": 500, "amount_out": 999}]}

        monkeypatch.setattr(adapter, "_select_fee_tier_by_quoter", fake_quoter)
        fee = adapter.select_fee_tier(USDC_ARB, WETH_ARB, 1_000_000)
        assert fee == 500
        assert seen_candidates == [(500,)]
        assert adapter.last_fee_selection["selected_fee_tier"] == 500
        assert adapter.last_fee_selection["quoted_candidates"] == [{"fee_tier": 500, "amount_out": 999}]
        assert adapter.get_quoted_amount_out() == 999

    def test_fixed_mode_quote_failure_keeps_the_pin(self, monkeypatch: pytest.MonkeyPatch) -> None:
        adapter = DefaultSwapAdapter(chain="arbitrum", protocol="uniswap_v3", rpc_url="http://localhost:1")
        adapter.pin_fee_tier(500)
        monkeypatch.setattr(adapter, "_select_fee_tier_by_quoter", lambda *a, **k: None)
        fee = adapter.select_fee_tier(USDC_ARB, WETH_ARB, 1_000_000)
        assert fee == 500
        assert adapter.get_quoted_amount_out() is None


# ===========================================================================
# Compiler: pinned fee tier
# ===========================================================================


class TestCompilerFeeTierPinning:
    def test_pinned_fee_tier_compiles_fixed(self) -> None:
        result = _offline_compiler().compile(_swap_intent(swap_params={"fee_tier": 500}))
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["pool_selection_mode"] == "fixed"
        assert metadata["selected_fee_tier"] == 500
        assert metadata["fee_selection_source"] == "intent_pinned"
        assert metadata["pinned_pool"] is None
        assert metadata["swap_token_meta"] == {
            "token_in": {"address": USDC_ARB.lower(), "symbol": "USDC", "decimals": 6},
            "token_out": {"address": WETH_ARB.lower(), "symbol": "WETH", "decimals": 18},
        }

    def test_pinned_fee_tier_overrides_auto_config(self) -> None:
        compiler = _offline_compiler()
        assert compiler._config.swap_pool_selection_mode == "auto"
        result = compiler.compile(_swap_intent(swap_params={"fee_tier": 3000}))
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.metadata["selected_fee_tier"] == 3000

    def test_pancakeswap_v3_pinned_tier_on_base(self) -> None:
        intent = SwapIntent(
            from_token=USDC_BASE,
            to_token=WETH_BASE,
            amount=Decimal("100"),
            max_slippage=Decimal("0.01"),
            protocol="pancakeswap_v3",
            swap_params={"fee_tier": 100},
        )
        result = _offline_compiler(chain="base").compile(intent)
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["selected_fee_tier"] == 100
        assert metadata["fee_selection_source"] == "intent_pinned"

    def test_invalid_pinned_fee_tier_fails_loudly(self) -> None:
        result = _offline_compiler().compile(_swap_intent(swap_params={"fee_tier": 1234}))
        assert result.status.value == "FAILED"
        assert "Pinned fee tier 1234 is unusable" in result.error
        assert "Invalid fixed fee tier 1234" in result.error

    def test_no_swap_params_keeps_auto_behavior(self) -> None:
        result = _offline_compiler().compile(_swap_intent())
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["pool_selection_mode"] == "auto"
        assert metadata["fee_selection_source"] != "intent_pinned"
        assert metadata["pinned_pool"] is None

    def test_camelot_rejects_pinning(self) -> None:
        intent = _swap_intent(protocol="camelot", swap_params={"fee_tier": 500})
        result = _offline_compiler().compile(intent)
        assert result.status.value == "FAILED"
        assert "pinning is not supported for" in result.error


class TestUnsupportedConnectorPinGate:
    """An explicit pin routed to a connector that never reads swap_params must
    FAIL, not route normally with the pin silently discarded (PR #3644 review).
    The gate runs on the normalized protocol, before any dispatch."""

    @pytest.mark.parametrize(
        ("protocol", "swap_params"),
        [
            ("traderjoe_v2", {"fee_tier": 500}),
            ("aerodrome", {"fee_tier": 500}),  # aerodrome consumes pool, not fee_tier
            ("enso", {"fee_tier": 500}),
            ("curve", {"fee_tier": 500}),  # curve consumes pool, not fee_tier
        ],
    )
    def test_pin_on_non_consuming_connector_fails(self, protocol: str, swap_params: dict) -> None:
        result = _offline_compiler().compile(_swap_intent(protocol=protocol, swap_params=swap_params))
        assert result.status.value == "FAILED"
        assert "never be silently discarded" in result.error

    def test_default_protocol_resolution_is_gated(self) -> None:
        """protocol=None resolves to the compiler default before the gate."""
        compiler = IntentCompiler(
            chain="ethereum",
            default_protocol="curve",
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        result = compiler.compile(
            SwapIntent(
                from_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC ethereum
                to_token="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH ethereum
                amount=Decimal("100"),
                max_slippage=Decimal("0.01"),
                swap_params={"fee_tier": 500},
            )
        )
        assert result.status.value == "FAILED"
        assert "never be silently discarded" in result.error

    @pytest.mark.parametrize(
        ("pin_key", "pin_value"),
        [("fee_tier", 500), ("pool", PINNED_POOL)],
    )
    def test_protocol_less_pt_swap_is_gated_on_the_INFERRED_protocol(self, pin_key: str, pin_value: object) -> None:
        """A pin must be judged against where it ACTUALLY lands, not the default.

        `_dispatch_swap_protocol_route` consults SWAP_ROUTE_INFERENCE_REGISTRY
        before the chain default, and Pendle claims any protocol-less swap whose
        token symbol starts with PT-/YT-. Resolving `intent.protocol` alone made
        the gate answer a different question than the one it asks: a pinned PT
        swap passed as `uniswap_v3` (which consumes pins), then compiled against
        Pendle (which never reads swap_params) with the pin silently discarded —
        the exact outcome this gate exists to prevent (CodeRabbit, PR #3644).
        """
        compiler = _offline_compiler()
        intent = SwapIntent(
            from_token="PT-wstETH-25JUN2026",
            to_token="USDC",
            amount=Decimal("10"),
            max_slippage=Decimal("0.01"),
            chain="arbitrum",
            swap_params={pin_key: pin_value},
        )
        result = compiler.compile(intent)
        assert result.status.value == "FAILED", "a pin Pendle cannot honour must fail, not be discarded"
        assert "never be silently discarded" in result.error
        assert "'pendle'" in result.error, "the refusal must name the protocol the pin actually reached"

    def test_protocol_less_pt_swap_without_a_pin_is_unaffected(self) -> None:
        """The gate must not regress the unpinned majority it does not govern.

        Asserted at the gate rather than through a full compile: Pendle's
        compile path needs transport, and this file is hermetic by construction
        (see the ``offline_transport`` fixture). The gate is the unit under test
        and it returns None here regardless of what the compile would do.
        """
        compiler = _offline_compiler()
        intent = SwapIntent(
            from_token="PT-wstETH-25JUN2026",
            to_token="USDC",
            amount=Decimal("10"),
            max_slippage=Decimal("0.01"),
            chain="arbitrum",
        )
        assert compiler._resolve_swap_pin_protocol(intent) == "pendle"
        assert compiler._reject_unsupported_swap_pin(intent) is None

    def test_protocol_less_v3_swap_with_a_pin_still_compiles(self) -> None:
        """Inference declines this one, so it falls back to the chain default."""
        result = _offline_compiler().compile(_swap_intent(protocol=None, swap_params={"fee_tier": 500}))
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.metadata["fee_selection_source"] == "intent_pinned"

    def test_curve_pool_pin_passes_the_gate(self) -> None:
        """pool is a curve-consumed key; the gate must not reject it."""
        compiler = _offline_compiler()
        intent = _swap_intent(protocol="curve", swap_params={"pool": PINNED_POOL})
        assert compiler._reject_unsupported_swap_pin(intent) is None

    def test_alias_normalization_passes_the_gate(self) -> None:
        """'agni' on mantle normalizes to agni_finance, a pin-capable fork."""
        compiler = _offline_compiler(chain="mantle")
        intent = _swap_intent(protocol="agni", swap_params={"fee_tier": 500})
        assert compiler._reject_unsupported_swap_pin(intent) is None

    def test_non_pin_keys_do_not_trip_the_gate(self) -> None:
        compiler = _offline_compiler()
        intent = _swap_intent(protocol="curve", swap_params={"oracle_guard_bps": 300})
        assert compiler._reject_unsupported_swap_pin(intent) is None


# ===========================================================================
# Compiler: pinned pool address
# ===========================================================================


def _patch_pool_reads(
    monkeypatch: pytest.MonkeyPatch,
    *,
    binding: V3PoolBinding | None,
    factory_pool_address: str | None = PINNED_POOL,
) -> None:
    """Patch the on-chain pool reads at the module the compiler imports from."""
    import almanak.connectors.uniswap_v3.pool_validation as pv

    monkeypatch.setattr(pv, "read_v3_pool_binding", lambda *a, **k: binding)

    from almanak.connectors._strategy_base.pool_validation_base import (
        PoolValidationReason,
        PoolValidationResult,
    )

    def fake_validate(chain, protocol, token_a, token_b, fee_tier, rpc_url, gateway_client=None):
        if factory_pool_address is None:
            return PoolValidationResult(exists=False, reason=PoolValidationReason.NOT_FOUND, error="no pool")
        return PoolValidationResult(
            exists=True,
            reason=PoolValidationReason.CONFIRMED,
            pool_address=factory_pool_address.lower(),
        )

    monkeypatch.setattr(pv, "validate_v3_pool", fake_validate)
    verified = SimpleNamespace(
        binding=SimpleNamespace(binding_hash="a" * 64, to_preimage_wire=lambda: {"schemaVersion": 1}),
        operational_refs=(SimpleNamespace(to_wire=lambda: {"role": "router", "reference": "0x" + "1" * 40}),),
        evidence=SimpleNamespace(
            block_number=123,
            block_hash="0x" + "b" * 64,
            verifier_ref="tests.fake:Verifier",
            verifier_contract_version="test.v1",
            to_wire=lambda: {"blockNumber": 123, "observedFacts": []},
        ),
    )
    monkeypatch.setattr(
        "almanak.connectors.uniswap_v3.compiler.UniswapV3Compiler._verify_exact_v3_binding",
        staticmethod(lambda **_kwargs: verified),
    )


class TestCompilerPoolAddressPinning:
    @pytest.mark.parametrize(
        ("protocol", "chain", "primitive"),
        (
            ("uniswap_v3", "arbitrum", "swap"),
            ("uniswap_v3", "base", "lp"),
            ("pancakeswap_v3", "base", "swap"),
            ("pancakeswap_v3", "bsc", "lp"),
        ),
    )
    def test_staged_exact_verifier_captures_declared_lanes(self, protocol: str, chain: str, primitive: str) -> None:
        from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

        assert UniswapV3Compiler._exact_venue_verifier_applies(
            ctx=SimpleNamespace(chain=chain), protocol=protocol, primitive=primitive
        )

    @pytest.mark.parametrize(
        ("protocol", "chain", "primitive"),
        (
            ("sushiswap_v3", "arbitrum", "swap"),
            ("agni_finance", "mantle", "swap"),
            ("uniswap_v3", "mantle", "swap"),
        ),
    )
    def test_staged_exact_verifier_does_not_capture_unmigrated_lanes(
        self, protocol: str, chain: str, primitive: str
    ) -> None:
        from almanak.connectors.uniswap_v3.compiler import UniswapV3Compiler

        assert not UniswapV3Compiler._exact_venue_verifier_applies(
            ctx=SimpleNamespace(chain=chain), protocol=protocol, primitive=primitive
        )

    def test_exact_verifier_refuses_before_approval_or_calldata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.intents.compiler import _ConnectorCompilerServices
        from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
        )
        monkeypatch.setattr(
            "almanak.connectors.uniswap_v3.compiler.UniswapV3Compiler._verify_exact_v3_binding",
            staticmethod(
                lambda **kwargs: CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Exact venue refused: factory_mismatch",
                    is_safety_refusal=True,
                    intent_id=kwargs["intent_id"],
                )
            ),
        )

        def unexpected_approval(*_args, **_kwargs):
            raise AssertionError("approval must not be built before exact venue verification")

        def unexpected_calldata(*_args, **_kwargs):
            raise AssertionError("swap calldata must not be built before exact venue verification")

        monkeypatch.setattr(_ConnectorCompilerServices, "build_approve_tx", unexpected_approval)
        monkeypatch.setattr(DefaultSwapAdapter, "get_swap_calldata", unexpected_calldata)
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))

        assert result.status.value == "FAILED"
        assert result.is_safety_refusal is True
        assert "factory_mismatch" in result.error

    def test_alm_3241_b_alm_3227_pinned_pool_resolves_and_pins_its_tier(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["pool_selection_mode"] == "fixed"
        assert metadata["selected_fee_tier"] == 500
        assert metadata["fee_selection_source"] == "intent_pinned"
        assert metadata["pinned_pool"] == PINNED_POOL
        # A pinned pool must also stamp the venue binding it was resolved
        # against — without this the metadata assertions above still pass when
        # the binding is never computed (PR #3745).
        assert metadata["venue_binding_hash"] == "a" * 64

    def test_alm_3241_b_alm_3227_pinned_pool_pair_mismatch_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        other_token = "0x912CE59144191C1204E64559FE8253a0e49E6548"  # ARB
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=other_token.lower(), fee_tier=500),
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "FAILED"
        assert "does not match the swap pair" in result.error

    def test_pinned_pool_and_conflicting_fee_tier_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL, "fee_tier": 3000}))
        assert result.status.value == "FAILED"
        assert "swap_params conflict" in result.error

    def test_pinned_pool_and_matching_fee_tier_compiles(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL, "fee_tier": 500}))
        assert result.status.value == "SUCCESS"

    def test_pinned_pool_factory_mismatch_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
            factory_pool_address="0x000000000000000000000000000000000000dEaD",
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "FAILED"
        assert "different protocol or chain" in result.error

    def test_pinned_pool_not_in_factory_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
            factory_pool_address=None,
        )
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "FAILED"
        assert "is not the uniswap_v3 pool for" in result.error

    def test_unresolvable_pinned_pool_fails_with_fee_tier_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_pool_reads(monkeypatch, binding=None)
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "FAILED"
        assert "Cannot resolve pinned pool" in result.error
        assert "fee_tier" in result.error


# ===========================================================================
# Compiler: permission discovery interplay
# ===========================================================================


class TestPermissionDiscoveryPinning:
    def test_discovery_skips_pool_resolution(self) -> None:
        """Offline permission discovery must not attempt on-chain pool reads;
        a pool-only pin compiles unpinned (the manifest is tier-agnostic)."""
        compiler = _offline_compiler(permission_discovery=True)
        result = compiler.compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["pool_selection_mode"] == "auto"
        assert metadata["fee_selection_source"] != "intent_pinned"

    def test_discovery_still_pins_explicit_fee_tier(self) -> None:
        compiler = _offline_compiler(permission_discovery=True)
        result = compiler.compile(_swap_intent(swap_params={"pool": PINNED_POOL, "fee_tier": 500}))
        assert result.status.value == "SUCCESS"
        assert result.action_bundle.metadata["selected_fee_tier"] == 500


# ===========================================================================
# Foundation: read_v3_pool_binding
# ===========================================================================


class TestReadV3PoolBinding:
    """Direct decode-path tests for the on-chain pool-binding reader (the
    compiler suites above mock it, so its own ABI handling is covered here)."""

    @staticmethod
    def _word_address(address: str) -> bytes:
        return bytes(12) + bytes.fromhex(address[2:])

    @staticmethod
    def _word_uint(value: int) -> bytes:
        return value.to_bytes(32, "big")

    def _patch_eth_call(self, monkeypatch: pytest.MonkeyPatch, responses: dict) -> None:
        import almanak.connectors._strategy_base.v3_pool_validation as v3pv
        from almanak.connectors._strategy_base.v3_pool_abi import (
            V3_FEE_SELECTOR,
            V3_TOKEN0_SELECTOR,
            V3_TOKEN1_SELECTOR,
        )

        selector_names = {
            V3_TOKEN0_SELECTOR: "token0",
            V3_TOKEN1_SELECTOR: "token1",
            V3_FEE_SELECTOR: "fee",
        }

        def fake_eth_call(rpc_url, to, data, *args, **kwargs):
            return responses[selector_names[data]]

        monkeypatch.setattr(v3pv, "eth_call", fake_eth_call)

    def test_decodes_well_formed_pool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors._strategy_base.v3_pool_validation import read_v3_pool_binding

        self._patch_eth_call(
            monkeypatch,
            {
                "token0": self._word_address(WETH_ARB),
                "token1": self._word_address(USDC_ARB),
                "fee": self._word_uint(500),
            },
        )
        binding = read_v3_pool_binding(PINNED_POOL, "http://localhost:1")
        assert binding is not None
        assert binding.token0 == WETH_ARB.lower()
        assert binding.token1 == USDC_ARB.lower()
        assert binding.fee_tier == 500

    def test_no_rpc_and_no_gateway_returns_none(self) -> None:
        from almanak.connectors._strategy_base.v3_pool_validation import read_v3_pool_binding

        assert read_v3_pool_binding(PINNED_POOL, None, gateway_client=None) is None

    @pytest.mark.parametrize(
        ("broken_field", "value"),
        [
            ("token0", None),  # read failed
            ("token1", None),
            ("fee", None),
            ("fee", b"\x01"),  # short return data
            ("token0", bytes(32)),  # zero address: not a pool
            ("fee", (0).to_bytes(32, "big")),  # fee() == 0: not a real V3 pool
            ("fee", (2_000_000).to_bytes(32, "big")),  # above 100%: not a V3 fee
        ],
    )
    def test_non_pool_responses_return_none(self, monkeypatch: pytest.MonkeyPatch, broken_field: str, value) -> None:
        from almanak.connectors._strategy_base.v3_pool_validation import read_v3_pool_binding

        responses = {
            "token0": self._word_address(WETH_ARB),
            "token1": self._word_address(USDC_ARB),
            "fee": self._word_uint(500),
        }
        responses[broken_field] = value
        self._patch_eth_call(monkeypatch, responses)
        assert read_v3_pool_binding(PINNED_POOL, "http://localhost:1") is None


class TestRegistryQuotePinEnforcement:
    """The registry quote path passes the pin as a request hint; the seam must
    not trust providers to honor it (CodeRabbit review on PR #3644)."""

    def test_provider_quote_with_wrong_tier_is_discarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteResult
        from almanak.connectors._strategy_swap_quote_registry import SWAP_QUOTE_REGISTRY

        def rogue_quote(ctx, request):
            # A provider ignoring request.fee_tier and quoting another tier.
            return SwapQuoteResult(amount_out=999, source="rogue", metadata={"fee_tier": 3000})

        calls: list[int | None] = []

        def counting_rogue_quote(ctx, request):
            calls.append(request.fee_tier)
            return rogue_quote(ctx, request)

        monkeypatch.setattr(SWAP_QUOTE_REGISTRY, "quote_swap", counting_rogue_quote)
        result = _offline_compiler().compile(_swap_intent(swap_params={"fee_tier": 500}))
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["selected_fee_tier"] == 500
        assert metadata["fee_selection_source"] == "intent_pinned"
        # The seam must actually have been exercised with the pin as the hint.
        assert calls == [500]
        assert int(metadata["min_amount_out"]) > 999

    def test_exact_pool_quote_without_matching_binding_is_discarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteResult
        from almanak.connectors._strategy_swap_quote_registry import SWAP_QUOTE_REGISTRY

        _patch_pool_reads(
            monkeypatch,
            binding=V3PoolBinding(token0=WETH_ARB.lower(), token1=USDC_ARB.lower(), fee_tier=500),
        )
        seen_hashes: list[str | None] = []

        def wrong_binding_quote(ctx, request):
            seen_hashes.append(request.venue_binding_hash)
            return SwapQuoteResult(amount_out=999, source="wrong-binding", venue_binding_hash="b" * 64,
                                   metadata={"fee_tier": 500})

        monkeypatch.setattr(SWAP_QUOTE_REGISTRY, "quote_swap", wrong_binding_quote)
        result = _offline_compiler().compile(_swap_intent(swap_params={"pool": PINNED_POOL}))
        assert result.status.value == "SUCCESS"
        assert seen_hashes == ["a" * 64]
        assert int(result.action_bundle.metadata["min_amount_out"]) > 999

    def test_provider_quote_honoring_the_pin_is_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors._strategy_base.swap_quote_registry import SwapQuoteResult
        from almanak.connectors._strategy_swap_quote_registry import SWAP_QUOTE_REGISTRY

        def honoring_quote(ctx, request):
            assert request.fee_tier == 500
            return SwapQuoteResult(amount_out=10**18, source="honoring", metadata={"fee_tier": 500})

        monkeypatch.setattr(SWAP_QUOTE_REGISTRY, "quote_swap", honoring_quote)
        result = _offline_compiler().compile(_swap_intent(swap_params={"fee_tier": 500}))
        assert result.status.value == "SUCCESS"
        metadata = result.action_bundle.metadata
        assert metadata["selected_fee_tier"] == 500
        assert metadata["fee_selection_source"] == "intent_pinned"
