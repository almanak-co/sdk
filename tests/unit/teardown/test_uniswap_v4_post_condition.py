"""Unit tests for the Uniswap V4 teardown post-condition (VIB-5634).

A Uniswap V4 LP position is an NFT tokenId on the V4 PositionManager — a
DISTINCT contract from the V3 NonfungiblePositionManager, with a different read
ABI (getPositionLiquidity / getPoolAndPositionInfo, not positions(tokenId)). So
it is NOT in ``AbiFamily.V3_NPM`` and the V3 hook cannot verify it.

Pre-fix, no V4 post-condition existed: a just-closed V4 position was, at best,
counted closed-by-execution (UNVERIFIED) — and when its empty-return read raised
through an unguarded decoder ("invalid string length"), the teardown
mis-reported FAILED / "0 of 1 confirmed". These tests assert:

1. The ``uniswap_v4`` slug AND the registry primitive label ``lp_v4`` both
   resolve to the V4 hook (the registry key-format / label mismatch: the WARM
   registry enumeration stamps V4 positions ``protocol='lp_v4'``, not the
   connector slug).
2. Empty-return (burned / gone) → ``closed=True`` (MEASURED closure).
3. Measured residual liquidity / fees → ``closed=False`` + residual (FAILED).
4. A read fault → ``unmeasured=True`` (UNVERIFIED, never FAILED) — Empty ≠ Zero.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.connectors.uniswap_v4.teardown_post_condition import (
    uniswap_v4_post_condition as _uniswap_v4_post_condition,
)
from almanak.framework.gateway_client import V4ClosureRead
from almanak.framework.teardown import post_conditions as _pc
from almanak.framework.teardown.post_conditions import (
    get_teardown_post_condition,
    has_teardown_post_condition,
)

# A real V4 PositionManager + StateView exist for Base in the connector addresses.
BASE = "base"


def _position(
    *,
    chain: str = BASE,
    protocol: str = "uniswap_v4",
    position_id: str = "777",
    position_type: str = "LP",
    details: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        position_type=SimpleNamespace(value=position_type),
        details=details or {},
    )


def _gateway(read: V4ClosureRead | None = None, *, raises: Exception | None = None) -> MagicMock:
    gw = MagicMock()
    if raises is not None:
        gw.query_v4_position_closure.side_effect = raises
    else:
        gw.query_v4_position_closure.return_value = read
    return gw


# ---------------------------------------------------------------------------
# Registration — registry key-format / protocol-label match (strand 2)
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_connector_slug_resolves_to_v4_hook(self):
        assert has_teardown_post_condition("uniswap_v4")
        assert get_teardown_post_condition("uniswap_v4") is _uniswap_v4_post_condition

    def test_registry_primitive_label_resolves_to_v4_hook(self):
        """The WARM registry enumerates V4 as ``protocol='lp_v4'`` (the primitive
        value), NOT ``uniswap_v4`` — so a restart-derived V4 position reaches the
        verifier under ``lp_v4``. It MUST resolve to the same hook, or the closed
        V4 position gets no post-condition and is mis-classified."""
        assert has_teardown_post_condition("lp_v4")
        assert get_teardown_post_condition("lp_v4") is _uniswap_v4_post_condition


# ---------------------------------------------------------------------------
# Closure classification
# ---------------------------------------------------------------------------


class TestClosureClassification:
    def test_empty_return_gone_is_closed(self):
        """(a) empty V4 read → decoded CLOSED (the headline VIB-5634 fix)."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw)
        assert result.closed is True
        assert result.unmeasured is False

    def test_measured_residual_liquidity_is_not_closed(self):
        gw = _gateway(V4ClosureRead(closed=False, residual_liquidity=123))
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw)
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual.get("liquidity") == 123

    def test_measured_residual_fees_is_not_closed(self):
        gw = _gateway(V4ClosureRead(closed=False, residual_owed1=9))
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw)
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual.get("tokens_owed1") == 9

    def test_read_fault_is_unmeasured_not_closed(self):
        """(b) a real read fault → UNVERIFIED (unmeasured), never CLOSED / FAILED."""
        gw = _gateway(V4ClosureRead(closed=False, unmeasured=True, error="execution reverted"))
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw)
        assert result.unmeasured is True
        assert result.closed is False

    def test_gateway_raise_is_unmeasured_not_closed(self):
        gw = _gateway(raises=RuntimeError("boom"))
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw)
        assert result.unmeasured is True
        assert result.closed is False

    def test_missing_gateway_is_unmeasured(self):
        result = _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=None)
        assert result.unmeasured is True
        assert result.closed is False


# ---------------------------------------------------------------------------
# Scope / input guards
# ---------------------------------------------------------------------------


class TestScopeGuards:
    def test_non_lp_position_is_out_of_scope(self):
        """A TOKEN position (swap-only strategy) is outside this NFT-shaped check
        — closed=True (skip), mirroring the V3 hook, so it never false-FAILs."""
        gw = _gateway(V4ClosureRead(closed=False, residual_liquidity=1))
        result = _uniswap_v4_post_condition(
            _position(position_type="TOKEN"), wallet_address="0x0", gateway_client=gw
        )
        assert result.closed is True
        gw.query_v4_position_closure.assert_not_called()

    def test_non_numeric_token_id_is_unmeasured(self):
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(position_id="v4-weth-usdc", details={}), wallet_address="0x0", gateway_client=gw
        )
        assert result.unmeasured is True

    def test_bool_token_id_is_rejected_not_coerced(self):
        """int(True)==1 — a bool tokenId must be rejected (UNVERIFIED), never
        coerced into a valid-looking-but-WRONG tokenId that queries position 1."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(position_id="v4-weth-usdc", details={"token_id": True}),
            wallet_address="0x0",
            gateway_client=gw,
        )
        assert result.unmeasured is True
        assert result.closed is False
        gw.query_v4_position_closure.assert_not_called()

    def test_float_token_id_is_rejected_not_coerced(self):
        """int(1.5)==1 — a float tokenId must be rejected (UNVERIFIED), never
        truncated into a wrong tokenId."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(position_id="v4-weth-usdc", details={"token_id": 1.5}),
            wallet_address="0x0",
            gateway_client=gw,
        )
        assert result.unmeasured is True
        assert result.closed is False
        gw.query_v4_position_closure.assert_not_called()

    def test_int_token_id_still_accepted(self):
        """The guard must not reject legitimate int / numeric-str tokenIds."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(position_id="v4-weth-usdc", details={"token_id": 2758987}),
            wallet_address="0x0",
            gateway_client=gw,
        )
        assert result.closed is True
        assert gw.query_v4_position_closure.call_args.kwargs["token_id"] == 2758987

    def test_token_id_resolved_from_details(self):
        """The numeric tokenId can live in details (nft_position_id / nft_id / token_id)."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(position_id="v4-weth-usdc", details={"token_id": 4242}),
            wallet_address="0x0",
            gateway_client=gw,
        )
        assert result.closed is True
        gw.query_v4_position_closure.assert_called_once()
        assert gw.query_v4_position_closure.call_args.kwargs["token_id"] == 4242

    def test_missing_chain_is_unmeasured(self):
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(_position(chain=""), wallet_address="0x0", gateway_client=gw)
        assert result.unmeasured is True

    def test_unsupported_chain_is_unmeasured(self):
        """A chain with no V4 PositionManager / StateView → unmeasured (no live path)."""
        gw = _gateway(V4ClosureRead(closed=True))
        result = _uniswap_v4_post_condition(
            _position(chain="fantom"), wallet_address="0x0", gateway_client=gw
        )
        assert result.unmeasured is True
        gw.query_v4_position_closure.assert_not_called()

    def test_block_is_forwarded_for_pinning(self):
        """The close-tx receipt block must be forwarded so the read pins to it."""
        gw = _gateway(V4ClosureRead(closed=True))
        _uniswap_v4_post_condition(_position(), wallet_address="0x0", gateway_client=gw, block=12345678)
        assert gw.query_v4_position_closure.call_args.kwargs["block"] == 12345678


class TestPrimitiveAliasRegistrationResilience:
    """VIB-5634 (Gemini #1): a broken connector must not crash framework startup.

    `_register_lp_v4_primitive_alias` loads BOTH the primitive ref and the hook
    ref inside the guarded try; a load failure in one connector is logged loudly
    and skipped (that connector's lp_v4 alias is absent -> restart-derived V4
    falls back to UNVERIFIED, fail-safe), never propagated to abort registration
    of every strategy.
    """

    def test_broken_hook_load_is_skipped_and_logged_not_raised(self, monkeypatch, caplog):
        from almanak.framework.primitives.types import Primitive

        # A fake manifest whose primitive resolves to LP_V4 but whose hook .load()
        # raises (broken import) — the exact "one bad connector" scenario.
        broken_primitive_ref = SimpleNamespace(load=lambda: SimpleNamespace(primitive=Primitive.LP_V4))

        def _boom():
            raise ImportError("broken hook module")

        broken_manifest = SimpleNamespace(
            name="broken_v4_connector",
            primitive=broken_primitive_ref,
            teardown_post_condition=SimpleNamespace(load=_boom),
        )

        fake_registry = SimpleNamespace(with_teardown_post_condition=lambda: [broken_manifest])
        monkeypatch.setattr(_pc, "CONNECTOR_REGISTRY", fake_registry)
        # Force entry into the loop (pretend lp_v4 is not yet registered) and make
        # registration a no-op capture so we don't mutate the real global registry.
        monkeypatch.setattr(_pc, "has_teardown_post_condition", lambda slug: False)
        registered: list = []
        monkeypatch.setattr(_pc, "_register_teardown_post_condition", lambda slug, hook: registered.append((slug, hook)))

        with caplog.at_level(logging.WARNING, logger=_pc.logger.name):
            # Must NOT raise despite the broken connector.
            _pc._register_lp_v4_primitive_alias()

        assert registered == []  # the broken hook was never registered
        assert any("broken_v4_connector" in r.message for r in caplog.records), (
            "the skip must be logged loudly with the connector name (never silent)"
        )
