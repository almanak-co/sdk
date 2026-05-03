"""VIB-3893 — slot0() fallback fills ``LPOpenData.current_tick``.

The receipt parser populates ``current_tick`` from a Swap event in the
same LP_OPEN receipt (atomic swap-then-mint). When the strategy splits
the swap and the mint into separate cycles — the canonical
AccountingQuant-LP path — there is no Swap event in the LP_OPEN receipt
and the parser leaves ``current_tick=None``. ``position_events.in_range``
then stays NULL, so the dashboard's "Primary risk" tile reads `—`.

This test fences the framework-side fallback: when ``current_tick`` is
None and ``pool_address`` is populated, ``enrich_lp_open_with_slot0``
issues a ``slot0()`` call through the gateway and patches the tick into
a fresh :class:`LPOpenData`.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from almanak.framework.connectors.uniswap_v3.slot0_fallback import (
    SLOT0_SELECTOR,
    _decode_slot0_tick,
    enrich_lp_open_with_slot0,
    fetch_slot0_tick,
)
from almanak.framework.execution.extracted_data import LPOpenData

POOL = "0xC6962004f452bE9203591991D15f6b388e09E8D0"


def _slot0_blob(tick: int) -> str:
    """Encode a slot0() return blob with ``tick`` at offset 32 bytes."""
    sqrt = (1_234_567_890).to_bytes(32, "big").hex()
    tick_word = (tick & ((1 << 256) - 1)).to_bytes(32, "big").hex()
    rest = "00" * 32 * 5
    return "0x" + sqrt + tick_word + rest


# ──────────────────────────────────────────────────────────────────────────
# _decode_slot0_tick — int24 sign-extension correctness
# ──────────────────────────────────────────────────────────────────────────


def test_decode_slot0_tick_positive():
    assert _decode_slot0_tick(_slot0_blob(12_345)) == 12_345


def test_decode_slot0_tick_negative():
    """The canonical Arbitrum WETH/USDC 500 tick is around −198489."""
    assert _decode_slot0_tick(_slot0_blob(-198_489)) == -198_489


def test_decode_slot0_tick_handles_garbage():
    assert _decode_slot0_tick("") is None
    assert _decode_slot0_tick("0x") is None
    assert _decode_slot0_tick("0xdeadbeef") is None  # too short


# ──────────────────────────────────────────────────────────────────────────
# fetch_slot0_tick — gateway call wrapper
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_slot0_tick_calls_gateway_with_correct_selector():
    """The helper passes the slot0() selector verbatim and decodes the
    response. This is the contract the framework relies on."""
    gateway = MagicMock()
    gateway.eth_call.return_value = _slot0_blob(-198_489)

    tick = fetch_slot0_tick(gateway, "arbitrum", POOL)

    assert tick == -198_489
    gateway.eth_call.assert_called_once_with("arbitrum", POOL, SLOT0_SELECTOR)


def test_fetch_slot0_tick_returns_none_on_eth_call_error():
    """Network errors / RPC failures must return None — the caller
    falls back to ``current_tick=None`` (degraded but valid)."""
    gateway = MagicMock()
    gateway.eth_call.side_effect = RuntimeError("rpc error")

    assert fetch_slot0_tick(gateway, "arbitrum", POOL) is None


def test_fetch_slot0_tick_returns_none_on_missing_inputs():
    gateway = MagicMock()
    gateway.eth_call.return_value = _slot0_blob(0)

    assert fetch_slot0_tick(None, "arbitrum", POOL) is None  # no client
    assert fetch_slot0_tick(gateway, "", POOL) is None  # no chain
    assert fetch_slot0_tick(gateway, "arbitrum", "") is None  # no pool


# ──────────────────────────────────────────────────────────────────────────
# enrich_lp_open_with_slot0 — the production entry point
# ──────────────────────────────────────────────────────────────────────────


def test_enrich_fills_current_tick_when_missing():
    """Canonical scenario: pure NPM.mint, parser left current_tick=None,
    pool_address is known. The fallback patches the tick in via slot0."""
    lp_open = LPOpenData(
        position_id=5_464_283,
        tick_lower=-199_940,
        tick_upper=-197_930,
        liquidity=1_239_953_554_111,
        amount0=1_201_546_157_867_181,
        amount1=3_064_900,
        current_tick=None,
        pool_address=POOL,
    )
    gateway = MagicMock()
    gateway.eth_call.return_value = _slot0_blob(-198_489)

    enriched = enrich_lp_open_with_slot0(lp_open, gateway_client=gateway, chain="arbitrum")

    assert enriched is not lp_open  # new instance (frozen dataclass)
    assert enriched.current_tick == -198_489
    # All other fields preserved verbatim.
    assert enriched.position_id == 5_464_283
    assert enriched.tick_lower == -199_940
    assert enriched.tick_upper == -197_930
    assert enriched.pool_address == POOL


def test_enrich_passes_through_when_current_tick_already_set():
    """Receipt-parser path won — no extra RPC, no mutation."""
    lp_open = LPOpenData(
        position_id=1,
        tick_lower=-100,
        tick_upper=100,
        current_tick=42,
        pool_address=POOL,
    )
    gateway = MagicMock()

    out = enrich_lp_open_with_slot0(lp_open, gateway_client=gateway, chain="arbitrum")

    assert out is lp_open  # SAME instance, no replace
    gateway.eth_call.assert_not_called()


def test_enrich_passes_through_when_pool_address_missing():
    """Without pool_address we have nothing to slot0() against."""
    lp_open = LPOpenData(
        position_id=1,
        tick_lower=-100,
        tick_upper=100,
        current_tick=None,
        pool_address="",
    )
    gateway = MagicMock()

    out = enrich_lp_open_with_slot0(lp_open, gateway_client=gateway, chain="arbitrum")

    assert out is lp_open
    gateway.eth_call.assert_not_called()


def test_enrich_swallows_eth_call_errors():
    """Fail-open: a slot0 RPC error must NOT propagate up. The position
    event still gets written; ``in_range`` simply stays None."""
    lp_open = LPOpenData(
        position_id=1,
        tick_lower=-100,
        tick_upper=100,
        current_tick=None,
        pool_address=POOL,
    )
    gateway = MagicMock()
    gateway.eth_call.side_effect = RuntimeError("rpc 503")

    out = enrich_lp_open_with_slot0(lp_open, gateway_client=gateway, chain="arbitrum")

    assert out is lp_open  # unchanged
    assert out.current_tick is None


def test_enrich_handles_none_input_gracefully():
    """The caller's defensive ``extracted.get('lp_open_data')`` may yield
    None — the helper must not crash."""
    out = enrich_lp_open_with_slot0(None, gateway_client=MagicMock(), chain="arbitrum")
    assert out is None
