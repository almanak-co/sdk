"""Shared async, orchestrator-aware setup helpers for Uniswap-V3-style LP tests.

The no-liquidity edge-case tests (``test_lp_close_position_no_liquidity_*``)
need to manipulate a position's state OUTSIDE the intent system — call
``decreaseLiquidity`` and/or ``collect`` directly on the
``NonfungiblePositionManager`` to put the position into the targeted state
before exercising ``LPCloseIntent``.

Pre-pivot, those helpers signed raw EOA transactions. Under default-on
Zodiac, the position is owned by the per-test Safe (because ``funded_wallet``
returns the Safe address), so an EOA-signed call to ``decreaseLiquidity``
reverts — only the position owner can decrease.

These helpers route the setup tx through whatever orchestrator the test
already holds. ``ExecutionOrchestrator`` signs with the test EOA (preserving
the legacy EOA-mode behaviour for ``no_zodiac``-marked tests).
``ZodiacOrchestrator`` wraps the call into ``execTransactionWithRole``
signed by the member EOA, so the Safe (the position owner) is the actual
caller — and the late-binding manifest applied at execute time covers
``decreaseLiquidity``/``collect``/``burn`` because we seed an ``LPCloseIntent``
into the recorder before the setup runs.

Used by every Uniswap V3 / SushiSwap V3 LP test under
``tests/intents/<chain>/`` to keep that family parallel and prevent local
copies of these helpers drifting out of sync.
"""

from __future__ import annotations

import time

from web3 import Web3

from almanak.connectors.uniswap_v3.adapter import UniswapV3LPAdapter
from almanak.framework.intents import LPCloseIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

# Sentinel passed to ``collect`` for unbounded amount0/amount1 maxes.
MAX_UINT128 = (1 << 128) - 1


def query_position_liquidity(web3: Web3, position_manager: str, token_id: int) -> int:
    """Return the on-chain liquidity for ``token_id`` on the position manager.

    Reads ``positions(uint256)`` directly via ``eth_call`` — no signing, no
    Zodiac, no orchestrator. Pure read.

    Fails fast on a short / malformed payload: ``int.from_bytes(b"")`` would
    return ``0``, which the no-liquidity helpers treat as "already empty"
    and silently skip the setup tx. A wrong ``position_manager`` address or
    an ABI-shape regression on the position manager would slip through as a
    false-positive "no liquidity" result and let the test pass on a
    fictional state. The explicit length assertion converts that into a
    loud setup error.
    """
    selector = "0x99fbab88"  # positions(uint256)
    data = selector + hex(token_id)[2:].zfill(64)
    result = web3.eth.call(
        {"to": Web3.to_checksum_address(position_manager), "data": data}
    )
    # positions() returns nonce(0), operator(1), token0(2), token1(3),
    # fee(4), tickLower(5), tickUpper(6), liquidity(7), …
    liquidity_offset = 7 * 32
    expected_min_len = liquidity_offset + 32
    if len(result) < expected_min_len:
        raise AssertionError(
            f"positions() returned {len(result)} bytes from {position_manager} "
            f"for token_id={token_id}; expected at least {expected_min_len} "
            f"bytes. Likely a wrong position manager address or an ABI-shape "
            f"regression — bailing out so the test doesn't silently treat the "
            f"position as having zero liquidity."
        )
    return int.from_bytes(
        result[liquidity_offset : liquidity_offset + 32], byteorder="big"
    )


def _seed_lp_close_intent_if_zodiac(
    orchestrator,
    chain: str,
    protocol: str,
    token_id: int,
) -> None:
    """If ``orchestrator`` is a ``ZodiacOrchestrator``, append an
    ``LPCloseIntent`` to its recorded-intents list so the next ``execute()``
    derives a manifest that covers ``decreaseLiquidity`` / ``collect`` /
    ``burn``.

    No-op when running under ``ExecutionOrchestrator`` (EOA mode) — the
    legacy raw-EOA path doesn't need a manifest.

    Idempotent: skips if an ``LPCloseIntent`` for this position is already
    in the recorder, so a test that runs setup twice doesn't bloat the list.
    """
    # Avoid a hard import cycle — ZodiacOrchestrator lives in the harness
    # module and the harness imports the conftest. Lazy-imported.
    from tests.intents._permission_onchain_harness import ZodiacOrchestrator

    if not isinstance(orchestrator, ZodiacOrchestrator):
        return
    if orchestrator.recorded_intents is None:
        return
    for existing in orchestrator.recorded_intents:
        if (
            isinstance(existing, LPCloseIntent)
            and getattr(existing, "position_id", None) == str(token_id)
        ):
            return
    orchestrator.recorded_intents.append(
        LPCloseIntent(
            position_id=str(token_id),
            protocol=protocol,
            chain=chain,
        )
    )


async def _send_via_orchestrator(
    orchestrator,
    to: str,
    data: bytes,
    value: int = 0,
    intent_type: str = "LP_CLOSE",
) -> None:
    """Build a single-tx ``ActionBundle`` and execute it through the orchestrator.

    ``ExecutionOrchestrator`` signs with the test EOA and submits.
    ``ZodiacOrchestrator`` wraps via ``execTransactionWithRole`` and submits
    via the member EOA — and applies any pending manifest targets first
    (driven by intents the recorder has captured).
    """
    bundle = ActionBundle(
        intent_type=intent_type,
        transactions=[
            {
                "to": Web3.to_checksum_address(to),
                "data": "0x" + data.hex(),
                "value": value,
                "gas": 1_000_000,
            }
        ],
    )
    result = await orchestrator.execute(bundle)
    err = getattr(result, "error", None)
    assert getattr(result, "success", False), (
        f"Setup tx via orchestrator failed (intent_type={intent_type}, to={to}): {err}"
    )


async def decrease_all_liquidity(
    web3: Web3,
    orchestrator,
    *,
    chain: str,
    protocol: str,
    position_manager: str,
    token_id: int,
) -> None:
    """Decrease all liquidity from a position, routing via the orchestrator.

    No-op when the position already has zero liquidity (e.g., the test setup
    has already run, or close-only test on a fresh-but-empty position).
    """
    liquidity = query_position_liquidity(web3, position_manager, token_id)
    if liquidity == 0:
        return
    _seed_lp_close_intent_if_zodiac(orchestrator, chain, protocol, token_id)
    adapter = UniswapV3LPAdapter(chain=chain)
    deadline = int(time.time()) + 86400
    calldata = adapter.get_decrease_liquidity_calldata(
        token_id=token_id,
        liquidity=liquidity,
        amount0_min=0,
        amount1_min=0,
        deadline=deadline,
    )
    await _send_via_orchestrator(orchestrator, position_manager, calldata)


async def collect_all_tokens(
    web3: Web3,
    orchestrator,
    *,
    chain: str,
    protocol: str,
    position_manager: str,
    token_id: int,
    recipient: str,
) -> None:
    """Collect all owed tokens from a position, routing via the orchestrator."""
    _seed_lp_close_intent_if_zodiac(orchestrator, chain, protocol, token_id)
    adapter = UniswapV3LPAdapter(chain=chain)
    calldata = adapter.get_collect_calldata(
        token_id=token_id,
        recipient=recipient,
        amount0_max=MAX_UINT128,
        amount1_max=MAX_UINT128,
    )
    await _send_via_orchestrator(orchestrator, position_manager, calldata)
