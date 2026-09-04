"""Morpho Blue market(bytes32) liquidity read for pre-deploy capacity checks (ALM-3515).

A verified market_id (VIB-5985's ``idToMarketParams`` recompute-and-compare) can
still be unsafe to deploy into if the market has too little available borrow
liquidity for the position. ``verify_morpho_market`` layers a best-effort
``market(bytes32)`` read on top of identity verification to populate
``LendingMarketRecord.total_supply_assets`` / ``total_borrow_assets`` (raw
loan-token base units). Coverage here:

* a successful ``market()`` read populates both fields correctly;
* a short / malformed / failing ``market()`` read leaves both fields empty
  (Empty ≠ Zero — never fabricated) WITHOUT failing the identity verification
  itself, since liquidity is supplementary, not part of the identity contract;
* the two reads target the CORRECT distinct selectors
  (``idToMarketParams`` vs ``market``).
"""

from __future__ import annotations

import pytest

from almanak.connectors.morpho_blue.addresses import MORPHO_MARKETS
from almanak.connectors.morpho_blue.gateway.market_discovery import (
    _MARKET_SELECTOR,
    verify_morpho_market,
)

_SUSDE_USDC_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"


def _word_addr(addr: str) -> str:
    return addr.lower().replace("0x", "").zfill(64)


def _word_uint(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _id_to_market_params_payload(loan: str, collateral: str, oracle: str, irm: str, lltv: int) -> str:
    return "0x" + _word_addr(loan) + _word_addr(collateral) + _word_addr(oracle) + _word_addr(irm) + _word_uint(lltv)


def _market_state_payload(
    *,
    total_supply_assets: int,
    total_supply_shares: int = 0,
    total_borrow_assets: int = 0,
    total_borrow_shares: int = 0,
    last_update: int = 0,
    fee: int = 0,
) -> str:
    return "0x" + "".join(
        _word_uint(v)
        for v in (total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares, last_update, fee)
    )


def _make_routing_eth_call(*, id_to_market_params: str, market_state: str | Exception):
    """Route by selector, mirroring the gateway's real per-call dispatch."""
    calls: list[tuple[str, str]] = []

    async def _call(to: str, data: str) -> str:
        calls.append((to, data))
        selector = data[:10]
        if selector == "0x2c3c9157":
            return id_to_market_params
        if selector == _MARKET_SELECTOR:
            if isinstance(market_state, Exception):
                raise market_state
            return market_state
        raise AssertionError(f"unexpected selector {selector!r}")

    _call.calls = calls  # type: ignore[attr-defined]
    return _call


@pytest.mark.asyncio
async def test_verify_pass_populates_liquidity_fields():
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    eth_call = _make_routing_eth_call(
        id_to_market_params=_id_to_market_params_payload(
            info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
        ),
        market_state=_market_state_payload(total_supply_assets=1_000_000_000_000, total_borrow_assets=500_000_000_000),
    )
    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == "1000000000000"
    assert record.total_borrow_assets == "500000000000"
    # Two distinct reads: identity (idToMarketParams) then liquidity (market()).
    assert len(eth_call.calls) == 2
    assert eth_call.calls[0][1].startswith("0x2c3c9157")
    assert eth_call.calls[1][1].startswith(_MARKET_SELECTOR)


@pytest.mark.asyncio
async def test_verify_pass_with_failing_liquidity_read_still_verifies_identity():
    """A market()-read failure must NEVER regress identity verification — it
    is a supplementary, best-effort read layered on top of it."""
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    eth_call = _make_routing_eth_call(
        id_to_market_params=_id_to_market_params_payload(
            info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
        ),
        market_state=RuntimeError("RPC timeout"),
    )
    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == ""
    assert record.total_borrow_assets == ""


@pytest.mark.asyncio
async def test_verify_pass_with_short_liquidity_payload_leaves_fields_empty():
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    eth_call = _make_routing_eth_call(
        id_to_market_params=_id_to_market_params_payload(
            info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
        ),
        market_state="0x" + "00" * 32 * 5,  # 5 words — market() needs 6
    )
    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == ""
    assert record.total_borrow_assets == ""


@pytest.mark.asyncio
async def test_verify_pass_with_all_zero_liquidity_reports_measured_zero():
    """Empty ≠ Zero the other direction too: a well-formed all-zero market()
    response (a freshly-created, never-supplied-to market) is a MEASURED zero,
    not an unmeasured absence."""
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    eth_call = _make_routing_eth_call(
        id_to_market_params=_id_to_market_params_payload(
            info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
        ),
        market_state=_market_state_payload(total_supply_assets=0, total_borrow_assets=0),
    )
    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.total_supply_assets == "0"
    assert record.total_borrow_assets == "0"


@pytest.mark.asyncio
async def test_verify_pass_with_slow_liquidity_read_times_out_and_still_verifies_identity(monkeypatch):
    """The liquidity read shares its eth_call transport with the identity read
    under one client-side gRPC deadline (GetLendingMarket). An unbounded
    second call could push total server time past that deadline and fail a
    strategy boot gate even though identity verification itself succeeded.
    The liquidity read must therefore time out on its OWN short budget,
    independent of however slow the transport is.

    Negative control: the fake eth_call sleeps far longer than either budget
    below, so this only passes when the inner `asyncio.wait_for` in
    `_read_market_liquidity` actually cancels it -- reverting that fix makes
    the outer bound fire first and this test fail, rather than the two
    outcomes being indistinguishable (a bare `except Exception` around an
    unbounded call would eventually produce the same ("", "") result, just
    slowly -- proven by simulated revert before landing this test)."""
    import asyncio

    from almanak.connectors.morpho_blue.gateway import market_discovery as md

    monkeypatch.setattr(md, "_LIQUIDITY_READ_TIMEOUT_SECONDS", 0.05)

    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    id_payload = _id_to_market_params_payload(
        info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
    )

    async def eth_call(to: str, data: str) -> str:
        if data[:10] == "0x2c3c9157":
            return id_payload
        await asyncio.sleep(3600)  # would hang forever without the inner bound

    # Comfortably longer than the patched 0.05s inner budget, comfortably
    # shorter than the fake call's 3600s sleep -- fires only if the inner
    # bound is missing or broken.
    record = await asyncio.wait_for(
        verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call), timeout=2.0
    )
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == ""
    assert record.total_borrow_assets == ""


@pytest.mark.asyncio
async def test_verify_pass_with_none_liquidity_result_leaves_fields_empty():
    """The injected eth_call is typed to return str; a non-conforming caller
    returning None (or any non-str) must not crash this best-effort read --
    it has to fail the same way a short/malformed payload does."""
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]

    async def eth_call(to: str, data: str):
        if data[:10] == "0x2c3c9157":
            return _id_to_market_params_payload(
                info["loan_token_address"],
                info["collateral_token_address"],
                info["oracle"],
                info["irm"],
                int(info["lltv"]),
            )
        return None

    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == ""
    assert record.total_borrow_assets == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("poisoned_word_index", [0, 2], ids=["supply_assets_word", "borrow_assets_word"])
async def test_verify_pass_with_nonzero_upper_bits_leaves_fields_empty(poisoned_word_index: int):
    """Each market() field is a uint128 packed into a 32-byte word -- a
    correctly-encoded response always zero-pads the upper 16 bytes. A nonzero
    upper half (a malformed/truncated response, or a struct layout this
    reader doesn't understand) must never be silently truncated into a
    wrong, possibly enormous, liquidity number. Parametrized over both
    checked words (0 = total_supply_assets, 2 = total_borrow_assets) --
    mutation-tested: deleting either half of the guard's `or` leaves the
    OTHER word's poisoning case passing silently."""
    info = MORPHO_MARKETS["ethereum"][_SUSDE_USDC_ID]
    # Poison the upper 16 bytes of the targeted word with 0x01, leaving a
    # plausible-looking value (1_000_000_000_000) in its lower half.
    words = [_word_uint(0) for _ in range(6)]
    words[poisoned_word_index] = "0" * 31 + "1" + _word_uint(1_000_000_000_000)[32:]
    market_state = "0x" + "".join(words)
    eth_call = _make_routing_eth_call(
        id_to_market_params=_id_to_market_params_payload(
            info["loan_token_address"], info["collateral_token_address"], info["oracle"], info["irm"], int(info["lltv"])
        ),
        market_state=market_state,
    )
    record = await verify_morpho_market(chain="ethereum", market_id=_SUSDE_USDC_ID, eth_call=eth_call)
    assert record is not None
    assert record.verified is True
    assert record.total_supply_assets == ""
    assert record.total_borrow_assets == ""
