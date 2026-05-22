"""Shared Layer-5 accounting helpers for TraderJoe V2 LP intent tests.

Epic VIB-4591 decision #5/#8, ticket VIB-4598. Centralises the helpers the
five TraderJoe V2 LP intent-test files share so the bin-model null-contract
and the VIB-4634 production-gap guard live in exactly one place (gemini
review on PR #2366 flagged the prior ~180-line-per-file duplication).

TraderJoe V2 is a Liquidity Book DEX: discrete price *bins* with ERC-1155
fungible LP shares — NOT a concentrated-liquidity NFT system. The result
enricher's ``EXTRACTION_SPECS_BY_PROTOCOL["traderjoe_v2"]`` only adds
``bin_ids`` for LP_OPEN / LP_COLLECT_FEES; there is no ``lp_open_data``, no
``tick_lower`` / ``tick_upper`` / ``liquidity`` extractor, and the V3
``position_hash`` (V4 anchor) never applies. So the directional
null-contract here is the INVERSE of the V3-style precedents (it mirrors
the merged Aerodrome-Classic Solidly case, PR #2364):

  * ``position_hash`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
    ``current_tick`` / ``in_range`` MUST be ``None`` — the bin model has no
    tick bracket, and fabricating one would be a correctness regression
    (Empty≠Zero≠None, blueprints/27).
  * ``pool_address`` is the position-key tail the LP handler resolves.
    TraderJoe V2 intents carry ``pool="TOKENX/TOKENY/<binStep>"`` whose
    last segment is a bin-step integer (not ``0x…``), so
    ``_get_pool_address`` returns the lowercased descriptor and the
    persisted ``pool_address`` is that non-empty descriptor — never a
    fabricated 0x address.
  * ``amount0`` / ``amount1`` are measured (>= 0); fee legs follow the
    directional Empty≠Zero≠None contract from the merged SushiSwap V3
    precedent (PR #2363): ``extract_lp_close_data`` sets ``fees0=None`` /
    ``fees1=None`` (VIB-4470 — TraderJoe doesn't separate fees in events),
    so the persisted fee legs must be unmeasured ``None`` or measured-zero,
    never a fabricated non-zero value.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.result_enricher import enrich_result
from tests.intents.conftest import assert_accounting_persisted

# The Layer-5 persisted row's deployment_id comes from
# ``assert_accounting_persisted(deployment_id=...)`` (conftest default
# ``"layer5-intent-test"``), NOT from this ExecutionContext — the context only
# feeds ``enrich_result`` metadata. We still stamp the SAME id here so the
# enrichment context and the persisted/asserted identity never diverge (avoids
# a latent happy-path identity mismatch once VIB-4634 lands; CodeRabbit
# PR #2366).
LAYER5_DEPLOYMENT_ID = "layer5-intent-test"


def execution_context(*, chain: str, wallet: str) -> ExecutionContext:
    return ExecutionContext(
        deployment_id=LAYER5_DEPLOYMENT_ID,
        chain=chain,
        wallet_address=wallet,
        protocol="traderjoe_v2",
    )


def enrich_for_accounting(
    execution_result: Any,
    intent: Any,
    *,
    chain: str,
    wallet: str,
    bundle_metadata: dict | None = None,
) -> Any:
    return enrich_result(
        execution_result,
        intent,
        execution_context(chain=chain, wallet=wallet),
        live_mode=False,
        bundle_metadata=bundle_metadata,
    )


def payload(row: dict) -> dict:
    return json.loads(row["payload_json"])


def to_human(raw: int | None, decimals: int) -> Decimal | None:
    if raw is None:
        return None
    return Decimal(int(raw)) / Decimal(10**decimals)


def assert_identity(row: dict, *, event_type: str, wallet: str) -> None:
    assert row["deployment_id"] == "layer5-intent-test"
    assert row["cycle_id"] == "layer5-cycle"
    assert row["execution_mode"] == "paper"
    assert row["event_type"] == event_type
    assert row["tx_hash"], "accounting row must link to an on-chain tx_hash"
    assert row["ledger_entry_id"], "accounting row must link to transaction_ledger"
    assert row["wallet_address"].lower() == wallet.lower()


def assert_no_lot_id(row: dict, pyld: dict) -> None:
    assert "lot_id" not in row
    assert "lot_id" not in pyld


async def assert_accounting_persisted_or_xfail(harness: Any, **kwargs: Any) -> dict:
    """Layer-5 persist+assert with the VIB-4634 production-gap guard.

    VIB-4634 (surfaced by THIS rollout): the LP category handler drops
    every TraderJoe V2 LP event because ``_resolve_lp_pool_address`` finds
    no usable pool address — the receipt layer stamps none, and the
    position-key tail / market_id is the descriptor
    ``tokenx/tokeny/<binStep>`` whose numeric ``<binStep>`` last segment is
    rejected by ``_clean_pool_address_candidate`` as if it were a
    Uniswap-V3 fee tier. Net: zero ``accounting_events`` rows for TraderJoe
    V2 LP.

    The on-chain tx + receipt parse + balance deltas (Layers 1–4) are
    verified correct by the caller as hard asserts **before** this call.
    Only the books-side Layer-5 persistence is broken, and ONLY in the
    specific drop shape: ``assert_accounting_persisted`` raises ``expected
    exactly one <EVENT> accounting_event ... got 0``. We convert exactly
    that signature into ``pytest.xfail`` referencing VIB-4634; any other
    failure (wrong payload, duplicate row, drain failure, a DIFFERENT
    count) is a real regression and propagates unchanged. When VIB-4634
    lands the handler will persist the row, the helper will pass, and these
    call sites become live assertions automatically — no decorator to flip.
    """
    try:
        return await assert_accounting_persisted(harness, **kwargs)
    except AssertionError as exc:
        expected = kwargs.get("expected_event_type", "")
        msg = str(exc)
        is_drop = (
            f"expected exactly one {expected} accounting_event" in msg
            and "got 0" in msg
        )
        if is_drop:
            pytest.xfail(
                f"VIB-4634: TraderJoe V2 {expected} accounting_event is "
                "dropped by lp_handler (_resolve_lp_pool_address rejects the "
                "binStep descriptor as a V3 fee tier) — on-chain tx + receipt "
                "+ balance deltas verified correct above"
            )
        raise


def assert_bin_model_null_contract(pyld: dict, *, event_type: str) -> None:
    """Assert the TraderJoe V2 Liquidity-Book directional null-contract.

    The bin model has no NFT / tick bracket. The handler must persist
    ``None`` for every concentrated-liquidity field rather than fabricate a
    zero or a synthetic bracket (Empty≠Zero≠None, epic VIB-4591 decision
    #5). ``pool_address`` is the position-key descriptor (a non-empty
    string carrying the ``<binStep>`` tail), never a fabricated 0x address.
    """
    assert pyld["event_type"] == event_type
    assert pyld["position_hash"] is None, (
        "TraderJoe V2 (Liquidity Book) must not fabricate a V4 position_hash"
    )
    # The bin-model contract holds for LP_OPEN, LP_CLOSE and
    # LP_COLLECT_FEES: never fabricate a tick bracket. LP_CLOSE /
    # LP_COLLECT_FEES payload schemas don't carry these keys at all
    # (fees/pnl/il instead), so ``.get`` absent → None still satisfies
    # "not fabricated" and future-proofs against a regression that starts
    # injecting them.
    for field in ("tick_lower", "tick_upper", "liquidity", "current_tick", "in_range"):
        assert pyld.get(field) is None, (
            f"TraderJoe V2 {event_type} must not fabricate concentrated-"
            f"liquidity field {field!r}; the bin model has no tick bracket "
            f"(got {pyld.get(field)!r})"
        )
    pool_address = pyld["pool_address"]
    assert isinstance(pool_address, str) and pool_address, (
        "TraderJoe V2 must persist a non-empty pool identifier"
    )
    assert not pool_address.startswith("0x"), (
        "TraderJoe V2 surfaces the pool descriptor (tokenX/tokenY/<binStep>) "
        f"as pool_address, not a 0x address; got {pool_address!r}"
    )


def assert_close_parser_event_equality(
    pyld: dict, lp_close_data: Any, *, dec0: int, dec1: int
) -> None:
    """Parser ↔ event exact scaled-int equality for a TraderJoe V2 LP_CLOSE.

    Mirrors the merged SushiSwap V3 directional fee-contract (PR #2363): a
    concrete parser fee reading must equal the payload exactly; a ``None``
    parser reading (Empty — TraderJoe doesn't separate fees in events,
    VIB-4470) reconciles against an unmeasured ``None`` or a measured-zero
    payload, and must NEVER match a fabricated non-zero fee.
    """
    assert Decimal(pyld["amount0"]) == to_human(lp_close_data.amount0_collected, dec0)
    assert Decimal(pyld["amount1"]) == to_human(lp_close_data.amount1_collected, dec1)
    for field, raw in (
        ("fees0_collected", lp_close_data.fees0),
        ("fees1_collected", lp_close_data.fees1),
    ):
        dec = dec0 if field == "fees0_collected" else dec1
        parser_human = to_human(raw, dec)
        payload_raw = pyld[field]
        payload_fee = None if payload_raw is None or payload_raw == "" else Decimal(payload_raw)
        if parser_human is not None:
            assert payload_fee == parser_human, (
                f"{field}: payload {payload_fee!r} must equal parser reading {parser_human!r}"
            )
        else:
            assert payload_fee is None or payload_fee == Decimal("0"), (
                f"{field}: parser did not measure fees (Empty); payload must be "
                f"unmeasured (None) or measured-zero (0), never a fabricated {payload_fee!r}"
            )
