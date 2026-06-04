"""Shared Layer-5 accounting helpers for TraderJoe V2 LP intent tests.

Epic VIB-4591 decision #5/#8, ticket VIB-4598. Centralises the helpers the
five TraderJoe V2 LP intent-test files share so the bin-model null-contract
lives in exactly one place (gemini review on PR #2366 flagged the prior
~180-line-per-file duplication).

TraderJoe V2 is a Liquidity Book DEX: discrete price *bins* with ERC-1155
fungible LP shares — NOT a concentrated-liquidity NFT system. The result
enricher's ``EXTRACTION_SPECS_BY_PROTOCOL["traderjoe_v2"]`` adds ``bin_ids``
for LP_OPEN / LP_COLLECT_FEES; there is no ``tick_lower`` / ``tick_upper`` /
``liquidity`` extractor, and the V3 ``position_hash`` (V4 anchor) never
applies. So the directional null-contract here is the INVERSE of the
V3-style precedents for the tick fields (it mirrors the merged
Aerodrome-Classic Solidly case, PR #2364):

  * ``position_hash`` / ``tick_lower`` / ``tick_upper`` / ``liquidity`` /
    ``current_tick`` / ``in_range`` MUST be ``None`` — the bin model has no
    tick bracket, and fabricating one would be a correctness regression
    (Empty≠Zero≠None, docs/internal/blueprints/27).
  * ``pool_address`` is the canonical LBPair address (a real ``0x…`` EVM
    address). VIB-4634: the receipt parser now stamps it on
    ``LPOpenData.pool_address`` / ``LPCloseData.pool_address`` from the
    ``DepositedToBins`` / ``WithdrawnFromBins`` emitter (the LBPair contract
    itself emits those events), mirroring the Uniswap V3 Mint/Burn
    ``pool_address`` path (VIB-3893/3940). The LP accounting handler's
    resolver accept-branch (``^0x[0-9a-f]{40}$``) then books the event,
    instead of dropping it because the position-key tail
    ``tokenX/tokenY/<binStep>`` is rejected by
    ``_clean_pool_address_candidate`` as a Uniswap-V3 fee-tier descriptor.
    The V3 numeric fee-tier rejection (VIB-4274/4396) is UNTOUCHED — this
    fix routes around it by supplying a real address from chain data.
  * ``amount0`` / ``amount1`` are measured (>= 0); fee legs follow the
    directional Empty≠Zero≠None contract from the merged SushiSwap V3
    precedent (PR #2363): ``extract_lp_close_data`` sets ``fees0=None`` /
    ``fees1=None`` (VIB-4470 — TraderJoe doesn't separate fees in events),
    so the persisted fee legs must be unmeasured ``None`` or measured-zero,
    never a fabricated non-zero value.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

from almanak.framework.execution.orchestrator import ExecutionContext
from almanak.framework.execution.result_enricher import enrich_result

# Re-exported so the per-chain test files can bind the real Layer-5
# persistence assertion as ``_l5.assert_accounting_persisted`` (VIB-4634:
# the prior ``assert_accounting_persisted_or_xfail`` production-gap guard is
# gone now that TraderJoe V2 LP events persist a canonical LBPair address).
from tests.intents.conftest import assert_accounting_persisted as assert_accounting_persisted

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


def assert_bin_model_null_contract(pyld: dict, *, event_type: str) -> None:
    """Assert the TraderJoe V2 Liquidity-Book directional null-contract.

    The bin model has no NFT / tick bracket. The handler must persist
    ``None`` for every concentrated-liquidity field rather than fabricate a
    zero or a synthetic bracket (Empty≠Zero≠None, epic VIB-4591 decision
    #5).

    VIB-4634: ``pool_address`` is the canonical LBPair address — a real
    ``0x``-prefixed 40-hex EVM address stamped by the receipt parser from
    the ``DepositedToBins`` / ``WithdrawnFromBins`` emitter (the LBPair
    contract itself), mirroring the Uniswap V3 Mint/Burn ``pool_address``
    path. NOT the ``tokenX/tokenY/<binStep>`` descriptor — that descriptor
    is exactly what the LP handler's ``_clean_pool_address_candidate``
    rejects as a V3 fee tier, which is why the event was dropped pre-fix.
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
    # VIB-4634: the persisted pool_address is the canonical LBPair address
    # (chain-truth from the receipt emitter), a 20-byte EVM address — never
    # the rejected tokenX/tokenY/<binStep> descriptor.
    assert re.fullmatch(r"0x[0-9a-fA-F]{40}", pool_address), (
        "TraderJoe V2 must persist the canonical LBPair address as "
        f"pool_address (0x + 40 hex), not the binStep descriptor; got {pool_address!r}"
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
