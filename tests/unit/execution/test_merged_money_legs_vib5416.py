"""VIB-5416 — a multi-tx intent's money legs are extracted from the UNION of its tx logs.

A Lido wrapped STAKE submits ETH→stETH in tx 1 and wraps stETH→wstETH in a later
tx. Neither receipt alone carries BOTH the ETH input leg and the wstETH output
leg, so the per-tx first-OK extraction mislabelled the ledger ``token_out`` as the
intermediate ``stETH`` — which then stranded the teardown swap-back for the
``wstETH`` the wallet actually holds (the VIB-5416 bug). ``primitive_money_legs``
is now extracted from a merged-logs receipt so the parser declares ETH→wstETH.
"""

from __future__ import annotations

from almanak.connectors.lido.receipt_parser import EVENT_TOPICS, LidoReceiptParser
from almanak.framework.execution.result_enricher import ResultEnricher

_STETH = "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84"
_WSTETH = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
_ZERO_TOPIC = "0x" + "0" * 24 + "0" * 40
_STAKER = "0x" + "0" * 24 + "1" * 40


def _u256(x: int) -> str:
    return "0x" + format(x, "064x")


def _submitted_data(wei: int) -> str:
    return "0x" + format(wei, "064x") + ("0" * 64)  # amount + referral


def _stake_tx(stake_wei: int) -> dict:
    return {
        "transactionHash": "0xtx1",
        "from_address": _STAKER,
        "logs": [
            {"address": _STETH, "topics": [EVENT_TOPICS["Submitted"], _STAKER], "data": _submitted_data(stake_wei)},
            {"address": _STETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO_TOPIC, _STAKER], "data": _u256(stake_wei)},
        ],
    }


def _wrap_tx(wst_wei: int) -> dict:
    return {
        "transactionHash": "0xtx3",
        "logs": [
            {"address": _WSTETH, "topics": [EVENT_TOPICS["Transfer"], _ZERO_TOPIC, _STAKER], "data": _u256(wst_wei)},
        ],
    }


def test_per_tx_stake_receipt_yields_intermediate_steth():
    # Documents the pre-fix behaviour: tx 1 alone declares ETH -> stETH.
    parser = LidoReceiptParser(chain="ethereum")
    legs = parser.extract_primitive_money_legs(_stake_tx(50000000000000000))
    tokens = {leg.token for leg in legs.legs}
    assert tokens == {"ETH", "stETH"}


def test_merged_receipt_yields_final_wsteth():
    parser = LidoReceiptParser(chain="ethereum")
    merged = ResultEnricher._merge_receipt_logs([_stake_tx(50000000000000000), _wrap_tx(40391203899605260)])
    legs = parser.extract_primitive_money_legs(merged)
    by_role = {leg.role.value: leg for leg in legs.legs}
    assert by_role["input"].token == "ETH"
    assert by_role["output"].token == "wstETH"
    assert by_role["output"].amount.is_measured
    assert str(by_role["output"].amount.value) == "0.04039120389960526"


def test_merge_unions_logs_and_preserves_first_context():
    merged = ResultEnricher._merge_receipt_logs([_stake_tx(1), _wrap_tx(2)])
    assert len(merged["logs"]) == 3  # 2 from stake tx + 1 from wrap tx
    assert merged["from_address"] == _STAKER  # first receipt's scalar context kept
    assert "logs" in merged


def test_merge_single_receipt_is_noop_shape():
    only = _stake_tx(5)
    merged = ResultEnricher._merge_receipt_logs([only])
    assert merged["logs"] == only["logs"]
    assert merged["from_address"] == _STAKER


def test_merged_receipt_has_set_unique_synthetic_hash():
    # The merged receipt must NOT inherit the first tx's hash, or the enricher's
    # parse cache (keyed on transactionHash) returns the stale per-tx parse.
    merged = ResultEnricher._merge_receipt_logs([_stake_tx(1), _wrap_tx(2)])
    assert merged["transactionHash"].startswith("merged:")
    assert merged["transactionHash"] == merged["tx_hash"]
    assert merged["transactionHash"] not in ("0xtx1", "0xtx3")
    # Different constituent tx sets → different keys (no cross-iteration collision).
    other = ResultEnricher._merge_receipt_logs(
        [{"transactionHash": "0xAA", "logs": []}, {"transactionHash": "0xBB", "logs": []}]
    )
    assert other["transactionHash"] != merged["transactionHash"]


def test_merged_extraction_bypasses_stale_parse_cache():
    # Reproduces the exact failure the real-fork E2E caught: with the parse cache
    # installed and tx 1 parsed FIRST (caching a stETH result), extracting from the
    # merged receipt must still yield wstETH — i.e. the merged call must not hit
    # tx 1's cache entry.
    parser = LidoReceiptParser(chain="ethereum")
    ResultEnricher._install_parse_cache(parser)
    try:
        # Prime the cache with tx 1's per-tx parse (stakes=1, wraps=0 → stETH).
        primed = parser.extract_primitive_money_legs(_stake_tx(50000000000000000))
        assert {leg.token for leg in primed.legs} == {"ETH", "stETH"}
        # Now the merged receipt — must re-parse (distinct synthetic key) → wstETH.
        merged = ResultEnricher._merge_receipt_logs(
            [_stake_tx(50000000000000000), _wrap_tx(40391203899605260)]
        )
        legs = parser.extract_primitive_money_legs(merged)
        assert {leg.role.value: leg.token for leg in legs.legs}["output"] == "wstETH"
    finally:
        ResultEnricher._remove_parse_cache(parser)
