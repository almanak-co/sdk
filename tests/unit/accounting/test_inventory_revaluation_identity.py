"""Identity regressions for persisted ambient and open-lot marks."""

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.accounting.inventory_revaluation import compute_inventory_revaluation

WALLET = "0x11709d8d09e074e28819d864dec4b35fa752f759"
OTHER_WALLET = "0x0000000000000000000000000000000000000002"
DEPLOYMENT = "deployment:scope"


def _persisted_inputs():
    fixture = Path(__file__).parents[2] / "fixtures/accounting/bsc_held_inventory.json"
    data = json.loads(fixture.read_text())
    data.pop("provenance")
    return data


def test_historical_snapshot_without_observed_wallet_scope_stays_unmeasured():
    result = compute_inventory_revaluation(**_persisted_inputs())
    assert result.total_usd is None
    assert result.confidence == "unmeasured_identity"


def test_scope_enriched_numeric_control_joins_symbol_lot_without_initial_mark():
    data = _persisted_inputs()
    # Explicitly reconstructed scope is a unit-test input, not historical acceptance evidence.
    for key in ("snapshot_initial", "snapshot_final"):
        rows = json.loads(data[key]["wallet_balances_json"])
        for row in rows:
            row.update(chain="bsc", wallet_address=WALLET)
        data[key]["wallet_balances_json"] = json.dumps(rows)
    before = deepcopy(data)
    result = compute_inventory_revaluation(**data)

    native = Decimal("99.999745475") * (Decimal("711.18") - Decimal("711.16"))
    acquired_lot = Decimal("0.002951026223422255") * Decimal("331.84000000000003") - Decimal("0.979283297111558210475")
    assert result.confidence == "measured"
    assert result.total_usd == native + acquired_lot
    assert data == before


def _row(token, balance, price, *, chain="bsc", wallet=WALLET, symbol=None):
    return {
        "symbol": symbol or token,
        "address": token if token.startswith("0x") else "",
        "balance": str(balance),
        "price_usd": str(price),
        "chain": chain,
        "wallet_address": wallet,
    }


def _snapshot(rows):
    return {"deployment_id": DEPLOYMENT, "wallet_balances_json": json.dumps(rows), "positions_json": "[]"}


def _value(initial, final, events=None):
    return compute_inventory_revaluation(
        snapshot_initial=_snapshot(initial),
        snapshot_final=_snapshot(final),
        accounting_events=events or [],
        deployment_id=DEPLOYMENT,
    )


@pytest.mark.parametrize("chains", [("bsc", "arbitrum"), ("bsc", "bsc")])
def test_equal_symbols_keep_independent_chain_and_contract_prices(chains):
    tokens = ("0x0000000000000000000000000000000000000011", "0x0000000000000000000000000000000000000012")
    initial = [
        _row(t, q, p, chain=c, symbol="SAME") for t, q, p, c in zip(tokens, (2, 3), (10, 100), chains, strict=True)
    ]
    final = [_row(t, q, p, chain=c, symbol="SAME") for t, q, p, c in zip(tokens, (2, 3), (12, 90), chains, strict=True)]
    assert _value(initial, final).total_usd == Decimal("-26")


def _swap(wallet, token_out, *, token_in="USDT", amount_in="1", amount_out="3", cost="30", chain="bsc"):
    return {
        "deployment_id": DEPLOYMENT,
        "chain": chain,
        "wallet_address": wallet,
        "event_type": "SWAP",
        "timestamp": "2026-09-10T00:00:00+00:00",
        "payload_json": json.dumps(
            {
                "swap_position_key": f"swap:{chain}:{wallet}",
                "token_in": token_in,
                "token_out": token_out,
                "amount_in": amount_in,
                "amount_out": amount_out,
                "amount_out_usd": cost,
            }
        ),
    }


def test_other_wallets_stale_lot_cannot_claim_ambient_inventory():
    token = "0x0000000000000000000000000000000000000011"
    result = _value(
        [_row(token, 3, 12, wallet=OTHER_WALLET)], [_row(token, 3, 14, wallet=OTHER_WALLET)], [_swap(WALLET, token)]
    )
    assert result.total_usd == Decimal("6")


@pytest.mark.parametrize("duplicate", [True, False])
def test_missing_or_duplicate_scope_is_not_measured(duplicate):
    row = _row("BNB", 1, 10)
    rows = [row, deepcopy(row)] if duplicate else [row]
    if not duplicate:
        row.pop("wallet_address")
    result = _value(rows, rows)
    assert result.total_usd is None
    assert result.confidence == "unmeasured_identity"


def test_native_and_wrapped_native_keep_separate_marks():
    assert _value(
        [_row("BNB", 2, 10), _row("WBNB", 3, 100)], [_row("BNB", 2, 12), _row("WBNB", 3, 90)]
    ).total_usd == Decimal("-26")


def test_case_distinct_solana_mints_survive_fifo_replay():
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    wallet = "So11111111111111111111111111111111111111112"
    other = mint[:-1] + "V"
    events = [
        _swap(wallet, mint, chain="solana", amount_out="2", cost="20"),
        _swap(wallet, other, chain="solana", amount_out="3", cost="300"),
    ]
    initial = [_row(mint, 0, 10, chain="solana", wallet=wallet), _row(other, 0, 100, chain="solana", wallet=wallet)]
    final = [_row(mint, 2, 12, chain="solana", wallet=wallet), _row(other, 3, 90, chain="solana", wallet=wallet)]
    assert _value(initial, final, events).total_usd == Decimal("-26")


def test_writer_lowercased_solana_swap_key_is_the_same_wallet():
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    wallet = "So11111111111111111111111111111111111111112"
    event = _swap(wallet, mint, chain="solana", amount_out="2", cost="20")
    payload = json.loads(event["payload_json"])
    payload["swap_position_key"] = f"swap:solana:{wallet.lower()}"
    event["payload_json"] = json.dumps(payload)
    result = _value(
        [_row(mint, 0, 10, chain="solana", wallet=wallet)],
        [_row(mint, 2, 12, chain="solana", wallet=wallet)],
        [event],
    )
    assert result.confidence == "measured"
    assert result.total_usd == Decimal("4")


@pytest.mark.parametrize("event_type", ["SUPPLY", "LP_OPEN"])
@pytest.mark.parametrize(
    "chain,wallet,token",
    [
        ("bsc", "0x11709D8D09e074E28819D864dEc4B35FA752F759", "0x0000000000000000000000000000000000000011"),
        ("solana", "So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
    ],
)
def test_non_swap_disposal_uses_the_same_scoped_fifo_key(event_type, chain, wallet, token):
    first = _swap(wallet, token, amount_out="3", cost="30", chain=chain)
    if chain == "bsc":
        first["wallet_address"] = wallet.lower()
    payload = (
        {"asset": token, "amount_token": "3", "amount_usd": "30"}
        if event_type == "SUPPLY"
        else {"token0": token, "amount0": "3", "token1": "USDT", "amount1": "0", "coin_symbols": [token, ""]}
    )
    disposal = {
        "deployment_id": DEPLOYMENT,
        "chain": chain,
        "wallet_address": wallet,
        "event_type": event_type,
        "position_key": "protocol:position",
        "timestamp": "2026-09-10T00:01:00+00:00",
        "payload_json": json.dumps(payload),
    }
    second = _swap(wallet, token, amount_out="2", cost="40", chain=chain)
    second["timestamp"] = "2026-09-10T00:02:00+00:00"
    result = _value(
        [_row(token, 0, 10, chain=chain, wallet=wallet)],
        [_row(token, 2, 30, chain=chain, wallet=wallet)],
        [first, disposal, second],
    )
    assert result.total_usd == Decimal("20")


def test_conflicting_event_payloads_cannot_select_a_different_replay():
    event = _swap(WALLET, "BNB")
    event["payload"] = dict(json.loads(event["payload_json"]), amount_out="300")
    result = _value([_row("BNB", 0, 10)], [_row("BNB", 3, 12)], [event])
    assert result.confidence == "unmeasured_identity"
    assert result.total_usd is None


def test_conflicting_event_wallet_scope_is_refused():
    event = _swap(WALLET, "BNB")
    event["wallet_address"] = OTHER_WALLET
    result = _value([_row("BNB", 0, 10)], [_row("BNB", 3, 12)], [event])
    assert result.confidence == "unmeasured_identity"


def _pt_case(*, symbol="PT-ASSET-31DEC2026", chain="bsc", wallet=WALLET, explicit_wallet=None):
    details = {"source": "pt_inventory_lots", "pt_symbol": symbol, "quantity": "1"}
    if explicit_wallet is not None:
        details["wallet_address"] = explicit_wallet
    position = {"chain": chain, "position_type": "TOKEN", "value_usd": "12", "cost_basis_usd": "10", "details": details}
    snapshot = _snapshot([])
    snapshot["positions_json"] = json.dumps(
        {"positions": [position], "metadata": {"wallet_scope": {"schema_version": 1, "chain_wallets": {chain: wallet}}}}
    )
    event = {
        "deployment_id": DEPLOYMENT,
        "chain": chain,
        "wallet_address": wallet,
        "event_type": "PT_BUY",
        "position_key": "pendle:market",
        "timestamp": "2026-09-10T00:00:00+00:00",
        "payload_json": json.dumps({"pt_token": symbol, "pt_amount": "1", "sy_amount": "10", "sy_price": "1"}),
    }
    return {
        "snapshot_initial": deepcopy(snapshot),
        "snapshot_final": snapshot,
        "accounting_events": [event],
        "deployment_id": DEPLOYMENT,
    }


def test_principal_token_uses_its_explicit_chain_endpoint_wallet_context():
    assert compute_inventory_revaluation(**_pt_case()).total_usd == Decimal("2")


def test_principal_token_cannot_borrow_a_foreign_wallet_context():
    result = compute_inventory_revaluation(**_pt_case(explicit_wallet=OTHER_WALLET))
    assert result.confidence == "unmeasured_identity"


def test_principal_token_without_any_wallet_binding_is_unmeasured():
    case = _pt_case()
    for key in ("snapshot_initial", "snapshot_final"):
        envelope = json.loads(case[key]["positions_json"])
        envelope.pop("metadata")
        case[key]["positions_json"] = json.dumps(envelope)
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_legacy_principal_token_cannot_merge_different_market_keys():
    case = _pt_case()
    second = dict(case["accounting_events"][0], position_key="pendle:other_market")
    case["accounting_events"].append(second)
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_raw_principal_token_mint_keeps_case_through_replay():
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    wallet = "So11111111111111111111111111111111111111112"
    case = _pt_case(symbol=mint, chain="solana", wallet=wallet)
    second = _pt_case(symbol=mint[:-1] + "V", chain="solana", wallet=wallet)
    for key in ("snapshot_initial", "snapshot_final"):
        envelope = json.loads(case[key]["positions_json"])
        envelope["positions"].extend(json.loads(second[key]["positions_json"])["positions"])
        case[key]["positions_json"] = json.dumps(envelope)
    case["accounting_events"].extend(second["accounting_events"])
    assert compute_inventory_revaluation(**case).total_usd == Decimal("4")


@pytest.mark.parametrize(
    "proof", [True, {"schema_version": True, "chain_wallets": {}}, {"schema_version": 2, "chain_wallets": {}}]
)
def test_malformed_endpoint_scope_proof_is_refused(proof):
    case = _pt_case()
    envelope = json.loads(case["snapshot_final"]["positions_json"])
    envelope["metadata"]["wallet_scope"] = proof
    case["snapshot_final"]["positions_json"] = json.dumps(envelope)
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_bundled_symbol_collision_cannot_choose_a_contract(monkeypatch):
    from almanak.core.asset_identity import AssetIdentity, AssetNamespace
    from almanak.framework.accounting import inventory_scope

    identities = frozenset(
        AssetIdentity("bsc", AssetNamespace.ERC20, address)
        for address in ("0x0000000000000000000000000000000000000011", "0x0000000000000000000000000000000000000012")
    )
    monkeypatch.setattr(inventory_scope, "_bundled_aliases", lambda: {("bsc", "COLLIDE"): identities})
    assert _value([_row("COLLIDE", 1, 10)], [_row("COLLIDE", 1, 12)]).confidence == "unmeasured_identity"


def test_measured_zero_price_is_not_missing():
    result = _value([_row("BNB", 1, 12)], [_row("BNB", 1, 0)])
    assert result.confidence == "measured"
    assert result.total_usd == Decimal("-12")


@pytest.mark.parametrize("missing", ["snapshot_initial", "snapshot_final"])
def test_missing_endpoint_cannot_certify_open_lot_inventory(missing):
    case = _pt_case()
    case[missing] = None
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_solana_caip_identity_matches_raw_mint_without_case_loss():
    from almanak.core.asset_identity import AssetIdentity, AssetNamespace

    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    wallet = "So11111111111111111111111111111111111111112"
    caip = AssetIdentity("solana", AssetNamespace.TOKEN, mint).caip19
    assert _value(
        [_row(caip, 2, 10, chain="solana", wallet=wallet)], [_row(mint, 2, 12, chain="solana", wallet=wallet)]
    ).total_usd == Decimal("4")
    assert _value([_row(caip, 2, 10)], [_row(caip, 2, 12)]).confidence == "unmeasured_identity"


def test_same_physical_contract_cannot_be_valued_in_pt_and_fungible_lanes():
    token = "0x0000000000000000000000000000000000000011"
    case = _pt_case(symbol=token)
    for key in ("snapshot_initial", "snapshot_final"):
        case[key]["wallet_balances_json"] = json.dumps([_row(token, 1, 12)])
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_open_lot_cannot_infer_zero_inventory_before_wallet_scope_switch():
    token = "0x0000000000000000000000000000000000000011"
    result = _value([_row("BNB", 1, 10, wallet=OTHER_WALLET)], [_row(token, 3, 12)], [_swap(WALLET, token)])
    assert result.confidence == "unmeasured_identity"


def test_principal_position_conflicting_asset_claims_are_not_first_truthy_wins():
    case = _pt_case()
    endpoint = json.loads(case["snapshot_final"]["positions_json"])
    endpoint["positions"][0]["details"]["asset"] = "PT-OTHER-31DEC2026"
    case["snapshot_final"]["positions_json"] = json.dumps(endpoint)
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"


def test_snapshot_chain_cannot_contradict_observed_endpoint_scope():
    case = _pt_case()
    case["snapshot_final"]["chain"] = "ethereum"
    assert compute_inventory_revaluation(**case).confidence == "unmeasured_identity"
