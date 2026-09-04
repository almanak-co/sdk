from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers.lp_handler import _apply_lp_wallet_basis_hooks, handle_lp

_TIMESTAMP = datetime(2026, 9, 3, tzinfo=UTC)
_WALLET_KEY = "swap:arbitrum:0xwallet"


def _apply(store: FIFOBasisStore | None, **overrides: Any) -> None:
    arguments: dict[str, Any] = {
        "basis_store": store,
        "intent_type_str": "LP_OPEN",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "chain": " Arbitrum ",
        "wallet_address": " 0xWallet ",
        "token0": "USDC",
        "token1": "WETH",
        "amount0": Decimal("1"),
        "amount1": Decimal("2"),
        "fees0": None,
        "fees1": None,
        "cost_basis_usd": None,
        "fees_total_usd": None,
        "price_oracle": {},
        "timestamp": _TIMESTAMP,
        "tx_hash": "0xtx",
        "ledger_entry_id": "ledger-1",
    }
    arguments.update(overrides)
    assert _apply_lp_wallet_basis_hooks(**arguments) is None


@pytest.mark.parametrize(
    ("store_present", "overrides"),
    [
        pytest.param(False, {}, id="no-basis-store"),
        pytest.param(True, {"chain": ""}, id="empty-chain"),
        pytest.param(True, {"chain": "   "}, id="blank-chain"),
        pytest.param(True, {"wallet_address": ""}, id="empty-wallet"),
        pytest.param(True, {"wallet_address": "   "}, id="blank-wallet"),
        pytest.param(True, {"intent_type_str": "LP_SNAPSHOT"}, id="unsupported-event"),
        pytest.param(
            True,
            {
                "intent_type_str": "LP_CLOSE",
                "amount0": None,
                "amount1": Decimal("0"),
                "fees0": None,
                "fees1": Decimal("0"),
            },
            id="close-with-no-positive-return",
        ),
        pytest.param(
            True,
            {
                "intent_type_str": "LP_COLLECT_FEES",
                "token0": "",
                "token1": "",
                "fees0": Decimal("1"),
                "fees1": Decimal("2"),
            },
            id="collect-with-no-token-identity",
        ),
    ],
)
def test_skip_branches_do_not_touch_fifo(store_present: bool, overrides: dict[str, Any]) -> None:
    store = MagicMock(spec=FIFOBasisStore) if store_present else None

    _apply(store, **overrides)

    if store is not None:
        assert store.method_calls == []


@pytest.mark.parametrize(
    ("token0", "amount0", "token1", "amount1", "expected"),
    [
        pytest.param(
            "USDC",
            Decimal("1"),
            "WETH",
            Decimal("2"),
            [("USDC", Decimal("1")), ("WETH", Decimal("2"))],
            id="both-positive",
        ),
        pytest.param("", Decimal("1"), "WETH", Decimal("2"), [("WETH", Decimal("2"))], id="missing-token"),
        pytest.param("USDC", None, "WETH", Decimal("0"), [], id="none-and-measured-zero"),
        pytest.param("USDC", Decimal("-1"), "WETH", Decimal("-2"), [], id="non-positive"),
    ],
)
def test_open_leg_table_preserves_disposal_order(
    token0: str,
    amount0: Decimal | None,
    token1: str,
    amount1: Decimal | None,
    expected: list[tuple[str, Decimal]],
) -> None:
    store = MagicMock(spec=FIFOBasisStore)

    _apply(store, token0=token0, amount0=amount0, token1=token1, amount1=amount1)

    assert store.method_calls == [
        call.match_swap_disposal(
            deployment_id="dep-1",
            position_key=_WALLET_KEY,
            token=token,
            amount=amount,
        )
        for token, amount in expected
    ]


@pytest.mark.parametrize(
    (
        "intent_type",
        "amount",
        "fees",
        "cost_basis_usd",
        "fees_total_usd",
        "expected_amount",
        "expected_cost",
    ),
    [
        pytest.param(
            "LP_CLOSE",
            Decimal("1.05"),
            Decimal("0.05"),
            Decimal("210"),
            Decimal("10"),
            Decimal("1.05"),
            Decimal("210"),
            id="fee-inclusive-close-uses-principal-value",
        ),
        pytest.param(
            "LP_COLLECT_FEES",
            Decimal("0"),
            Decimal("0.03"),
            Decimal("0"),
            Decimal("6"),
            Decimal("0.03"),
            Decimal("6"),
            id="fee-separate-collect-uses-fee-value",
        ),
        pytest.param(
            "LP_CLOSE",
            None,
            Decimal("0.03"),
            None,
            Decimal("0"),
            Decimal("0.03"),
            Decimal("0"),
            id="measured-zero-fee-value-stays-zero",
        ),
        pytest.param(
            "LP_CLOSE",
            Decimal("1"),
            None,
            None,
            Decimal("6"),
            Decimal("1"),
            None,
            id="unmeasured-principal-value-stays-none",
        ),
    ],
)
def test_return_transition_table_preserves_quantity_and_basis_semantics(
    intent_type: str,
    amount: Decimal | None,
    fees: Decimal | None,
    cost_basis_usd: Decimal | None,
    fees_total_usd: Decimal | None,
    expected_amount: Decimal,
    expected_cost: Decimal | None,
) -> None:
    store = MagicMock(spec=FIFOBasisStore)

    _apply(
        store,
        intent_type_str=intent_type,
        token1="",
        amount0=amount,
        amount1=Decimal("0"),
        fees0=fees,
        fees1=Decimal("0"),
        cost_basis_usd=cost_basis_usd,
        fees_total_usd=fees_total_usd,
        price_oracle={"USDC": Decimal("1")},
    )

    kwargs = store.record_swap_acquisition.call_args.kwargs
    assert kwargs["amount"] == expected_amount
    assert kwargs["cost_usd"] == expected_cost
    assert kwargs["source"] == intent_type


def test_close_records_legs_in_order_and_assigns_exact_residual() -> None:
    store = MagicMock(spec=FIFOBasisStore)

    _apply(
        store,
        intent_type_str="LP_CLOSE",
        amount0=Decimal("100"),
        amount1=Decimal("0.01"),
        fees0=Decimal("0"),
        fees1=Decimal("0"),
        cost_basis_usd=Decimal("120"),
        fees_total_usd=Decimal("0"),
        price_oracle={"USDC": Decimal("1"), "WETH": Decimal("2000")},
    )

    calls = store.record_swap_acquisition.call_args_list
    assert [item.kwargs["token"] for item in calls] == ["USDC", "WETH"]
    assert [item.kwargs["amount"] for item in calls] == [Decimal("100"), Decimal("0.01")]
    assert [item.kwargs["cost_usd"] for item in calls] == [Decimal("100"), Decimal("20")]


@pytest.mark.parametrize(
    ("tx_hash", "ledger_entry_id", "expected_seed"),
    [
        pytest.param("0xtx", "ledger-1", "0xtx", id="tx-hash-precedes-ledger-id"),
        pytest.param("", "ledger-1", "ledger-1", id="ledger-id-fallback"),
        pytest.param("", "", None, id="empty-seed-keeps-empty-lot-id"),
    ],
)
def test_close_lot_identity_seed_precedence(tx_hash: str, ledger_entry_id: str, expected_seed: str | None) -> None:
    store = MagicMock(spec=FIFOBasisStore)
    event_id = MagicMock(return_value="event-id")

    with patch(
        "almanak.framework.accounting.category_handlers.lp_handler.make_accounting_event_id",
        event_id,
    ):
        _apply(
            store,
            intent_type_str="LP_CLOSE",
            token1="",
            amount1=Decimal("0"),
            cost_basis_usd=Decimal("1"),
            price_oracle={"USDC": Decimal("1")},
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
        )

    lot_id = store.record_swap_acquisition.call_args.kwargs["lot_id"]
    if expected_seed is None:
        event_id.assert_not_called()
        assert lot_id == ""
    else:
        event_id.assert_called_once_with(
            "dep-1",
            "cycle-1",
            "LP_CLOSE_WALLET_LOT",
            expected_seed,
            "USDC",
        )
        assert lot_id == "event-id"


@pytest.mark.parametrize("transition", ["LP_OPEN", "LP_CLOSE"])
def test_fifo_errors_propagate_without_running_later_legs(transition: str) -> None:
    store = MagicMock(spec=FIFOBasisStore)
    method = store.match_swap_disposal if transition == "LP_OPEN" else store.record_swap_acquisition
    method.side_effect = RuntimeError("fifo failure")

    with pytest.raises(RuntimeError, match="fifo failure"):
        _apply(
            store,
            intent_type_str=transition,
            cost_basis_usd=Decimal("3"),
            price_oracle={"USDC": Decimal("1"), "WETH": Decimal("1")},
        )

    assert method.call_count == 1


@pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"])
def test_handle_lp_and_direct_hook_emit_identical_fifo_operations(intent_type: str) -> None:
    public_store = MagicMock(spec=FIFOBasisStore)
    direct_store = MagicMock(spec=FIFOBasisStore)
    outbox = {
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "wallet_address": "0xwallet",
        "position_key": "lp:uniswap_v3:arbitrum:0xwallet:USDC/WETH/500",
        "market_id": "0x1111111111111111111111111111111111111111",
    }
    ledger = {
        "id": "ledger-1",
        "deployment_id": "dep-1",
        "cycle_id": "cycle-1",
        "execution_mode": "live",
        "timestamp": _TIMESTAMP.isoformat(),
        "intent_type": intent_type,
        "token_in": "USDC",
        "token_out": "WETH",
        "amount_in": "100",
        "amount_out": "0.01",
        "tx_hash": "0xtx",
        "chain": "arbitrum",
        "protocol": "uniswap_v3",
        "extracted_data_json": "",
        "price_inputs_json": json.dumps({"USDC": "1", "WETH": "2000"}),
    }

    event = handle_lp(outbox, ledger, basis_store=public_store)

    assert event is not None
    _apply(
        direct_store,
        intent_type_str=intent_type,
        amount0=event.amount0,
        amount1=event.amount1,
        fees0=event.fees0_collected,
        fees1=event.fees1_collected,
        cost_basis_usd=event.cost_basis_usd,
        fees_total_usd=event.fees_total_usd,
        price_oracle={"USDC": Decimal("1"), "WETH": Decimal("2000")},
    )
    assert public_store.method_calls == direct_store.method_calls
