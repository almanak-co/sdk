"""Managed-Anvil GMX keeper executor unit coverage."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from eth_abi import encode as abi_encode

from almanak.connectors.gmx_v2.anvil_order_executor import (
    _find_order_keeper,
    _GmxDependencies,
    _read_price_bounds,
    _send_transaction,
    execute_pending_orders_on_anvil,
)

_KEY_A = "0x" + "11" * 32
_KEY_B = "0x" + "22" * 32
_WALLET = "0x" + "33" * 20
_MARKET_A = "0x" + "44" * 20
_MARKET_B = "0x" + "55" * 20
_ORDER_HANDLER = "0x" + "66" * 20
_ORACLE = "0x" + "77" * 20
_ROLE_STORE = "0x" + "88" * 20
_DATA_STORE = "0x" + "99" * 20
_READER = "0x" + "aa" * 20
_KEEPER = "0x" + "bb" * 20
_TOKEN = "0x" + "cc" * 20

_DEPENDENCIES = _GmxDependencies(
    order_handler=_ORDER_HANDLER,
    oracle=_ORACLE,
    role_store=_ROLE_STORE,
    data_store=_DATA_STORE,
    reader=_READER,
)


def _pending(*items: tuple[str, str]) -> SimpleNamespace:
    orders = tuple(SimpleNamespace(order_key=key, market=market) for key, market in items)
    return SimpleNamespace(
        ok=True,
        order_keys=[key for key, _market in items],
        orders=orders,
        truncated=False,
        error=None,
    )


def test_executor_rejects_non_anvil_without_reading_orders() -> None:
    with patch("almanak.connectors.gmx_v2.anvil_order_executor.read_pending_orders") as read:
        result = execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address=_WALLET,
            orders=(SimpleNamespace(order_id=_KEY_A),),
            network="mainnet",
        )

    assert result.ok is False
    assert "restricted" in (result.reason or "")
    read.assert_not_called()


def test_executor_rejects_malformed_or_zero_order_keys() -> None:
    for key in ("not-hex", "0x" + "00" * 32):
        with patch("almanak.connectors.gmx_v2.anvil_order_executor.read_pending_orders") as read:
            result = execute_pending_orders_on_anvil(
                gateway_client=object(),
                chain="arbitrum",
                wallet_address=_WALLET,
                orders=(SimpleNamespace(order_id=key),),
                network="anvil",
            )

        assert result.ok is False
        assert "Invalid GMX order key" in (result.reason or "")
        read.assert_not_called()


def test_executor_seeds_and_cleans_oracle_state_per_exact_order() -> None:
    provider = MagicMock()
    seed_hashes = (("0xseed-a",), ("0xseed-b",))
    receipt_a = {"transactionHash": "0xexecute-a", "status": "0x1", "logs": []}
    receipt_b = {"transactionHash": "0xexecute-b", "status": "0x1", "logs": []}
    with (
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor.read_pending_orders",
            return_value=_pending((_KEY_A, _MARKET_A), (_KEY_B, _MARKET_B)),
        ),
        patch("almanak.connectors.gmx_v2.anvil_order_executor.GatewayWeb3Provider", return_value=provider),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._load_dependencies", return_value=_DEPENDENCIES),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._has_role", return_value=True),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._find_order_keeper", return_value=_KEEPER),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._impersonated", return_value=nullcontext()),
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._oracle_price_count",
            side_effect=(0, 0, 0, 0),
        ),
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._seed_oracle_prices",
            side_effect=seed_hashes,
        ) as seed,
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._execute_order",
            side_effect=(("0xexecute-a", receipt_a), ("0xexecute-b", receipt_b)),
        ) as execute,
        patch("almanak.connectors.gmx_v2.anvil_order_executor._clear_oracle_prices") as clear,
    ):
        result = execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address=_WALLET,
            orders=(SimpleNamespace(order_id=_KEY_A), SimpleNamespace(order_id=_KEY_B)),
            network="anvil",
        )

    assert result.ok is True
    assert result.executed_order_keys == (_KEY_A, _KEY_B)
    assert result.transaction_hashes == ("0xseed-a", "0xexecute-a", "0xseed-b", "0xexecute-b")
    assert result.execution_receipts == (receipt_a, receipt_b)
    assert seed.call_args_list[0].kwargs["markets"] == (_MARKET_A,)
    assert seed.call_args_list[1].kwargs["markets"] == (_MARKET_B,)
    assert execute.call_args_list == [
        call(provider, _DEPENDENCIES, _KEEPER, _KEY_A),
        call(provider, _DEPENDENCIES, _KEEPER, _KEY_B),
    ]
    clear.assert_not_called()


def test_executor_clears_partial_oracle_state_when_seeding_fails() -> None:
    provider = MagicMock()
    with (
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor.read_pending_orders",
            return_value=_pending((_KEY_A, _MARKET_A)),
        ),
        patch("almanak.connectors.gmx_v2.anvil_order_executor.GatewayWeb3Provider", return_value=provider),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._load_dependencies", return_value=_DEPENDENCIES),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._has_role", return_value=True),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._find_order_keeper", return_value=_KEEPER),
        patch("almanak.connectors.gmx_v2.anvil_order_executor._impersonated", return_value=nullcontext()),
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._oracle_price_count",
            side_effect=(0, 1),
        ),
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._seed_oracle_prices",
            side_effect=RuntimeError("price unavailable"),
        ),
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._clear_oracle_prices",
            return_value="0xcleanup",
        ) as clear,
    ):
        result = execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address=_WALLET,
            orders=(SimpleNamespace(order_id=_KEY_A),),
            network="anvil",
        )

    assert result.ok is False
    assert "price unavailable" in (result.reason or "")
    clear.assert_called_once_with(provider, _DEPENDENCIES)


def test_price_bounds_use_gateway_price_and_measured_on_chain_decimals() -> None:
    gateway_client = MagicMock()
    gateway_client.market.GetPrice.return_value = SimpleNamespace(price="1.2345678901234", stale=False)
    provider = MagicMock()

    with patch("almanak.connectors.gmx_v2.anvil_order_executor._read_token_decimals", return_value=18):
        minimum, maximum = _read_price_bounds(gateway_client, provider, "arbitrum", _TOKEN)

    assert minimum == 1_234_567_890_123
    assert maximum == 1_234_567_890_124
    request = gateway_client.market.GetPrice.call_args.args[0]
    assert request.token == _TOKEN
    assert request.chain == "arbitrum"


def test_keeper_is_enumerated_from_the_forked_role_store() -> None:
    provider = MagicMock()
    encoded_count = "0x" + abi_encode(["uint256"], [1]).hex()
    encoded_members = "0x" + abi_encode(["address[]"], [[_KEEPER]]).hex()
    encoded_role = "0x" + abi_encode(["bool"], [True]).hex()

    with patch(
        "almanak.connectors.gmx_v2.anvil_order_executor._eth_call",
        side_effect=(encoded_count, encoded_members, encoded_role),
    ):
        keeper = _find_order_keeper(provider, _ROLE_STORE)

    assert keeper.lower() == _KEEPER.lower()


def test_impersonated_transaction_uses_measured_gas_limit() -> None:
    provider = MagicMock()
    web3 = MagicMock()
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1, "gasUsed": 20_000}
    with (
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._rpc",
            side_effect=("0x7530", "0x1", "0x186a0", "0xtx"),
        ) as rpc,
        patch("almanak.connectors.gmx_v2.anvil_order_executor.Web3", return_value=web3),
    ):
        tx_hash = _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234")

    assert tx_hash == "0xtx"
    assert rpc.call_args_list[0] == call(
        provider,
        "eth_estimateGas",
        [
            {
                "from": _ORDER_HANDLER,
                "to": _ORACLE,
                "data": "0x1234",
                "value": "0x0",
            }
        ],
    )
    assert rpc.call_args_list[3].args[2][0]["gas"] == "0x7530"
    assert rpc.call_args_list[3].args[2][0]["gasPrice"] == "0x1"


def test_impersonated_transaction_tops_up_only_measured_gas_cost() -> None:
    provider = MagicMock()
    web3 = MagicMock()
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1, "gasUsed": 20_000}
    with (
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._rpc",
            side_effect=("0x7530", "0x2", "0x0", None, "0xtx"),
        ) as rpc,
        patch("almanak.connectors.gmx_v2.anvil_order_executor.Web3", return_value=web3),
    ):
        _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234")

    assert rpc.call_args_list[3] == call(
        provider,
        "anvil_setBalance",
        [_ORDER_HANDLER, hex(30_000 * 2)],
    )


# ---------------------------------------------------------------------------
# executeOrder outcome verification (tx success != fill)
# ---------------------------------------------------------------------------


def _empty_group() -> tuple[list, list]:
    return ([], [])


def _event_emitter_data(event_name: str, *, reason: str = "", reason_bytes: bytes = b"") -> str:
    from eth_abi import encode as abi_encode

    from almanak.connectors.gmx_v2.receipt_parser import _EVENT_LOG_DATA_ABI_TYPE

    strings_group = ([("reason", reason)] if reason else [], [])
    bytes_group = ([("reasonBytes", reason_bytes)] if reason_bytes else [], [])
    payload = abi_encode(
        ["address", "string", _EVENT_LOG_DATA_ABI_TYPE],
        [
            "0x" + "11" * 20,
            event_name,
            (
                _empty_group(),  # addresses
                _empty_group(),  # uints
                _empty_group(),  # ints
                _empty_group(),  # bools
                _empty_group(),  # bytes32
                bytes_group,
                strings_group,
            ),
        ],
    )
    return "0x" + payload.hex()


def _outcome_log(event_name: str, order_key: str, data: str) -> dict:
    from almanak.connectors.gmx_v2.receipt_parser import EVENT_TOPICS

    return {
        "topics": ["0x" + "ee" * 32, EVENT_TOPICS[event_name], order_key],
        "data": data,
        "address": "0x" + "22" * 20,
        "logIndex": "0x0",
    }


def test_execution_outcome_requires_an_order_outcome_event() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _verify_execution_outcome,
    )

    with pytest.raises(GmxAnvilOrderExecutionError, match="outcome unmeasured"):
        _verify_execution_outcome({"logs": []}, _KEY_A, "0xtx")
    with pytest.raises(GmxAnvilOrderExecutionError, match="no log list"):
        _verify_execution_outcome({}, _KEY_A, "0xtx")


def test_execution_outcome_accepts_executed_order() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import _verify_execution_outcome

    receipt = {"logs": [_outcome_log("OrderExecuted", _KEY_A, _event_emitter_data("OrderExecuted"))]}
    _verify_execution_outcome(receipt, _KEY_A, "0xtx")  # must not raise


def test_execution_outcome_surfaces_venue_cancellation_reason() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _verify_execution_outcome,
    )

    data = _event_emitter_data("OrderCancelled", reason="OrderNotFulfillableAtAcceptablePrice")
    receipt = {"logs": [_outcome_log("OrderCancelled", _KEY_A, data)]}
    with pytest.raises(GmxAnvilOrderExecutionError, match="OrderNotFulfillableAtAcceptablePrice"):
        _verify_execution_outcome(receipt, _KEY_A, "0xtx")


def test_execution_outcome_ignores_other_orders_events() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _verify_execution_outcome,
    )

    # A cancellation for a DIFFERENT key must not be attributed to ours.
    data = _event_emitter_data("OrderCancelled", reason="SomeOtherOrder")
    receipt = {"logs": [_outcome_log("OrderCancelled", _KEY_B, data)]}
    with pytest.raises(GmxAnvilOrderExecutionError, match="outcome unmeasured"):
        _verify_execution_outcome(receipt, _KEY_A, "0xtx")


# ---------------------------------------------------------------------------
# Cold-fork estimateGas warm retry
# ---------------------------------------------------------------------------


def test_estimate_gas_retries_on_client_deadline_then_succeeds() -> None:
    import grpc

    from almanak.connectors.gmx_v2.anvil_order_executor import _estimate_gas_with_warm_retry

    class _Deadline(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    provider = MagicMock()
    provider.make_request.side_effect = [_Deadline(), {"result": "0x5208"}]

    assert _estimate_gas_with_warm_retry(provider, {"from": "0x0"}) == "0x5208"
    assert provider.make_request.call_count == 2


def test_estimate_gas_retries_on_gateway_request_timeout() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import _estimate_gas_with_warm_retry

    provider = MagicMock()
    provider.make_request.side_effect = [
        {"error": {"code": -32603, "message": "Request timeout"}},
        {"result": "0x5208"},
    ]
    assert _estimate_gas_with_warm_retry(provider, {"from": "0x0"}) == "0x5208"


def test_estimate_gas_does_not_retry_non_timeout_errors() -> None:
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _estimate_gas_with_warm_retry,
    )

    provider = MagicMock()
    provider.make_request.return_value = {"error": {"code": 3, "message": "execution reverted"}}
    with pytest.raises(GmxAnvilOrderExecutionError, match="execution reverted"):
        _estimate_gas_with_warm_retry(provider, {"from": "0x0"})
    assert provider.make_request.call_count == 1


def test_estimate_gas_gives_up_after_bounded_attempts() -> None:
    import grpc

    from almanak.connectors.gmx_v2.anvil_order_executor import (
        _ESTIMATE_GAS_ATTEMPTS,
        _estimate_gas_with_warm_retry,
    )

    class _Deadline(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.DEADLINE_EXCEEDED

    provider = MagicMock()
    provider.make_request.side_effect = _Deadline()
    with pytest.raises(grpc.RpcError):
        _estimate_gas_with_warm_retry(provider, {"from": "0x0"})
    assert provider.make_request.call_count == _ESTIMATE_GAS_ATTEMPTS


def test_transient_upstream_fetch_failure_is_classified_transient() -> None:
    """Anvil fork-backend fetch failures must be retryable, not structural."""
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _is_transient_execution_error,
    )

    assert _is_transient_execution_error(
        GmxAnvilOrderExecutionError(
            "eth_estimateGas failed: failed to get storage for 0xFD70 at 123: error sending request for url"
        )
    )
    assert _is_transient_execution_error(GmxAnvilOrderExecutionError("eth_call failed: Request timeout"))
    assert not _is_transient_execution_error(GmxAnvilOrderExecutionError("execution reverted: OrderNotFound"))


def test_entrypoint_marks_transient_failures_on_result() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    def _raise_transient(*args, **kwargs):
        raise mod.GmxAnvilOrderExecutionError("failed to get storage for 0xabc at 1: error sending request for url")

    with (
        patch.object(mod, "read_pending_orders", return_value=_pending((_KEY_A, _MARKET_A))),
        patch.object(mod, "GatewayWeb3Provider", return_value=MagicMock()),
        patch.object(mod, "_load_dependencies", side_effect=_raise_transient),
    ):
        result = mod.execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address=_WALLET,
            orders=(SimpleNamespace(order_id=_KEY_A),),
            network="anvil",
        )
    assert result.ok is False
    assert result.transient is True

    def _raise_structural(*args, **kwargs):
        raise mod.GmxAnvilOrderExecutionError("GMX OrderHandler does not hold CONTROLLER")

    with (
        patch.object(mod, "read_pending_orders", return_value=_pending((_KEY_A, _MARKET_A))),
        patch.object(mod, "GatewayWeb3Provider", return_value=MagicMock()),
        patch.object(mod, "_load_dependencies", side_effect=_raise_structural),
    ):
        result = mod.execute_pending_orders_on_anvil(
            gateway_client=object(),
            chain="arbitrum",
            wallet_address=_WALLET,
            orders=(SimpleNamespace(order_id=_KEY_A),),
            network="anvil",
        )
    assert result.ok is False
    assert result.transient is False


def test_execution_outcome_never_attributes_a_keyless_event() -> None:
    """An outcome event with a missing/undecodable key must not count as proof."""
    from almanak.connectors.gmx_v2.anvil_order_executor import (
        GmxAnvilOrderExecutionError,
        _verify_execution_outcome,
    )

    keyless = _outcome_log("OrderExecuted", _KEY_B, _event_emitter_data("OrderExecuted"))
    keyless["topics"] = keyless["topics"][:2]  # strip the indexed key topic entirely
    receipt = {"logs": [keyless]}
    with pytest.raises(GmxAnvilOrderExecutionError, match="outcome unmeasured"):
        _verify_execution_outcome(receipt, _KEY_A, "0xtx")
