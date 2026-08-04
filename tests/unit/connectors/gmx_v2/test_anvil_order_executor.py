"""Managed-Anvil GMX keeper executor unit coverage."""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import grpc
import pytest
from eth_abi import encode as abi_encode

from almanak.connectors.gmx_v2.anvil_order_executor import (
    GmxAnvilOrderExecutionError,
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


def test_stale_gateway_price_is_advisory_on_the_anvil_path(caplog: pytest.LogCaptureFixture) -> None:
    """VIB-6491: fork staleness must warn and return the price, never raise.

    On a pinned fork the Chainlink ``updatedAt`` is frozen while the staleness
    clock is live wall time, so every fork older than the threshold flags
    stale. Raising here gave pinned forks a ~1h shelf life and destroyed the
    only reproduction block of VIB-6437.
    """
    gateway_client = MagicMock()
    gateway_client.market.GetPrice.return_value = SimpleNamespace(price="1.2345678901234", stale=True)
    provider = MagicMock()

    with (
        patch("almanak.connectors.gmx_v2.anvil_order_executor._read_token_decimals", return_value=18),
        caplog.at_level("WARNING", logger="almanak.connectors.gmx_v2.anvil_order_executor"),
    ):
        minimum, maximum = _read_price_bounds(gateway_client, provider, "arbitrum", _TOKEN)

    # Same measured bounds as the fresh-price test: the price is used, not substituted.
    assert minimum == 1_234_567_890_123
    assert maximum == 1_234_567_890_124
    # Liveness of the downgrade: the staleness must still be visible to the operator.
    assert any("is stale" in record.message and record.levelname == "WARNING" for record in caplog.records)


def test_invalid_gateway_price_still_fails_loudly_even_when_stale() -> None:
    """VIB-6491 acceptance: the stale downgrade must not widen into ignoring bad prices."""
    gateway_client = MagicMock()
    provider = MagicMock()
    for bad_price in ("not-a-price", "0", "-1"):
        gateway_client.market.GetPrice.return_value = SimpleNamespace(price=bad_price, stale=True)
        with (
            patch("almanak.connectors.gmx_v2.anvil_order_executor._read_token_decimals", return_value=18),
            pytest.raises(GmxAnvilOrderExecutionError),
        ):
            _read_price_bounds(gateway_client, provider, "arbitrum", _TOKEN)


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


def test_impersonated_transaction_submits_estimate_plus_bounded_headroom() -> None:
    provider = MagicMock()
    web3 = MagicMock()
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1, "gasUsed": 20_000}
    with (
        patch(
            "almanak.connectors.gmx_v2.anvil_order_executor._rpc",
            # estimate, gasPrice, balance (100,000 — BELOW the floored 130,000
            # submitted limit, so a top-up fires), setBalance ack, send.
            side_effect=("0x7530", "0x1", "0x186a0", None, "0xtx"),
        ) as rpc,
        patch("almanak.connectors.gmx_v2.anvil_order_executor.Web3", return_value=web3),
    ):
        tx_hash = _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234", kind="order")

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
    # Submitted limit = estimate (0x7530 = 30,000) + the VIB-6450 headroom,
    # FLOORED at 100,000 for small transactions. A bare-estimate submission is
    # the zero-margin defect R16 measured killing the VIB-6437 partial close —
    # this assertion fails on that behaviour.
    assert rpc.call_args_list[3] == call(provider, "anvil_setBalance", [_ORDER_HANDLER, hex(130_000)])
    assert rpc.call_args_list[4].args[2][0]["gas"] == hex(130_000)
    assert rpc.call_args_list[4].args[2][0]["gasPrice"] == "0x1"


def test_submitted_gas_limit_carries_bounded_headroom() -> None:
    """VIB-6450 requires a floor and a ceiling, not an unbounded multiplier."""
    from almanak.connectors.gmx_v2.anvil_order_executor import _submitted_gas_limit

    # R17-validated case preserved bit-for-bit: estimate 4,108,084 → +410,808
    # (proportional band) → 4,518,892 — the exact run that filled the 7/7
    # reproducer. The +410,808 covers the measured ~50k drift several times over.
    assert _submitted_gas_limit(4_108_084) == 4_518_892
    # FLOOR: small estimates get an absolute 100k headroom, not 10%.
    assert _submitted_gas_limit(30_000) == 130_000
    assert _submitted_gas_limit(999_999) == 1_099_999
    # Proportional band begins where 10% exceeds the floor.
    assert _submitted_gas_limit(1_000_000) == 1_100_000
    # CEILING: the margin never exceeds 1M no matter the estimate.
    assert _submitted_gas_limit(30_000_000) == 31_000_000
    # Never negative or shrinking.
    assert _submitted_gas_limit(1) == 100_001


def test_impersonated_transaction_tops_up_the_submitted_gas_cost() -> None:
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
        _send_transaction(provider, _ORDER_HANDLER, _ORACLE, "0x1234", kind="order")

    # The top-up funds the SUBMITTED (headroom-carrying) limit, not the bare
    # estimate — funding less than the limit would fail the send on balance.
    assert rpc.call_args_list[3] == call(
        provider,
        "anvil_setBalance",
        [_ORDER_HANDLER, hex(130_000 * 2)],
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


def _rpc_error(code: grpc.StatusCode, details: str = "") -> grpc.RpcError:
    class _Err(grpc.RpcError):
        def code(self):
            return code

        def details(self):
            return details

        def __str__(self):
            return f"{code}: {details}"

    return _Err()


def test_gateway_rate_limit_is_classified_transient() -> None:
    """ALM-3025: a rate limit is self-healing and the server says exactly when.

    Classifying it structural made the settlement barrier report
    INFRASTRUCTURE_UNSUPPORTED and give up immediately, 37s short of recovery
    and well inside its 360s budget.
    """
    from almanak.connectors.gmx_v2.anvil_order_executor import _is_transient_execution_error

    assert _is_transient_execution_error(
        _rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limited, retry after 37.71s")
    )
    # The pre-existing DEADLINE_EXCEEDED case must keep working.
    assert _is_transient_execution_error(_rpc_error(grpc.StatusCode.DEADLINE_EXCEEDED))
    # Config/auth defects stay structural — more attempts cannot fix them.
    assert not _is_transient_execution_error(_rpc_error(grpc.StatusCode.UNAUTHENTICATED))
    assert not _is_transient_execution_error(_rpc_error(grpc.StatusCode.PERMISSION_DENIED))
    assert not _is_transient_execution_error(_rpc_error(grpc.StatusCode.INVALID_ARGUMENT))


def test_rate_limited_settlement_is_retryable_not_infrastructure_unsupported() -> None:
    """End-to-end at the executor boundary: transient=True reaches the caller."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    with (
        patch.object(mod, "read_pending_orders", return_value=_pending((_KEY_A, _MARKET_A))),
        patch.object(mod, "GatewayWeb3Provider", return_value=MagicMock()),
        patch.object(
            mod,
            "_load_dependencies",
            side_effect=_rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limited, retry after 37.71s"),
        ),
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


def test_impersonation_cleanup_failure_does_not_mask_the_body_failure(caplog) -> None:
    """ALM-3025: cleanup runs on the same channel that just failed the body.

    Python lets an exception raised in a ``finally`` REPLACE the in-flight one,
    which is how a rate-limited run reported ``anvil_stopImpersonatingAccount``
    as its root cause and buried the real failure.

    A sustained rate limit fails BOTH cleanup RPCs, so the log must name both —
    dropping the balance restore would hide that the account was left funded.
    """
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()

    def _rpc(_provider, method, _params):
        if method in {"anvil_setBalance", "anvil_stopImpersonatingAccount"}:
            raise mod.GmxAnvilOrderExecutionError(f"{method} failed: Rate limited, retry after 37.71s")
        return "0x0"

    with patch.object(mod, "_rpc", side_effect=_rpc), caplog.at_level("ERROR", logger=mod.__name__):
        with pytest.raises(mod.GmxAnvilOrderExecutionError, match="the original failure"):
            with mod._impersonated(provider, _ORDER_HANDLER):
                raise mod.GmxAnvilOrderExecutionError("the original failure")

    cleanup_log = "\n".join(record.getMessage() for record in caplog.records)
    assert "anvil_setBalance failed" in cleanup_log
    assert "anvil_stopImpersonatingAccount failed" in cleanup_log
    assert "left impersonating" in cleanup_log


def test_impersonation_cleanup_failure_surfaces_when_the_body_succeeded() -> None:
    """A dirty fork is not a silent success — nothing else would notice."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()

    def _rpc(_provider, method, _params):
        if method == "anvil_stopImpersonatingAccount":
            raise mod.GmxAnvilOrderExecutionError("anvil_stopImpersonatingAccount failed: boom")
        return "0x0"

    with patch.object(mod, "_rpc", side_effect=_rpc):
        with pytest.raises(mod.GmxAnvilOrderExecutionError, match="stopImpersonating"):
            with mod._impersonated(provider, _ORDER_HANDLER):
                pass


def test_impersonation_cleanup_raises_the_first_failure_when_both_steps_fail(caplog) -> None:
    """Both steps are attempted; the balance restore is the one re-raised.

    ``anvil_stopImpersonatingAccount`` must still run after ``anvil_setBalance``
    fails (leaving the fork impersonating is worse), but the balance failure is
    the more actionable of the two, so it is the one that surfaces.
    """
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    attempted: list[str] = []

    def _rpc(_provider, method, _params):
        if method in {"anvil_setBalance", "anvil_stopImpersonatingAccount"}:
            attempted.append(method)
            raise mod.GmxAnvilOrderExecutionError(f"{method} failed: Rate limited, retry after 37.71s")
        return "0x0"

    with patch.object(mod, "_rpc", side_effect=_rpc), caplog.at_level("ERROR", logger=mod.__name__):
        with pytest.raises(mod.GmxAnvilOrderExecutionError, match="anvil_setBalance failed"):
            with mod._impersonated(MagicMock(), _ORDER_HANDLER):
                pass

    assert attempted == ["anvil_setBalance", "anvil_stopImpersonatingAccount"]
    cleanup_log = "\n".join(record.getMessage() for record in caplog.records)
    assert "anvil_setBalance failed" in cleanup_log
    assert "anvil_stopImpersonatingAccount failed" in cleanup_log


def test_oracle_cleanup_failure_does_not_mask_the_body_failure() -> None:
    """Seeded prices left behind make the fork unusable, but the body's error wins."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    hashes: list[str] = []
    with patch.object(
        mod,
        "_oracle_price_count",
        side_effect=mod.GmxAnvilOrderExecutionError("eth_call failed: Rate limited, retry after 37.71s"),
    ):
        # body_failed=True → log the dirty fork, let the original propagate.
        mod._clear_seeded_oracle_prices(MagicMock(), _DEPENDENCIES, hashes, body_failed=True)

        # body_failed=False → the cleanup failure IS the failure.
        with pytest.raises(mod.GmxAnvilOrderExecutionError, match="Rate limited"):
            mod._clear_seeded_oracle_prices(MagicMock(), _DEPENDENCIES, hashes, body_failed=False)

    assert hashes == []


# ---------------------------------------------------------------------------
# Call-trace probe (VIB-6437): the primary revert GMX's error handler swallows
# is recoverable only from a call trace, and the probe must never fabricate one.


def _gmx_revert_tree() -> dict:
    """A callTracer tree in the shape the VIB-6437 revert produces.

    Outer executeOrder frame reverts with the HANDLER's error
    (InsufficientGasForCancellation); the inner try/catch sub-call carries the
    PRIMARY revert payload the handler swallowed.
    """
    return {
        "type": "CALL",
        "to": "0x" + "aa" * 20,
        "input": "0x" + "e68e69a6" + "00" * 32,
        "gasUsed": hex(3_252_233),
        "error": "execution reverted",
        "output": "0xd3dacaac" + "00" * 62 + "1234",
        "calls": [
            {"type": "STATICCALL", "to": "0x" + "bb" * 20, "input": "0x12345678", "gasUsed": "0x100"},
            {
                "type": "CALL",
                "to": "0x" + "cc" * 20,
                "input": "0xdeadbeef",
                "gasUsed": hex(3_000_000),
                "error": "execution reverted",
                # The primary cause — a distinct custom error selector.
                "output": "0x11223344" + "ab" * 32,
                "calls": [],
            },
        ],
    }


def _fill_tree() -> dict:
    """A callTracer tree for a clean fill: frames, no errors anywhere."""
    return {
        "type": "CALL",
        "to": "0x" + "aa" * 20,
        "input": "0x" + "e68e69a6",
        "gasUsed": hex(900_000),
        "calls": [{"type": "STATICCALL", "to": "0x" + "bb" * 20, "input": "0x12345678", "gasUsed": "0x100"}],
    }


def test_erroring_frames_returns_outermost_first_with_depths() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    frames, truncated = mod._erroring_frames(_gmx_revert_tree())

    assert truncated is False
    assert [frame["depth"] for frame in frames] == [0, 1]
    assert frames[0]["output"].startswith("0xd3dacaac")
    assert frames[1]["output"].startswith("0x11223344")


def test_erroring_frames_on_a_fill_tree_is_empty() -> None:
    """Negative control: a clean fill must yield NO erroring frames — the probe
    must never fabricate a revert out of a successful trace."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    frames, truncated = mod._erroring_frames(_fill_tree())

    assert frames == []
    assert truncated is False


def test_erroring_frames_announces_truncation_at_the_walk_cap() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    wide = {"type": "CALL", "error": "boom", "calls": [{"type": "CALL", "error": "x"} for _ in range(50)]}
    with patch.object(mod, "_TRACE_MAX_FRAMES", 10):
        frames, truncated = mod._erroring_frames(wide)

    assert truncated is True
    assert len(frames) <= 10


def test_trace_revert_frames_reports_the_inner_payload() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()
    provider.make_request.return_value = {"result": _gmx_revert_tree()}

    detail = mod._trace_revert_frames(provider, "0x" + "11" * 32)

    assert "outermost first" in detail
    assert "0xd3dacaac" in detail
    assert "0x11223344" in detail
    method, params = provider.make_request.call_args.args
    assert method == "debug_traceTransaction"
    assert params[1] == {"tracer": "callTracer"}


def test_trace_revert_frames_reads_unavailable_as_unmeasured() -> None:
    """A lost trace must read as unmeasured, not as an empty or clean trace."""
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()
    provider.make_request.return_value = {"error": {"message": "method not found"}}

    detail = mod._trace_revert_frames(provider, "0x" + "11" * 32)

    assert "unavailable" in detail


def test_trace_without_erroring_frames_does_not_corroborate() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    provider = MagicMock()
    provider.make_request.return_value = {"result": _fill_tree()}

    detail = mod._trace_revert_frames(provider, "0x" + "11" * 32)

    assert "does not corroborate" in detail


def test_diagnose_mined_revert_trace_failure_never_masks_the_replay() -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    with (
        patch.object(mod, "_replay_revert_reason", return_value="replay says X"),
        patch.object(mod, "_trace_revert_frames", side_effect=RuntimeError("tracer exploded")),
    ):
        diagnosis = mod._diagnose_mined_revert(MagicMock(), {}, "0x" + "11" * 32, 7)

    assert "replay says X" in diagnosis
    assert "trace unavailable" in diagnosis


def test_trace_artifact_written_only_under_env_dir(tmp_path, monkeypatch) -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    tx_hash = "0x" + "ab" * 32
    monkeypatch.delenv(mod._TRACE_DIR_ENV, raising=False)
    mod._write_trace_artifact(tx_hash, {"type": "CALL"})
    assert list(tmp_path.iterdir()) == []

    monkeypatch.setenv(mod._TRACE_DIR_ENV, str(tmp_path))
    mod._write_trace_artifact(tx_hash, {"type": "CALL"})
    written = list(tmp_path.iterdir())
    assert [p.name for p in written] == [f"{tx_hash}.calltrace.json"]

    # A non-hash never becomes a filename.
    mod._write_trace_artifact("../../etc/passwd", {"type": "CALL"})
    assert len(list(tmp_path.iterdir())) == 1


def test_fill_trace_capture_is_inert_without_the_env_dir(monkeypatch) -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    monkeypatch.delenv(mod._TRACE_DIR_ENV, raising=False)
    provider = MagicMock()
    mod._capture_trace_artifact_if_enabled(provider, "0x" + "ab" * 32)
    provider.make_request.assert_not_called()


def test_fill_trace_capture_writes_the_control_artifact(tmp_path, monkeypatch) -> None:
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    monkeypatch.setenv(mod._TRACE_DIR_ENV, str(tmp_path))
    provider = MagicMock()
    provider.make_request.return_value = {"result": _fill_tree()}
    tx_hash = "0x" + "cd" * 32
    mod._capture_trace_artifact_if_enabled(provider, tx_hash)
    assert (tmp_path / f"{tx_hash}.calltrace.json").exists()


def test_trace_frame_and_block_timing_accept_decimal_ints(caplog: pytest.LogCaptureFixture) -> None:
    """PR #3602 review: tracer/provider layers may deliver ints, not hex strings.

    Feeding a decimal int through ``int(str(x), 16)`` logs a silently wrong
    wall delta; dropping an int ``gasUsed`` hides the one number a starved
    frame exists to show.
    """
    from almanak.connectors.gmx_v2 import anvil_order_executor as mod

    assert "gas_used=10659" in mod._describe_trace_frame({"depth": 4, "error": "out of gas", "gasUsed": 10_659})

    provider = MagicMock()
    with (
        patch.object(mod, "_rpc", return_value={"timestamp": 1_785_807_521}),
        caplog.at_level("INFO", logger="almanak.connectors.gmx_v2.anvil_order_executor"),
    ):
        mod._log_mined_block_timing(provider, "0x" + "ab" * 32, "order", 42)

    assert any("timestamp=1785807521" in record.getMessage() for record in caplog.records)

    # A timing-read failure must never mask the transaction outcome (it is
    # telemetry): the helper swallows the error and the caller proceeds.
    with patch.object(mod, "_rpc", side_effect=RuntimeError("rpc down")):
        mod._log_mined_block_timing(provider, "0x" + "ab" * 32, "order", 42)

    # Malformed block data (no timestamp) is likewise a silent no-op.
    with patch.object(mod, "_rpc", return_value={"number": "0x2a"}):
        mod._log_mined_block_timing(provider, "0x" + "ab" * 32, "order", 42)
