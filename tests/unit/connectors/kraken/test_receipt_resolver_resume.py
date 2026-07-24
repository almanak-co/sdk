"""Branch coverage for KrakenReceiptResolver crash recovery and polling.

Covers ``resume_operation`` for all three CEX operation types (swap,
withdraw, deposit) plus the resolve/poll helpers it delegates to and the
``ExecutionDetails`` (de)serialization round-trip — all against stubbed
``KrakenSDK`` methods; no network and no real sleeping.
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest
from pydantic import SecretStr

from almanak.connectors.kraken.exceptions import (
    KrakenOrderNotFoundError,
    KrakenTimeoutError,
)
from almanak.connectors.kraken.models import (
    CEXIdempotencyKey,
    CEXOperationType,
    KrakenConfig,
    KrakenCredentials,
)
from almanak.connectors.kraken.receipt_resolver import (
    ExecutionDetails,
    KrakenReceiptResolver,
    TokenAmount,
)
from almanak.connectors.kraken.sdk import KrakenSDK

ORDER_ID = "OABC12-DEF34-GHI56"
USERREF = 12345
REFID = "FTXXXX-YYYYY-ZZZZZ"
TX_HASH = "0xdeadbeef"

SWAP_RESULT = {
    "amount_in": 2 * 10**18,
    "amount_out": 5994 * 10**6,
    "fee": 6 * 10**6,
    "fee_asset": "USDC",
    "average_price": Decimal("3000"),
    "timestamp": 1700000000,
}


@pytest.fixture
def sdk() -> KrakenSDK:
    sdk = KrakenSDK(
        credentials=KrakenCredentials(api_key=SecretStr("key"), api_secret=SecretStr("secret"))
    )
    # The resolver only consumes the high-level status/result helpers; stub
    # them directly so no Kraken REST client is ever exercised.
    sdk.get_swap_status = Mock(return_value="success")
    sdk.get_swap_result = Mock(return_value=dict(SWAP_RESULT))
    sdk.get_withdrawal_status = Mock(return_value="success")
    sdk.get_withdrawal_tx_hash = Mock(return_value=TX_HASH)
    sdk.get_deposit_status = Mock(return_value="success")
    return sdk


@pytest.fixture
def resolver(sdk) -> KrakenReceiptResolver:
    return KrakenReceiptResolver(sdk)


@pytest.fixture
def zero_timeout_resolver(sdk) -> KrakenReceiptResolver:
    config = KrakenConfig()
    # Assign past the field minimums (no validate_assignment on the model):
    # a 0-second budget makes every poll loop time out without real waiting.
    config.order_timeout_seconds = 0
    config.withdrawal_timeout_seconds = 0
    config.deposit_timeout_seconds = 0
    return KrakenReceiptResolver(sdk, config)


@pytest.fixture
def instant_sleep(monkeypatch):
    real_sleep = asyncio.sleep

    async def _instant(_delay, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _swap_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "swap-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.SWAP,
        "order_id": ORDER_ID,
        "userref": USERREF,
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


def _withdraw_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "withdraw-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.WITHDRAW,
        "refid": REFID,
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


def _deposit_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "deposit-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.DEPOSIT,
        "order_id": TX_HASH,  # order_id doubles as the deposit tx hash
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


class TestExecutionDetailsSerialization:
    def test_roundtrip_preserves_all_fields(self):
        details = ExecutionDetails(
            success=True,
            venue="kraken",
            operation_type="swap",
            amounts_in=[TokenAmount("ETH", 10**18, 18)],
            amounts_out=[TokenAmount("USDC", 5 * 10**6, 6)],
            fees=[TokenAmount("USDC", 10**4, 6)],
            source_id=ORDER_ID,
            timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            cex_metadata={"userref": USERREF},
        )
        assert ExecutionDetails.from_dict(details.to_dict()) == details

    def test_to_dict_without_timestamp_serializes_none(self):
        details = ExecutionDetails(success=False, venue="kraken", operation_type="swap")
        assert details.to_dict()["timestamp"] is None

    def test_from_dict_applies_defaults(self):
        details = ExecutionDetails.from_dict(
            {
                "success": True,
                "venue": "kraken",
                "operation_type": "withdraw",
                "amounts_out": [{"token": "ETH", "amount": 5}],
            }
        )
        assert details.amounts_in == []
        assert details.amounts_out == [TokenAmount("ETH", 5, 18)]
        assert details.fees == []
        assert details.source_id == ""
        assert details.timestamp is None
        assert details.cex_metadata is None


class TestResolveSwap:
    pytestmark = pytest.mark.asyncio

    async def test_success_builds_full_details(self, resolver, sdk):
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.success is True
        assert details.venue == "kraken"
        assert details.operation_type == "swap"
        assert details.amounts_in == [TokenAmount("ETH", SWAP_RESULT["amount_in"], 18)]
        assert details.amounts_out == [TokenAmount("USDC", SWAP_RESULT["amount_out"], 6)]
        # Fee denominated in the output asset picks decimals_out.
        assert details.fees == [TokenAmount("USDC", SWAP_RESULT["fee"], 6)]
        assert details.source_id == ORDER_ID
        assert details.timestamp == datetime.fromtimestamp(SWAP_RESULT["timestamp"], tz=UTC)
        assert details.cex_metadata == {
            "userref": USERREF,
            "average_price": "3000",
            "status": "success",
        }
        sdk.get_swap_result.assert_called_once_with(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
            chain="ethereum",
        )

    async def test_fee_in_input_asset_uses_input_decimals(self, resolver, sdk):
        sdk.get_swap_result.return_value = dict(SWAP_RESULT, fee_asset="ETH", fee=10**15)
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.fees == [TokenAmount("ETH", 10**15, 18)]

    async def test_partial_fill_is_not_success(self, resolver, sdk):
        sdk.get_swap_status.return_value = "partial"
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.success is False
        assert details.cex_metadata["status"] == "partial"

    async def test_zero_result_timestamp_maps_to_none(self, resolver, sdk):
        sdk.get_swap_result.return_value = dict(SWAP_RESULT, timestamp=0)
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.timestamp is None

    @pytest.mark.parametrize("status", ["failed", "cancelled"])
    async def test_terminal_failure_skips_result_lookup(self, resolver, sdk, status):
        sdk.get_swap_status.return_value = status
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.success is False
        assert details.source_id == ORDER_ID
        assert details.cex_metadata == {"userref": USERREF, "status": status}
        sdk.get_swap_result.assert_not_called()

    async def test_unknown_status_raises_timeout(self, resolver, sdk):
        sdk.get_swap_status.return_value = "unknown"
        with pytest.raises(KrakenTimeoutError):
            await resolver.resolve_swap(
                txid=ORDER_ID,
                userref=USERREF,
                asset_in="ETH",
                asset_out="USDC",
                decimals_in=18,
                decimals_out=6,
            )

    async def test_still_pending_at_timeout_raises(self, zero_timeout_resolver, sdk):
        with pytest.raises(KrakenTimeoutError):
            await zero_timeout_resolver.resolve_swap(
                txid=ORDER_ID,
                userref=USERREF,
                asset_in="ETH",
                asset_out="USDC",
                decimals_in=18,
                decimals_out=6,
            )
        sdk.get_swap_status.assert_not_called()

    async def test_poll_backs_off_and_updates_idempotency_key(
        self, resolver, sdk, instant_sleep
    ):
        sdk.get_swap_status.side_effect = ["pending", "success"]
        key = _swap_key()
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
            idempotency_key=key,
        )
        assert details.success is True
        assert sdk.get_swap_status.call_count == 2
        assert key.last_poll is not None

    async def test_poll_survives_order_not_found(self, resolver, sdk, instant_sleep):
        sdk.get_swap_status.side_effect = [KrakenOrderNotFoundError(ORDER_ID), "success"]
        details = await resolver.resolve_swap(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
        )
        assert details.success is True
        assert sdk.get_swap_status.call_count == 2


class TestResolveWithdrawal:
    pytestmark = pytest.mark.asyncio

    async def test_success_uses_tx_hash_as_source(self, resolver, sdk):
        details = await resolver.resolve_withdrawal(
            refid=REFID,
            asset="ETH",
            chain="arbitrum",
            decimals=18,
            to_address="0xabc",
            amount=10**18,
        )
        assert details.success is True
        assert details.operation_type == "withdraw"
        assert details.amounts_out == [TokenAmount("ETH", 10**18, 18)]
        assert details.source_id == TX_HASH
        assert details.cex_metadata == {
            "refid": REFID,
            "chain": "arbitrum",
            "to_address": "0xabc",
            "tx_hash": TX_HASH,
            "status": "success",
        }

    async def test_failed_without_tx_hash_falls_back_to_refid(self, resolver, sdk):
        sdk.get_withdrawal_status.return_value = "failed"
        sdk.get_withdrawal_tx_hash.return_value = None
        details = await resolver.resolve_withdrawal(
            refid=REFID,
            asset="ETH",
            chain="arbitrum",
            decimals=18,
            to_address="0xabc",
            amount=10**18,
        )
        assert details.success is False
        assert details.source_id == REFID
        assert details.cex_metadata["tx_hash"] is None

    async def test_still_pending_at_timeout_raises(self, zero_timeout_resolver, sdk):
        with pytest.raises(KrakenTimeoutError):
            await zero_timeout_resolver.resolve_withdrawal(
                refid=REFID,
                asset="ETH",
                chain="arbitrum",
                decimals=18,
                to_address="0xabc",
                amount=10**18,
            )
        sdk.get_withdrawal_status.assert_not_called()

    async def test_poll_none_then_success_updates_key(self, resolver, sdk, instant_sleep):
        sdk.get_withdrawal_status.side_effect = [None, "pending", "success"]
        key = _withdraw_key()
        details = await resolver.resolve_withdrawal(
            refid=REFID,
            asset="ETH",
            chain="arbitrum",
            decimals=18,
            to_address="0xabc",
            amount=10**18,
            idempotency_key=key,
        )
        assert details.success is True
        assert sdk.get_withdrawal_status.call_count == 3
        assert key.last_poll is not None


class TestResolveDeposit:
    pytestmark = pytest.mark.asyncio

    @pytest.mark.parametrize(
        ("status", "expected_success"),
        [("success", True), ("failed", False)],
    )
    async def test_terminal_status_builds_details(
        self, resolver, sdk, status, expected_success
    ):
        sdk.get_deposit_status.return_value = status
        details = await resolver.resolve_deposit(
            tx_hash=TX_HASH,
            asset="USDC",
            chain="arbitrum",
            decimals=6,
            amount=1000 * 10**6,
        )
        assert details.success is expected_success
        assert details.operation_type == "deposit"
        assert details.amounts_in == [TokenAmount("USDC", 1000 * 10**6, 6)]
        assert details.source_id == TX_HASH
        assert details.cex_metadata == {
            "tx_hash": TX_HASH,
            "chain": "arbitrum",
            "status": status,
        }

    async def test_still_pending_at_timeout_raises(self, zero_timeout_resolver, sdk):
        with pytest.raises(KrakenTimeoutError):
            await zero_timeout_resolver.resolve_deposit(
                tx_hash=TX_HASH,
                asset="USDC",
                chain="arbitrum",
                decimals=6,
                amount=1000 * 10**6,
            )
        sdk.get_deposit_status.assert_not_called()

    async def test_poll_pending_then_success_updates_key(self, resolver, sdk, instant_sleep):
        sdk.get_deposit_status.side_effect = [None, "pending", "success"]
        key = _deposit_key()
        details = await resolver.resolve_deposit(
            tx_hash=TX_HASH,
            asset="USDC",
            chain="arbitrum",
            decimals=6,
            amount=1000 * 10**6,
            idempotency_key=key,
        )
        assert details.success is True
        assert sdk.get_deposit_status.call_count == 3
        assert key.last_poll is not None


class TestResumeSwap:
    pytestmark = pytest.mark.asyncio

    @pytest.mark.parametrize(
        "overrides",
        [{"order_id": None}, {"userref": None}],
        ids=["missing-order-id", "missing-userref"],
    )
    async def test_missing_identifiers_return_none(self, resolver, sdk, overrides):
        assert await resolver.resume_operation(_swap_key(**overrides)) is None
        sdk.get_swap_status.assert_not_called()

    async def test_pending_returns_none(self, resolver, sdk):
        sdk.get_swap_status.return_value = "pending"
        assert await resolver.resume_operation(_swap_key()) is None
        sdk.get_swap_result.assert_not_called()

    async def test_success_delegates_to_resolve_swap_with_context(self, resolver, sdk):
        key = _swap_key()
        details = await resolver.resume_operation(
            key,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
            chain="arbitrum",
        )
        assert details.success is True
        assert details.amounts_in == [TokenAmount("ETH", SWAP_RESULT["amount_in"], 18)]
        assert details.amounts_out == [TokenAmount("USDC", SWAP_RESULT["amount_out"], 6)]
        # First check in resume_operation, second inside the resolve_swap poll.
        assert sdk.get_swap_status.call_count == 2
        assert key.last_poll is not None
        sdk.get_swap_result.assert_called_once_with(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="ETH",
            asset_out="USDC",
            decimals_in=18,
            decimals_out=6,
            chain="arbitrum",
        )

    async def test_success_without_context_uses_defaults(self, resolver, sdk):
        details = await resolver.resume_operation(_swap_key())
        assert details.success is True
        sdk.get_swap_result.assert_called_once_with(
            txid=ORDER_ID,
            userref=USERREF,
            asset_in="",
            asset_out="",
            decimals_in=18,
            decimals_out=18,
            chain="ethereum",
        )

    async def test_partial_resolves_with_success_false(self, resolver, sdk):
        sdk.get_swap_status.return_value = "partial"
        details = await resolver.resume_operation(_swap_key(), asset_in="ETH", asset_out="USDC")
        assert details.success is False
        assert details.cex_metadata["status"] == "partial"

    @pytest.mark.parametrize("status", ["failed", "cancelled"])
    async def test_terminal_failure_returns_failure_details(self, resolver, sdk, status):
        sdk.get_swap_status.return_value = status
        details = await resolver.resume_operation(_swap_key())
        assert details.success is False
        assert details.operation_type == "swap"
        assert details.source_id == ORDER_ID
        assert details.cex_metadata == {"userref": USERREF, "status": status}
        sdk.get_swap_result.assert_not_called()


class TestResumeWithdraw:
    pytestmark = pytest.mark.asyncio

    async def test_missing_refid_returns_none(self, resolver, sdk):
        assert await resolver.resume_operation(_withdraw_key(refid=None)) is None
        sdk.get_withdrawal_status.assert_not_called()

    @pytest.mark.parametrize("status", [None, "pending"])
    async def test_unresolved_status_returns_none(self, resolver, sdk, status):
        sdk.get_withdrawal_status.return_value = status
        assert await resolver.resume_operation(_withdraw_key()) is None
        sdk.get_withdrawal_tx_hash.assert_not_called()

    async def test_success_uses_tx_hash_and_context(self, resolver, sdk):
        details = await resolver.resume_operation(
            _withdraw_key(), asset="ETH", chain="arbitrum"
        )
        assert details.success is True
        assert details.operation_type == "withdraw"
        assert details.source_id == TX_HASH
        assert details.cex_metadata == {
            "refid": REFID,
            "status": "success",
            "tx_hash": TX_HASH,
        }
        sdk.get_withdrawal_status.assert_called_once_with("ETH", "arbitrum", refid=REFID)
        sdk.get_withdrawal_tx_hash.assert_called_once_with("ETH", "arbitrum", REFID)

    async def test_failed_without_tx_hash_falls_back_to_refid(self, resolver, sdk):
        sdk.get_withdrawal_status.return_value = "failed"
        sdk.get_withdrawal_tx_hash.return_value = None
        details = await resolver.resume_operation(_withdraw_key())
        assert details.success is False
        assert details.source_id == REFID
        # Context defaults to empty asset/chain when not supplied.
        sdk.get_withdrawal_status.assert_called_once_with("", "", refid=REFID)


class TestResumeDeposit:
    pytestmark = pytest.mark.asyncio

    async def test_missing_tx_hash_returns_none(self, resolver, sdk):
        assert await resolver.resume_operation(_deposit_key(order_id=None)) is None
        sdk.get_deposit_status.assert_not_called()

    @pytest.mark.parametrize("status", [None, "pending"])
    async def test_unresolved_status_returns_none(self, resolver, sdk, status):
        sdk.get_deposit_status.return_value = status
        assert await resolver.resume_operation(_deposit_key()) is None

    @pytest.mark.parametrize(
        ("status", "expected_success"),
        [("success", True), ("failed", False)],
    )
    async def test_terminal_status_builds_details(
        self, resolver, sdk, status, expected_success
    ):
        sdk.get_deposit_status.return_value = status
        details = await resolver.resume_operation(
            _deposit_key(), asset="USDC", chain="arbitrum"
        )
        assert details.success is expected_success
        assert details.operation_type == "deposit"
        assert details.source_id == TX_HASH
        assert details.cex_metadata == {"tx_hash": TX_HASH, "status": status}
        sdk.get_deposit_status.assert_called_once_with(TX_HASH, "USDC", "arbitrum")


class TestResumeUnknownOperation:
    pytestmark = pytest.mark.asyncio

    async def test_unrecognized_operation_type_returns_none(self, resolver, sdk):
        key = CEXIdempotencyKey(
            action_id="other-1",
            exchange="kraken",
            operation_type="transfer",  # not a CEXOperationType member
        )
        assert await resolver.resume_operation(key) is None
        sdk.get_swap_status.assert_not_called()
        sdk.get_withdrawal_status.assert_not_called()
        sdk.get_deposit_status.assert_not_called()
