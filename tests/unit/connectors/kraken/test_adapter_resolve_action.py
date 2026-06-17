from unittest.mock import AsyncMock

import pytest

from almanak.connectors.kraken.adapter import ActionType, CEXAction, ExecutionContext, KrakenAdapter
from almanak.connectors.kraken.models import CEXIdempotencyKey, CEXOperationType


class _ResolverSpy:
    def __init__(self) -> None:
        self.resolve_swap = AsyncMock(return_value="swap-details")
        self.resolve_withdrawal = AsyncMock(return_value="withdraw-details")
        self.resolve_deposit = AsyncMock(return_value="deposit-details")


def _adapter_with_resolver() -> tuple[KrakenAdapter, _ResolverSpy]:
    adapter = KrakenAdapter()
    resolver = _ResolverSpy()
    adapter._receipt_resolver = resolver
    return adapter, resolver


def _swap_action(**overrides) -> CEXAction:
    fields = {
        "id": "swap-1",
        "type": ActionType.CEX_SWAP,
        "exchange": "kraken",
        "asset_in": "USDC",
        "asset_out": "ETH",
        "decimals_in": 6,
        "decimals_out": 18,
        "metadata": {"chain": "arbitrum"},
    }
    fields.update(overrides)
    return CEXAction(**fields)


def _swap_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "swap-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.SWAP,
        "order_id": "ORDER-1",
        "userref": 12345,
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


def _withdraw_action(**overrides) -> CEXAction:
    fields = {
        "id": "withdraw-1",
        "type": ActionType.CEX_WITHDRAW,
        "exchange": "kraken",
        "asset": "USDC",
        "chain": "arbitrum",
        "decimals": 6,
        "to_address": "0xabc",
        "amount": 1000,
    }
    fields.update(overrides)
    return CEXAction(**fields)


def _withdraw_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "withdraw-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.WITHDRAW,
        "refid": "REF-1",
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


def _deposit_action(**overrides) -> CEXAction:
    fields = {
        "id": "deposit-1",
        "type": ActionType.CEX_DEPOSIT,
        "exchange": "kraken",
        "asset": "USDC",
        "from_chain": "optimism",
        "tx_hash": "0xdeposit",
        "decimals": 6,
        "amount": 1000,
    }
    fields.update(overrides)
    return CEXAction(**fields)


def _deposit_key(**overrides) -> CEXIdempotencyKey:
    fields = {
        "action_id": "deposit-1",
        "exchange": "kraken",
        "operation_type": CEXOperationType.DEPOSIT,
        "order_id": "0xdeposit",
    }
    fields.update(overrides)
    return CEXIdempotencyKey(**fields)


@pytest.mark.asyncio
async def test_resolve_swap_forwards_exact_resolver_kwargs_with_metadata_chain() -> None:
    adapter, resolver = _adapter_with_resolver()
    action = _swap_action()
    key = _swap_key()

    result = await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))

    assert result == "swap-details"
    resolver.resolve_swap.assert_awaited_once_with(
        txid="ORDER-1",
        userref=12345,
        asset_in="USDC",
        asset_out="ETH",
        decimals_in=6,
        decimals_out=18,
        chain="arbitrum",
        idempotency_key=key,
    )


@pytest.mark.asyncio
async def test_resolve_swap_falls_back_to_context_chain() -> None:
    adapter, resolver = _adapter_with_resolver()
    action = _swap_action(metadata={})
    key = _swap_key()

    result = await adapter.resolve_action(action, key, ExecutionContext(chain="base"))

    assert result == "swap-details"
    assert resolver.resolve_swap.await_args.kwargs["chain"] == "base"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "key", "message"),
    [
        (
            _swap_action(),
            _swap_key(order_id=None),
            "Swap resolution requires order_id and userref on idempotency key",
        ),
        (_swap_action(asset_in=None), _swap_key(), "Swap resolution requires asset_in and asset_out"),
        (_swap_action(decimals_out=None), _swap_key(), "Swap resolution requires decimals_in and decimals_out"),
    ],
)
async def test_resolve_swap_validates_required_fields(
    action: CEXAction,
    key: CEXIdempotencyKey,
    message: str,
) -> None:
    adapter, _resolver = _adapter_with_resolver()

    with pytest.raises(ValueError, match=message):
        await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))


@pytest.mark.asyncio
async def test_resolve_withdrawal_forwards_exact_resolver_kwargs() -> None:
    adapter, resolver = _adapter_with_resolver()
    action = _withdraw_action()
    key = _withdraw_key()

    result = await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))

    assert result == "withdraw-details"
    resolver.resolve_withdrawal.assert_awaited_once_with(
        refid="REF-1",
        asset="USDC",
        chain="arbitrum",
        decimals=6,
        to_address="0xabc",
        amount=1000,
        idempotency_key=key,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "key", "message"),
    [
        (
            _withdraw_action(),
            _withdraw_key(refid=None),
            "Withdrawal resolution requires refid on idempotency key",
        ),
        (_withdraw_action(asset=None), _withdraw_key(), "Withdrawal resolution requires asset and chain"),
        (
            _withdraw_action(amount=None),
            _withdraw_key(),
            "Withdrawal resolution requires decimals, to_address, and amount",
        ),
    ],
)
async def test_resolve_withdrawal_validates_required_fields(
    action: CEXAction,
    key: CEXIdempotencyKey,
    message: str,
) -> None:
    adapter, _resolver = _adapter_with_resolver()

    with pytest.raises(ValueError, match=message):
        await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))


@pytest.mark.asyncio
async def test_resolve_deposit_forwards_exact_resolver_kwargs() -> None:
    adapter, resolver = _adapter_with_resolver()
    action = _deposit_action()
    key = _deposit_key()

    result = await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))

    assert result == "deposit-details"
    resolver.resolve_deposit.assert_awaited_once_with(
        tx_hash="0xdeposit",
        asset="USDC",
        chain="optimism",
        decimals=6,
        amount=1000,
        idempotency_key=key,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "key", "message"),
    [
        (
            _deposit_action(tx_hash=None),
            _deposit_key(),
            "Deposit resolution requires tx_hash, asset, and from_chain",
        ),
        (_deposit_action(amount=None), _deposit_key(), "Deposit resolution requires decimals and amount"),
    ],
)
async def test_resolve_deposit_validates_required_fields(
    action: CEXAction,
    key: CEXIdempotencyKey,
    message: str,
) -> None:
    adapter, _resolver = _adapter_with_resolver()

    with pytest.raises(ValueError, match=message):
        await adapter.resolve_action(action, key, ExecutionContext(chain="ethereum"))


@pytest.mark.asyncio
async def test_resolve_unknown_action_type_preserves_error_message() -> None:
    adapter, _resolver = _adapter_with_resolver()
    action = _swap_action(type="mystery")

    with pytest.raises(ValueError, match="Unknown action type: mystery"):
        await adapter.resolve_action(action, _swap_key(), ExecutionContext(chain="ethereum"))
