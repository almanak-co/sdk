"""Tests for KrakenAdapter action execution (_execute_swap / _execute_withdraw)."""

from unittest.mock import MagicMock

import pytest

from almanak.connectors.kraken.adapter import ActionType, CEXAction, ExecutionContext, KrakenAdapter
from almanak.connectors.kraken.models import CEXOperationType


def _adapter_with_sdk() -> tuple[KrakenAdapter, MagicMock]:
    sdk = MagicMock()
    sdk.swap.return_value = "TXID-123"
    sdk.withdraw.return_value = "REF-456"
    return KrakenAdapter(sdk=sdk), sdk


def _swap_action(**overrides) -> CEXAction:
    fields = {
        "id": "swap-1",
        "type": ActionType.CEX_SWAP,
        "exchange": "kraken",
        "asset_in": "USDC",
        "asset_out": "ETH",
        "amount_in": 1_000_000,
        "decimals_in": 6,
        "userref": 12345,
        "metadata": {"chain": "arbitrum"},
    }
    fields.update(overrides)
    return CEXAction(**fields)


def _withdraw_action(**overrides) -> CEXAction:
    fields = {
        "id": "withdraw-1",
        "type": ActionType.CEX_WITHDRAW,
        "exchange": "kraken",
        "asset": "USDC",
        "chain": "arbitrum",
        "amount": 1_000_000,
        "decimals": 6,
        "to_address": "0xabc",
    }
    fields.update(overrides)
    return CEXAction(**fields)


# =========================================================================
# Swap Execution
# =========================================================================


@pytest.mark.asyncio
async def test_execute_swap_returns_key_and_txid() -> None:
    adapter, sdk = _adapter_with_sdk()

    key, result_id = await adapter.execute_action(_swap_action(), ExecutionContext(chain="ethereum"))

    assert result_id == "TXID-123"
    assert key.order_id == "TXID-123"
    assert key.action_id == "swap-1"
    assert key.exchange == "kraken"
    assert key.operation_type == CEXOperationType.SWAP
    assert key.userref == 12345
    sdk.swap.assert_called_once_with(
        asset_in="USDC",
        asset_out="ETH",
        amount_in=1_000_000,
        decimals_in=6,
        userref=12345,
        chain="arbitrum",
    )


@pytest.mark.asyncio
async def test_execute_swap_falls_back_to_context_chain() -> None:
    adapter, sdk = _adapter_with_sdk()

    await adapter.execute_action(_swap_action(metadata={}), ExecutionContext(chain="ethereum"))

    assert sdk.swap.call_args.kwargs["chain"] == "ethereum"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"asset_in": None}, "asset_in and asset_out"),
        ({"asset_out": None}, "asset_in and asset_out"),
        ({"amount_in": None}, "amount_in and decimals_in"),
        ({"decimals_in": None}, "amount_in and decimals_in"),
        ({"userref": None}, "userref"),
    ],
)
async def test_execute_swap_missing_required_field_raises(overrides: dict, match: str) -> None:
    adapter, sdk = _adapter_with_sdk()

    with pytest.raises(ValueError, match=match):
        await adapter.execute_action(_swap_action(**overrides), ExecutionContext(chain="ethereum"))

    sdk.swap.assert_not_called()


# =========================================================================
# Withdrawal Execution
# =========================================================================


@pytest.mark.asyncio
async def test_execute_withdraw_returns_key_and_refid() -> None:
    adapter, sdk = _adapter_with_sdk()

    key, result_id = await adapter.execute_action(_withdraw_action(), ExecutionContext(chain="ethereum"))

    assert result_id == "REF-456"
    assert key.refid == "REF-456"
    assert key.action_id == "withdraw-1"
    assert key.exchange == "kraken"
    assert key.operation_type == CEXOperationType.WITHDRAW
    sdk.withdraw.assert_called_once_with(
        asset="USDC",
        chain="arbitrum",
        amount=1_000_000,
        decimals=6,
        to_address="0xabc",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"asset": None}, "asset and chain"),
        ({"chain": None}, "asset and chain"),
        ({"amount": None}, "amount and decimals"),
        ({"decimals": None}, "amount and decimals"),
        ({"to_address": None}, "to_address"),
    ],
)
async def test_execute_withdraw_missing_required_field_raises(overrides: dict, match: str) -> None:
    adapter, sdk = _adapter_with_sdk()

    with pytest.raises(ValueError, match=match):
        await adapter.execute_action(_withdraw_action(**overrides), ExecutionContext(chain="ethereum"))

    sdk.withdraw.assert_not_called()
