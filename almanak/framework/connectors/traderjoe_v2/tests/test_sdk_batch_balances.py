from unittest.mock import MagicMock

from web3 import Web3

from ..sdk import TraderJoeV2SDK

WALLET = "0x1234567890123456789012345678901234567890"
POOL = "0x2222222222222222222222222222222222222222"


def _sdk_with_pair(pair: MagicMock) -> TraderJoeV2SDK:
    sdk = TraderJoeV2SDK.__new__(TraderJoeV2SDK)
    sdk.get_pair_contract = MagicMock(return_value=pair)
    return sdk


def test_get_position_balances_uses_balance_of_batch() -> None:
    pair = MagicMock()
    pair.functions.getActiveId.return_value.call.return_value = 1000
    pair.functions.balanceOfBatch.return_value.call.return_value = [0, 7, 0, 9, 0]
    sdk = _sdk_with_pair(pair)

    result = sdk.get_position_balances(POOL, WALLET, bin_range=2)

    wallet = Web3.to_checksum_address(WALLET)
    pair.functions.balanceOfBatch.assert_called_once_with(
        [wallet, wallet, wallet, wallet, wallet],
        [998, 999, 1000, 1001, 1002],
    )
    pair.functions.balanceOf.assert_not_called()
    assert result == {999: 7, 1001: 9}


def test_get_position_balances_falls_back_to_per_bin_when_batch_fails() -> None:
    pair = MagicMock()
    pair.functions.getActiveId.return_value.call.return_value = 1000
    pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("function not found")
    # Per-bin: bin 998 → 0, bin 999 → 4, bin 1000 → 0, bin 1001 → 6, bin 1002 → 0
    per_bin_values = {998: 0, 999: 4, 1000: 0, 1001: 6, 1002: 0}
    pair.functions.balanceOf.side_effect = lambda w, b: MagicMock(**{"call.return_value": per_bin_values.get(b, 0)})
    sdk = _sdk_with_pair(pair)

    result = sdk.get_position_balances(POOL, WALLET, bin_range=2)

    pair.functions.balanceOfBatch.return_value.call.assert_called_once()
    assert pair.functions.balanceOf.call_count == 5
    assert result == {999: 4, 1001: 6}


def test_get_position_balances_for_ids_uses_balance_of_batch() -> None:
    pair = MagicMock()
    pair.functions.balanceOfBatch.return_value.call.return_value = [3, 0, 5]
    sdk = _sdk_with_pair(pair)

    result = sdk.get_position_balances_for_ids(POOL, WALLET, [42, 43, 44])

    wallet = Web3.to_checksum_address(WALLET)
    pair.functions.balanceOfBatch.assert_called_once_with([wallet, wallet, wallet], [42, 43, 44])
    pair.functions.balanceOf.assert_not_called()
    assert result == {42: 3, 44: 5}


def test_get_position_balances_for_ids_falls_back_to_per_bin_when_batch_fails() -> None:
    pair = MagicMock()
    pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("function not found")
    per_bin_values = {42: 3, 43: 0, 44: 5}
    pair.functions.balanceOf.side_effect = lambda w, b: MagicMock(**{"call.return_value": per_bin_values.get(b, 0)})
    sdk = _sdk_with_pair(pair)

    result = sdk.get_position_balances_for_ids(POOL, WALLET, [42, 43, 44])

    pair.functions.balanceOfBatch.return_value.call.assert_called_once()
    assert pair.functions.balanceOf.call_count == 3
    assert result == {42: 3, 44: 5}
