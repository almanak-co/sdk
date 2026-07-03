from unittest.mock import MagicMock

from web3 import Web3

from almanak.connectors.traderjoe_v2.sdk import TraderJoeV2SDK, _as_block_identifier

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


# ---------------------------------------------------------------------------
# VIB-5148 (Layer-2 follow-up to VIB-5140): block_identifier threading.
#
# Before this fix, ``get_position_balances`` / ``get_position_balances_for_ids``
# always called ``.call()`` with no block, so a post-tx teardown verify read
# "latest" regardless of what the caller pinned — a stale read-replica trailing
# the close-tx writer by a block could return PRE-close LB-token balances and
# false-negative a healthy TJ V2 LP teardown. These tests prove the
# ``block_identifier`` kwarg reaches every underlying ``.call()``, including
# ``getActiveId`` and the per-bin ``balanceOf`` fallback.
# ---------------------------------------------------------------------------


def test_get_position_balances_omitted_block_identifier_defaults_to_none() -> None:
    """Legacy behaviour preserved: omitting block_identifier reads "latest" (web3.py default)."""
    pair = MagicMock()
    pair.functions.getActiveId.return_value.call.return_value = 1000
    pair.functions.balanceOfBatch.return_value.call.return_value = [0, 7, 0, 9, 0]
    sdk = _sdk_with_pair(pair)

    sdk.get_position_balances(POOL, WALLET, bin_range=2)

    pair.functions.getActiveId.return_value.call.assert_called_once_with(block_identifier=None)
    pair.functions.balanceOfBatch.return_value.call.assert_called_once_with(block_identifier=None)


def test_get_position_balances_pinned_block_identifier_reaches_every_call() -> None:
    """A pinned block MUST reach getActiveId AND balanceOfBatch — same block, not "latest"."""
    pair = MagicMock()
    pair.functions.getActiveId.return_value.call.return_value = 1000
    pair.functions.balanceOfBatch.return_value.call.return_value = [0, 7, 0, 9, 0]
    sdk = _sdk_with_pair(pair)

    sdk.get_position_balances(POOL, WALLET, bin_range=2, block_identifier=19_000_000)

    pair.functions.getActiveId.return_value.call.assert_called_once_with(block_identifier=19_000_000)
    pair.functions.balanceOfBatch.return_value.call.assert_called_once_with(block_identifier=19_000_000)


def test_get_position_balances_pinned_block_identifier_reaches_per_bin_fallback() -> None:
    pair = MagicMock()
    pair.functions.getActiveId.return_value.call.return_value = 1000
    pair.functions.balanceOfBatch.return_value.call.side_effect = Exception("function not found")
    per_bin_values = {998: 0, 999: 4, 1000: 0, 1001: 6, 1002: 0}
    # Memoize per (wallet, bin) so the SAME MagicMock is returned on every call —
    # letting the assertions below inspect the exact mock the SDK invoked.
    per_bin_mocks: dict[tuple[str, int], MagicMock] = {}

    def _balance_of(w: str, b: int) -> MagicMock:
        key = (w, b)
        if key not in per_bin_mocks:
            m = MagicMock()
            m.call.return_value = per_bin_values.get(b, 0)
            per_bin_mocks[key] = m
        return per_bin_mocks[key]

    pair.functions.balanceOf.side_effect = _balance_of
    sdk = _sdk_with_pair(pair)

    sdk.get_position_balances(POOL, WALLET, bin_range=2, block_identifier=19_000_000)

    wallet = Web3.to_checksum_address(WALLET)
    assert len(per_bin_mocks) == 5
    for bin_id in (998, 999, 1000, 1001, 1002):
        per_bin_mocks[(wallet, bin_id)].call.assert_called_once_with(block_identifier=19_000_000)


def test_get_position_balances_for_ids_pinned_block_identifier_reaches_batch() -> None:
    pair = MagicMock()
    pair.functions.balanceOfBatch.return_value.call.return_value = [3, 0, 5]
    sdk = _sdk_with_pair(pair)

    sdk.get_position_balances_for_ids(POOL, WALLET, [42, 43, 44], block_identifier=19_000_000)

    pair.functions.balanceOfBatch.return_value.call.assert_called_once_with(block_identifier=19_000_000)


def test_get_position_balances_for_ids_omitted_block_identifier_defaults_to_none() -> None:
    pair = MagicMock()
    pair.functions.balanceOfBatch.return_value.call.return_value = [3, 0, 5]
    sdk = _sdk_with_pair(pair)

    sdk.get_position_balances_for_ids(POOL, WALLET, [42, 43, 44])

    pair.functions.balanceOfBatch.return_value.call.assert_called_once_with(block_identifier=None)


# ---------------------------------------------------------------------------
# _as_block_identifier normalisation (Gemini review, PR #3179): a decimal
# string block reference must be converted to int so web3.py hex-encodes it
# for the JSON-RPC eth_call — a raw decimal string would be rejected by the
# node (needs int / 0x-hex / tag).
# ---------------------------------------------------------------------------


def test_as_block_identifier_converts_decimal_string_to_int() -> None:
    assert _as_block_identifier("19000000") == 19_000_000
    assert isinstance(_as_block_identifier("19000000"), int)


def test_as_block_identifier_passes_through_int_none_tag_and_hex() -> None:
    assert _as_block_identifier(19_000_000) == 19_000_000
    assert _as_block_identifier(None) is None
    assert _as_block_identifier("latest") == "latest"
    # 0x-hex must NOT be coerced (isdigit() is False) — web3.py accepts it as-is.
    assert _as_block_identifier("0x121eac0") == "0x121eac0"
