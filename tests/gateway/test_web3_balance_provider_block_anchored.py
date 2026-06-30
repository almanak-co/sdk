"""VIB-3350: block-anchored balance reads on Web3BalanceProvider.

The regression gate for the production false-positive reconciliation incident
(see docs/internal/accounting/Reconcil.md): a swap that succeeded on-chain was flagged
because the post-execution read returned pre-tx "latest" state from a lagging
RPC/replica, so every token delta computed to 0. Block-anchored reads pin the
post-read to the confirmed receipt block, so the delta is correct.

Core test: ``test_latest_is_stale_but_pinned_is_correct`` — the unanchored
"latest" read returns the stale pre-tx balance while ``block=receipt_block``
returns the correct post-tx balance. That is the bug, and the fix.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from web3.exceptions import Web3Exception

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.gateway.data.balance.web3_provider import RPCError, Web3BalanceProvider
from almanak.gateway.utils.indexer_lag import INDEXER_LAG_ERROR_MARKERS, is_indexer_lag_error


def _lag_rpc_error() -> RPCError:
    """An RPCError whose inner cause is a receipt-indexer-lag ('Unknown block')."""
    return RPCError("balanceOf failed", method="balanceOf", original_error=Web3Exception("Unknown block"))


WALLET = "0x0000000000000000000000000000000000000001"
USDC_ADDR = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


def _resolver_for_usdc() -> MagicMock:
    resolver = MagicMock()
    resolver.resolve.return_value = ResolvedToken(
        symbol="USDC",
        address=USDC_ADDR,
        decimals=6,
        chain="arbitrum",
        chain_id=42161,
        is_native=False,
        is_wrapped_native=False,
        source="static",
    )
    return resolver


def _provider(**kwargs) -> Web3BalanceProvider:
    return Web3BalanceProvider(
        rpc_url="http://localhost:8545",
        wallet_address=WALLET,
        chain="arbitrum",
        token_resolver=_resolver_for_usdc(),
        retry_delay=0.0,
        **kwargs,
    )


@pytest.mark.parametrize("bad_max", [0, -1, -512])
def test_non_positive_block_cache_max_rejected(bad_max):
    """VIB-3350 (CodeRabbit): a non-positive block_cache_max would make the LRU
    eviction loop popitem() an empty cache -> KeyError on the first pinned write.
    Reject it loudly at construction instead."""
    with pytest.raises(ValueError, match="block_cache_max must be >= 1"):
        _provider(block_cache_max=bad_max)


@pytest.mark.asyncio
async def test_pinned_read_passes_block_to_rpc():
    """get_balance(block=N) threads block_identifier=N to the ERC-20 read."""
    provider = _provider()
    erc20 = AsyncMock(return_value=5_000_000)  # 5 USDC raw (6 decimals)
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=erc20):
        result = await provider.get_balance("USDC", block=21_000_000)

    assert result.balance == Decimal("5")
    # block was forwarded
    _, kwargs = erc20.call_args
    assert kwargs.get("block") == 21_000_000 or erc20.call_args.args[-1] == 21_000_000


@pytest.mark.asyncio
async def test_latest_is_stale_but_pinned_is_correct():
    """THE regression gate: latest returns stale pre-tx, pinned returns post-tx.

    Simulates the production failure: the underlying RPC serves the pre-tx
    balance for an unanchored "latest" read (lagging replica) but the correct
    post-tx balance when the read is pinned to the receipt block.
    """
    pre_tx_raw = 10_000_000  # 10 USDC (stale, pre-swap)
    post_tx_raw = 5_000_000  # 5 USDC (correct, post-swap: spent 5)
    receipt_block = 21_000_000

    fake_erc20 = AsyncMock(side_effect=lambda _addr, block=None: post_tx_raw if block == receipt_block else pre_tx_raw)

    provider = _provider()
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=fake_erc20):
        latest = await provider.get_balance("USDC")  # unanchored -> stale
        pinned = await provider.get_balance("USDC", block=receipt_block)  # anchored -> correct

    assert latest.balance == Decimal("10")  # the bug: stale pre-tx
    assert pinned.balance == Decimal("5")  # the fix: correct post-tx


@pytest.mark.asyncio
async def test_block_cache_never_crosses_latest_cache():
    """A 'latest' cache entry never satisfies a pinned request, nor the reverse."""
    receipt_block = 21_000_000
    calls: list[int | None] = []

    def _se(_addr, block=None):
        calls.append(block)
        return 5_000_000 if block == receipt_block else 10_000_000

    fake_erc20 = AsyncMock(side_effect=_se)
    provider = _provider()
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=fake_erc20):
        # 1) latest read populates the TTL cache
        await provider.get_balance("USDC")
        await provider.get_balance("USDC")  # served from latest cache -> no new RPC
        assert calls == [None]  # only one RPC so far

        # 2) pinned read must NOT be satisfied by the latest cache -> hits RPC
        await provider.get_balance("USDC", block=receipt_block)
        assert calls == [None, receipt_block]

        # 3) second pinned read IS served from the block cache -> no new RPC
        await provider.get_balance("USDC", block=receipt_block)
        assert calls == [None, receipt_block]

        # 4) a fresh latest read is still NOT served by the block cache
        provider.invalidate_cache("USDC")  # drop the latest entry
        await provider.get_balance("USDC")
        assert calls == [None, receipt_block, None]


@pytest.mark.asyncio
async def test_block_cache_is_lru_bounded():
    """Immutable pinned entries never TTL-expire, so the map is LRU-bounded."""
    provider = _provider(block_cache_max=2)
    fake_erc20 = AsyncMock(side_effect=lambda _addr, block=None: block or 0)

    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=fake_erc20):
        await provider.get_balance("USDC", block=100)
        await provider.get_balance("USDC", block=200)
        await provider.get_balance("USDC", block=300)  # evicts block=100 (LRU)

    assert len(provider._block_cache) == 2
    assert ("USDC", 100) not in provider._block_cache
    assert ("USDC", 200) in provider._block_cache
    assert ("USDC", 300) in provider._block_cache


@pytest.mark.asyncio
async def test_pinned_read_has_no_stale_fallback():
    """On RPC error a pinned read RAISES — never returns a stale 'latest'.

    Correctness over availability: a wrong-but-available reconciliation read is
    exactly the bug. A stale 'latest' entry in the TTL cache must not rescue a
    failed pinned read.
    """
    provider = _provider()

    # Seed the latest cache with a value.
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=AsyncMock(return_value=10_000_000)):
        await provider.get_balance("USDC")

    # Pinned read fails at the RPC layer.
    failing = AsyncMock(side_effect=RuntimeError("boom"))
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=failing):
        with pytest.raises(DataSourceUnavailable):
            await provider.get_balance("USDC", block=21_000_000)


@pytest.mark.asyncio
async def test_pinned_erc20_inner_breaks_on_lag_for_outer_budget():
    """VIB-3350 (audit I1): a PINNED ERC-20 read does NOT retry 'Unknown block'
    inside the inner loop — it breaks after one attempt and forwards the pinned
    block, delegating lag retries to the dedicated OUTER budget (so the total
    cost is block_lag_max_retries, not max_retries × block_lag_max_retries)."""
    receipt_block = 21_000_000
    provider = _provider(max_retries=3)

    call_mock = AsyncMock(side_effect=Web3Exception("Unknown block"))
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call = call_mock
    provider._w3 = MagicMock()
    provider._w3.eth.contract.return_value = contract

    with pytest.raises(RPCError):
        await provider._get_erc20_balance_with_retry(USDC_ADDR, block=receipt_block)

    assert call_mock.await_count == 1  # broke immediately; no inner lag retry
    assert call_mock.await_args_list[0].kwargs.get("block_identifier") == receipt_block


@pytest.mark.asyncio
async def test_pinned_persistent_lag_total_attempts_equals_outer_budget():
    """End-to-end (pr-auditor I1): under persistent lag a pinned read costs
    EXACTLY block_lag_max_retries RPC attempts — the inner loop does not multiply
    the outer budget. Resolves USDC (ERC-20) and drives the real inner loop."""
    provider = _provider(max_retries=3, block_lag_max_retries=2)
    call_mock = AsyncMock(side_effect=Web3Exception("Unknown block"))
    contract = MagicMock()
    contract.functions.balanceOf.return_value.call = call_mock
    provider._w3 = MagicMock()
    provider._w3.eth.contract.return_value = contract

    with pytest.raises(DataSourceUnavailable):
        await provider.get_balance("USDC", block=21_000_000)

    assert call_mock.await_count == 2  # == block_lag_max_retries, NOT 3*2
    assert provider._metrics.indexer_lag_retries == 2


@pytest.mark.asyncio
async def test_native_balance_pinned_read_forwards_block():
    """The native (eth_getBalance) path forwards block_identifier when pinned."""
    receipt_block = 21_000_000
    provider = _provider()
    get_balance = AsyncMock(return_value=7_000_000_000_000_000_000)  # 7 ETH in wei
    provider._w3 = MagicMock()
    provider._w3.eth.get_balance = get_balance

    raw = await provider._get_native_balance_with_retry(block=receipt_block)

    assert raw == 7_000_000_000_000_000_000
    _, kwargs = get_balance.call_args
    assert kwargs.get("block_identifier") == receipt_block


@pytest.mark.asyncio
async def test_native_unpinned_inner_retries_transient_lag():
    """For an UNPINNED native read, the inner loop still retries a transient
    error (no outer lag budget owns it), then succeeds."""
    provider = _provider(max_retries=3)
    get_balance = AsyncMock(side_effect=[Web3Exception("header not found"), 4_200])
    provider._w3 = MagicMock()
    provider._w3.eth.get_balance = get_balance

    raw = await provider._get_native_balance_with_retry()  # block=None -> unpinned

    assert raw == 4_200
    assert get_balance.await_count == 2


@pytest.mark.asyncio
async def test_native_pinned_inner_breaks_on_lag_for_outer_budget():
    """VIB-3350 (audit I1): a PINNED native read does NOT retry lag inside the
    inner loop — it breaks after one attempt so the dedicated OUTER lag budget
    owns the retries (no inner×outer amplification)."""
    receipt_block = 21_000_000
    provider = _provider(max_retries=3)
    get_balance = AsyncMock(side_effect=Web3Exception("header not found"))
    provider._w3 = MagicMock()
    provider._w3.eth.get_balance = get_balance

    with pytest.raises(RPCError):
        await provider._get_native_balance_with_retry(block=receipt_block)

    assert get_balance.await_count == 1  # broke immediately, no inner retry


@pytest.mark.asyncio
async def test_native_balance_latest_path_omits_block_identifier():
    """Unanchored native read does not pass block_identifier (latest)."""
    provider = _provider()
    get_balance = AsyncMock(return_value=1)
    provider._w3 = MagicMock()
    provider._w3.eth.get_balance = get_balance

    raw = await provider._get_native_balance_with_retry()

    assert raw == 1
    _, kwargs = get_balance.call_args
    assert "block_identifier" not in kwargs


# =============================================================================
# VIB-3350 Item 3: pinned-read lag retry aligned with gateway _is_indexer_lag_error
# =============================================================================


def test_shared_lag_classifier_matches_markers():
    """The block-pinned path classifies lag with the SAME shared marker set the
    gateway RpcService uses (one source of truth, no drift)."""
    for marker in INDEXER_LAG_ERROR_MARKERS:
        assert is_indexer_lag_error(marker.upper()) is True  # case-insensitive
    assert is_indexer_lag_error("Unknown block") is True
    # non-lag errors must fail fast, not be treated as transient
    assert is_indexer_lag_error("execution reverted") is False
    assert is_indexer_lag_error("invalid api key") is False
    assert is_indexer_lag_error(None) is False
    assert is_indexer_lag_error("") is False


def test_rpc_service_uses_shared_marker_set():
    """RpcService's class attr is the shared frozenset (delegation, not a copy)."""
    from almanak.gateway.services.rpc_service import RpcServiceServicer

    assert RpcServiceServicer._INDEXER_LAG_ERROR_MARKERS is INDEXER_LAG_ERROR_MARKERS
    assert RpcServiceServicer._is_indexer_lag_error("header not found") is True
    assert RpcServiceServicer._is_indexer_lag_error("execution reverted") is False


@pytest.mark.asyncio
async def test_pinned_read_lag_gets_dedicated_retry_then_succeeds():
    """A pinned read that hits indexer lag is retried on the dedicated budget,
    metered, and succeeds once the replica catches up."""
    provider = _provider(max_retries=1, block_lag_max_retries=3)
    inner = AsyncMock(side_effect=[_lag_rpc_error(), 5_000_000])
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=inner):
        result = await provider.get_balance("USDC", block=21_000_000)

    assert result.balance == Decimal("5")
    assert inner.await_count == 2  # one lag retry on the dedicated budget
    assert provider._metrics.indexer_lag_retries == 1


@pytest.mark.asyncio
async def test_pinned_read_non_lag_error_is_not_retried():
    """A non-lag RPC error must NOT consume the lag budget — fail fast."""
    provider = _provider(max_retries=1, block_lag_max_retries=3)
    non_lag = RPCError("boom", method="balanceOf", original_error=RuntimeError("connection reset"))
    inner = AsyncMock(side_effect=non_lag)
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=inner):
        with pytest.raises(DataSourceUnavailable):
            await provider.get_balance("USDC", block=21_000_000)

    assert inner.await_count == 1  # no lag retry
    assert provider._metrics.indexer_lag_retries == 0


@pytest.mark.asyncio
async def test_pinned_read_lag_budget_exhausts_then_raises():
    """Persistent lag exhausts the dedicated budget and fails closed (no stale)."""
    provider = _provider(max_retries=1, block_lag_max_retries=2)  # _provider sets retry_delay=0
    inner = AsyncMock(side_effect=[_lag_rpc_error(), _lag_rpc_error()])
    with patch.object(Web3BalanceProvider, "_get_erc20_balance_with_retry", new=inner):
        with pytest.raises(DataSourceUnavailable):
            await provider.get_balance("USDC", block=21_000_000)

    assert inner.await_count == 2  # exactly the lag budget
    assert provider._metrics.indexer_lag_retries == 2
