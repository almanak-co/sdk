"""Unit tests for the VIB-3699 refactor of ``PolymarketServiceServicer``.

Covers four pieces of behavior introduced by the refactor:

  1. ``_ensure_wallet_ready`` SELL short-circuit — must not query the pUSD
     balance when ``min_pusd_units == 0``.
  2. BUY-side pUSD balance cache — first call reads on-chain; subsequent calls
     with a cached balance ≥ ``min_pusd_units`` skip the read; insufficient
     cache triggers a wrap and updates the cache to ``cached + wrap``.
  3. Chain-id assertion in ``_sign_and_submit_setup_tx`` — refuses to sign
     against any chain other than Polygon mainnet (137), unless the gateway
     is wired up against an Anvil polygon fork.
  4. EIP-1559 gas pricing — ``_build_eip1559_gas_fields`` emits
     ``maxFeePerGas`` / ``maxPriorityFeePerGas`` when the latest block has a
     ``baseFeePerGas`` and falls back to legacy ``gasPrice`` otherwise.

Plus: the cached-Web3 helper integration — confirms the servicer routes
``_get_polygon_web3`` through ``get_cached_web3`` instead of constructing a
fresh ``HTTPProvider`` per call.

Existing wallet-ready coverage lives in ``test_polymarket_service_setup.py``
(idempotency, lock coalescing, source-asset shortfall). Per the ticket's
"extend if it exists" rubric: there is no prior file at this path, so this
file is the dedicated home for the VIB-3699 deltas.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from eth_account import Account

from almanak.framework.connectors.polymarket import TransactionData
from almanak.gateway.core.settings import GatewaySettings
from almanak.connectors.polymarket.gateway.service import (
    POLYGON_MAINNET_CHAIN_ID,
    POLYGON_MIN_PRIORITY_FEE_WEI,
    PUSD_CACHE_STALE_BLOCKS,
    PolymarketServiceServicer,
)

# Deterministic Anvil-style key — never funded, never used in production.
TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_ACCOUNT = Account.from_key(TEST_PRIVATE_KEY)
TEST_WALLET = TEST_ACCOUNT.address


@pytest.fixture
def settings() -> MagicMock:
    s = MagicMock(spec=GatewaySettings)
    s.private_key = TEST_PRIVATE_KEY
    s.polymarket_private_key = None
    s.eoa_address = TEST_WALLET
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    s.polymarket_api_key = "k"
    s.polymarket_secret = "c2VjcmV0"  # base64("secret")
    s.polymarket_passphrase = "p"
    s.polymarket_network = "mainnet"
    return s


@pytest.fixture
def servicer(settings: MagicMock) -> PolymarketServiceServicer:
    return PolymarketServiceServicer(settings=settings)


class _CtfStub:
    """Minimal CTF stand-in. Tracks call counts on ``get_pusd_balance`` so the
    pUSD-cache tests can assert on-chain reads happen only when expected."""

    def __init__(
        self,
        *,
        approval_txs: list[TransactionData] | None = None,
        pusd_balance_seq: list[int] | None = None,
        source_balance: int = 10_000_000_000,
        native_usdc_balance: int = 0,
        source_asset: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        native_usdc: str = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        collateral_onramp: str = "0x93070a847efEf7F70739046A929D47a521F5B8ee",
    ) -> None:
        self._approval_txs = approval_txs or []
        self._pusd_balance_seq = list(pusd_balance_seq or [0])
        self._source_balance = source_balance
        self._native_usdc_balance = native_usdc_balance
        self.source_asset = source_asset
        self.native_usdc = native_usdc
        self.collateral_onramp = collateral_onramp
        self.get_pusd_balance_calls = 0
        # VIB-3770: wrap_calls tracks (amount, source) pairs.
        self.wrap_calls: list[int] = []
        self.ensure_allowances_calls = 0
        self._approve_calls: list[tuple[str, str]] = []

    def ensure_allowances(self, _wallet: str, _web3) -> list[TransactionData]:
        self.ensure_allowances_calls += 1
        return list(self._approval_txs)

    def get_pusd_balance(self, _wallet: str, _web3) -> int:
        self.get_pusd_balance_calls += 1
        # Pop one balance per call when multiple are queued; otherwise sticky.
        if len(self._pusd_balance_seq) > 1:
            return self._pusd_balance_seq.pop(0)
        return self._pusd_balance_seq[0]

    def get_source_asset_balance(self, _wallet: str, _web3) -> int:
        return self._source_balance

    def check_allowances(self, _wallet: str, _web3):  # noqa: ANN201
        from almanak.framework.connectors.polymarket.ctf_sdk import (
            MAX_UINT256,
            AllowanceStatus,
        )

        return AllowanceStatus(
            source_asset_balance=self._source_balance,
            pusd_balance=0,
            source_asset_allowance_onramp=MAX_UINT256,
            pusd_allowance_ctf_exchange=MAX_UINT256,
            pusd_allowance_neg_risk_exchange=MAX_UINT256,
            pusd_allowance_neg_risk_adapter=MAX_UINT256,
            ctf_approved_for_ctf_exchange=True,
            ctf_approved_for_neg_risk_adapter=True,
            native_usdc_balance=self._native_usdc_balance,
            native_usdc_allowance_onramp=MAX_UINT256 if self._native_usdc_balance > 0 else 0,
        )

    def select_source_for_wrap(self, deficit: int, status) -> str:
        if status.source_asset_balance >= deficit:
            return self.source_asset
        if status.native_usdc_balance >= deficit:
            return self.native_usdc
        if status.native_usdc_balance > status.source_asset_balance:
            return self.native_usdc
        return self.source_asset

    def build_approve_collateral_tx(self, asset: str, spender: str, sender: str) -> TransactionData:  # noqa: ARG002
        self._approve_calls.append((asset, spender))
        return TransactionData(to=asset, data="0x", gas_estimate=80_000, description="approve")

    def build_wrap_to_pusd_tx(
        self,
        _wallet: str,
        amount: int,
        source_asset: str | None = None,  # noqa: ARG002
    ) -> TransactionData:
        self.wrap_calls.append(amount)
        return TransactionData(
            to="0xWrap",
            data="0x",
            gas_estimate=150_000,
            description=f"wrap {amount}",
        )


def _fake_web3(*, block_number: int = 1_000_000) -> MagicMock:
    """A web3 stand-in whose ``eth.block_number`` returns a real int.

    The default ``MagicMock()`` returns nested mocks for attribute access,
    which our cache-staleness arithmetic ``int(raw_block)`` rejects (and
    correctly falls back to a fresh read). Tests that exercise the cache
    path need block_number to be a real int so the staleness check passes.
    """
    web3 = MagicMock()
    web3.eth.block_number = block_number
    return web3


# =============================================================================
# (a) SELL orders skip the pUSD balance check
# =============================================================================


class TestSellShortCircuit:
    """SELL orders compute ``min_pusd_units == 0``: no pUSD on the maker side,
    no need to read its balance, no wrap. The refactor adds an early return
    after allowances so we save one ERC20 ``balanceOf`` call per SELL."""

    @pytest.mark.asyncio
    async def test_sell_does_not_call_get_pusd_balance(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[0])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=0)

        assert ctf.get_pusd_balance_calls == 0
        assert ctf.wrap_calls == []
        # Allowances pass still ran (even SELLs need CTF→exchange approvals).
        assert ctf.ensure_allowances_calls == 1

    @pytest.mark.asyncio
    async def test_sell_skips_pusd_after_first_buy(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """A SELL after a BUY uses no pUSD lookups even though a cached value
        exists — the refactor short-circuits BEFORE the cache lookup, not
        after, so the cache state is untouched on SELL."""
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[10_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        # BUY first: warms the cache.
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert ctf.get_pusd_balance_calls == 1
        assert servicer._cached_pusd_balance == 10_000_000

        # SELL: short-circuit; no extra pUSD call.
        await servicer._ensure_wallet_ready(min_pusd_units=0)
        assert ctf.get_pusd_balance_calls == 1


# =============================================================================
# (b, c) BUY-side pUSD balance cache
# =============================================================================


class TestPusdBalanceCache:
    """The cache turns repeat BUYs into a hot path: zero RPCs after the first
    successful order, until the cache goes stale or runs short."""

    @pytest.mark.asyncio
    async def test_first_buy_reads_pusd_second_buy_uses_cache(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """First BUY: reads pUSD on-chain once. Second BUY with a cached
        balance ≥ min_pusd_units: zero pUSD reads."""
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[20_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        # First BUY needs 5 pUSD; we hold 20.
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert ctf.get_pusd_balance_calls == 1
        assert servicer._cached_pusd_balance == 20_000_000

        # Second BUY for 5 pUSD: cache covers, no on-chain read.
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert ctf.get_pusd_balance_calls == 1
        # Wrap never triggered.
        assert ctf.wrap_calls == []

    @pytest.mark.asyncio
    async def test_buy_with_insufficient_cache_wraps_and_updates_cache(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Cache held 2 pUSD; need 5; wrap deficit of 3; cache becomes 5
        (2 + 3). A subsequent BUY for 5 pUSD then hits the new cached value
        without re-reading."""
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[2_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        assert ctf.wrap_calls == [3_000_000]  # exact deficit
        assert ctf.get_pusd_balance_calls == 1
        # Cache updated optimistically post-wrap.
        assert servicer._cached_pusd_balance == 5_000_000

        # Next BUY for 5 pUSD: cache covers, no extra read, no extra wrap.
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert ctf.get_pusd_balance_calls == 1
        assert ctf.wrap_calls == [3_000_000]

    @pytest.mark.asyncio
    async def test_cache_stale_block_window_triggers_re_read(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """When the cache is older than ``PUSD_CACHE_STALE_BLOCKS`` we re-read,
        even if the cached value would otherwise cover the order. Catches
        outside transfers / consumption from a different process."""
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[20_000_000, 25_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3(block_number=1_000_000)
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        assert ctf.get_pusd_balance_calls == 1
        assert servicer._cached_pusd_balance_block == 1_000_000

        # Advance well past the staleness window.
        servicer._polygon_web3 = _fake_web3(
            block_number=1_000_000 + PUSD_CACHE_STALE_BLOCKS + 1
        )
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        # Re-read kicked in: balance now reflects the second pop in the seq.
        assert ctf.get_pusd_balance_calls == 2
        assert servicer._cached_pusd_balance == 25_000_000

    @pytest.mark.asyncio
    async def test_cache_below_min_pusd_triggers_re_read(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """If the cached value can't cover the new order's ``min_pusd_units``,
        re-read before deciding to wrap. The cached value might be stale and
        an outside transfer could have already covered the gap."""
        ctf = _CtfStub(approval_txs=[], pusd_balance_seq=[3_000_000, 100_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = _fake_web3(block_number=2_000)
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        # First BUY for 2 pUSD — cache primed at 3.
        await servicer._ensure_wallet_ready(min_pusd_units=2_000_000)
        assert ctf.get_pusd_balance_calls == 1
        assert ctf.wrap_calls == []

        # Next BUY for 50 pUSD: cache (3) < min (50) → re-read.
        await servicer._ensure_wallet_ready(min_pusd_units=50_000_000)
        assert ctf.get_pusd_balance_calls == 2
        # Re-read returned 100, which covers — no wrap needed.
        assert ctf.wrap_calls == []
        assert servicer._cached_pusd_balance == 100_000_000


# =============================================================================
# (d, e) Chain-id assertion
# =============================================================================


class TestChainIdAssertion:
    """The chain-id check guards the signing path against silent wrong-chain
    sends — a generic ``RPC_URL`` pointing at Arbitrum would otherwise be
    used for setup txs against contracts that don't exist there."""

    @pytest.mark.asyncio
    async def test_raises_when_chain_id_is_not_polygon(self, servicer: PolymarketServiceServicer) -> None:
        """Force the assertion path: real polygon RPC URL (mainnet), but the
        node reports the wrong chain-id."""
        # Pretend we're on a real polygon mainnet RPC (not anvil, not localhost).
        servicer.settings.polymarket_network = "mainnet"
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="https://polygon-rpc.example.com",
        ):
            web3 = MagicMock()
            # Wrong chain — Arbitrum, not Polygon.
            web3.eth.chain_id = 42161

            with pytest.raises(ValueError, match="expected polygon mainnet"):
                await servicer._assert_polygon_chain_id(web3)

        # Verification flag must NOT be set after a mismatch — a future call
        # with a correctly configured RPC should re-check.
        assert servicer._chain_id_verified is False

    @pytest.mark.asyncio
    async def test_passes_when_chain_id_is_polygon_mainnet(self, servicer: PolymarketServiceServicer) -> None:
        servicer.settings.polymarket_network = "mainnet"
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="https://polygon-rpc.example.com",
        ):
            web3 = MagicMock()
            web3.eth.chain_id = POLYGON_MAINNET_CHAIN_ID

            await servicer._assert_polygon_chain_id(web3)

        assert servicer._chain_id_verified is True

    @pytest.mark.asyncio
    async def test_chain_id_is_cached_after_first_check(self, servicer: PolymarketServiceServicer) -> None:
        """A second call must NOT touch the RPC — the verification flag short-
        circuits. This keeps high-frequency setup paths from paying an
        ``eth_chainId`` round-trip per send."""
        servicer.settings.polymarket_network = "mainnet"
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="https://polygon-rpc.example.com",
        ):
            web3 = MagicMock()
            web3.eth.chain_id = POLYGON_MAINNET_CHAIN_ID

            await servicer._assert_polygon_chain_id(web3)
            await servicer._assert_polygon_chain_id(web3)
            await servicer._assert_polygon_chain_id(web3)

        # ``chain_id`` is a property — accessing it on a MagicMock counts as
        # one access regardless of how many times we read it. Use a sentinel
        # to assert exactly-once read instead.
        web3 = MagicMock()
        servicer2 = type(servicer)(settings=servicer.settings)
        chain_id_reads: list[int] = []

        class _OneShot:
            def __get__(self, _obj, _cls):
                chain_id_reads.append(1)
                return POLYGON_MAINNET_CHAIN_ID

        type(web3.eth).chain_id = _OneShot()  # type: ignore[misc]
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="https://polygon-rpc.example.com",
        ):
            await servicer2._assert_polygon_chain_id(web3)
            await servicer2._assert_polygon_chain_id(web3)

        assert len(chain_id_reads) == 1

    @pytest.mark.asyncio
    async def test_anvil_via_env_skips_chain_id_check(self, servicer: PolymarketServiceServicer) -> None:
        """``ALMANAK_POLYMARKET_NETWORK=anvil`` exempts the call from the
        chain-id check — Anvil forks default to chain 31337 unless launched
        with ``--chain-id 137``, and we don't want to require that flag."""
        servicer.settings.polymarket_network = "anvil"
        web3 = MagicMock()
        web3.eth.chain_id = 31337  # Default Anvil chain id

        await servicer._assert_polygon_chain_id(web3)

        assert servicer._chain_id_verified is True

    @pytest.mark.asyncio
    async def test_anvil_via_localhost_url_skips_chain_id_check(self, servicer: PolymarketServiceServicer) -> None:
        """Localhost RPC URL → treated as Anvil even without the env var."""
        servicer.settings.polymarket_network = "mainnet"
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="http://127.0.0.1:8545",
        ):
            web3 = MagicMock()
            web3.eth.chain_id = 31337

            await servicer._assert_polygon_chain_id(web3)

        assert servicer._chain_id_verified is True


# =============================================================================
# (f) EIP-1559 gas pricing
# =============================================================================


class TestEip1559GasFields:
    """The setup tx now uses EIP-1559 (``maxFeePerGas`` /
    ``maxPriorityFeePerGas``) when the chain supports it. Polygon enforces a
    30 gwei minimum priority fee at the validator layer — we never go below
    that floor even if the node estimates lower."""

    @pytest.mark.asyncio
    async def test_eip1559_fields_when_base_fee_present(self) -> None:
        web3 = MagicMock()
        # Latest block carries baseFeePerGas → London-enabled chain.
        web3.eth.get_block = MagicMock(return_value={"baseFeePerGas": 100 * 10**9})  # 100 gwei
        web3.eth.max_priority_fee = 50 * 10**9  # 50 gwei (above the 30 gwei floor)

        gas_fields = await PolymarketServiceServicer._build_eip1559_gas_fields(web3)

        # No legacy ``gasPrice`` key.
        assert "gasPrice" not in gas_fields
        # Priority is the node's estimate (50 gwei, above the floor).
        assert gas_fields["maxPriorityFeePerGas"] == 50 * 10**9
        # 2*baseFee + priority = 250 gwei.
        assert gas_fields["maxFeePerGas"] == 2 * 100 * 10**9 + 50 * 10**9

    @pytest.mark.asyncio
    async def test_priority_floored_to_polygon_minimum(self) -> None:
        """A node returning a too-low priority estimate must be clamped to the
        30 gwei Polygon floor — otherwise validators silently drop the tx."""
        web3 = MagicMock()
        web3.eth.get_block = MagicMock(return_value={"baseFeePerGas": 50 * 10**9})
        # Node estimate WAY below Polygon's 30 gwei minimum.
        web3.eth.max_priority_fee = 1 * 10**9  # 1 gwei

        gas_fields = await PolymarketServiceServicer._build_eip1559_gas_fields(web3)

        assert gas_fields["maxPriorityFeePerGas"] == POLYGON_MIN_PRIORITY_FEE_WEI

    @pytest.mark.asyncio
    async def test_max_priority_fee_rpc_failure_uses_floor(self) -> None:
        """Some RPC backends raise on ``eth_maxPriorityFeePerGas``. Catch and
        use the Polygon floor — never let a missing helper kill setup."""
        web3 = MagicMock()
        web3.eth.get_block = MagicMock(return_value={"baseFeePerGas": 80 * 10**9})

        # Make the property access raise.
        type(web3.eth).max_priority_fee = property(  # type: ignore[misc]
            lambda _self: (_ for _ in ()).throw(RuntimeError("not supported"))
        )

        gas_fields = await PolymarketServiceServicer._build_eip1559_gas_fields(web3)

        assert gas_fields["maxPriorityFeePerGas"] == POLYGON_MIN_PRIORITY_FEE_WEI

    @pytest.mark.asyncio
    async def test_legacy_gas_price_when_base_fee_missing(self) -> None:
        """Anvil-without-EIP-1559 / pre-London chains have no baseFeePerGas in
        the latest block. The helper must fall back to legacy ``gasPrice``."""
        web3 = MagicMock()
        web3.eth.get_block = MagicMock(return_value={})  # no baseFeePerGas
        web3.eth.gas_price = 60 * 10**9  # 60 gwei

        gas_fields = await PolymarketServiceServicer._build_eip1559_gas_fields(web3)

        assert gas_fields == {"gasPrice": 60 * 10**9}
        assert "maxFeePerGas" not in gas_fields
        assert "maxPriorityFeePerGas" not in gas_fields


# =============================================================================
# (g) Cached Web3 helper integration
# =============================================================================


class TestCachedWeb3Integration:
    """``_get_polygon_web3`` must route through the gateway's ``get_cached_web3``
    helper instead of constructing a fresh ``Web3(HTTPProvider(...))`` per
    call. Without this, every gateway service spawns its own connection pool
    against Polygon — wasteful and harder to tune."""

    def test_get_polygon_web3_uses_cached_helper(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Patch the cached helper and assert the servicer routes through it
        (not through a fresh ``HTTPProvider``)."""
        sentinel_web3 = MagicMock(name="cached_web3_instance")
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_cached_web3",
            return_value=sentinel_web3,
        ) as mock_get_cached:
            result = servicer._get_polygon_web3()

        assert result is sentinel_web3
        mock_get_cached.assert_called_once_with("polygon", network="mainnet")

    def test_get_polygon_web3_passes_anvil_network_when_env_set(self, settings: MagicMock) -> None:
        """``polymarket_network='anvil'`` flows into the helper so the cache
        key is properly partitioned between mainnet and Anvil."""
        settings.polymarket_network = "anvil"
        servicer = PolymarketServiceServicer(settings=settings)
        sentinel_web3 = MagicMock()
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_cached_web3",
            return_value=sentinel_web3,
        ) as mock_get_cached:
            servicer._get_polygon_web3()

        mock_get_cached.assert_called_once_with("polygon", network="anvil")

    def test_get_polygon_web3_caches_on_servicer(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """The first call resolves the helper; subsequent calls return the
        cached attribute directly without re-invoking the helper. The helper
        itself is process-cached too, but we still want to avoid the import +
        attribute lookup in the hot path."""
        sentinel_web3 = MagicMock()
        with patch(
            "almanak.connectors.polymarket.gateway.service.get_cached_web3",
            return_value=sentinel_web3,
        ) as mock_get_cached:
            first = servicer._get_polygon_web3()
            second = servicer._get_polygon_web3()
            third = servicer._get_polygon_web3()

        assert first is second is third is sentinel_web3
        # Resolved exactly once — subsequent calls hit the per-instance attr.
        mock_get_cached.assert_called_once()


class TestSignAndSubmitChainIdGate:
    """End-to-end at the public boundary: ``_sign_and_submit_setup_tx`` must
    abort BEFORE building the transaction when chain-id is wrong. A chain-id
    error must not have already burned ``eth_getTransactionCount`` /
    ``eth_gasPrice`` round-trips."""

    @pytest.mark.asyncio
    async def test_send_aborts_on_wrong_chain_id(self, servicer: PolymarketServiceServicer) -> None:
        servicer.settings.polymarket_network = "mainnet"
        web3 = MagicMock()
        web3.eth.chain_id = 1  # Ethereum mainnet
        web3.eth.get_transaction_count = MagicMock(side_effect=AssertionError("must not be called"))
        web3.eth.send_raw_transaction = MagicMock(side_effect=AssertionError("must not be called"))
        servicer._polygon_web3 = web3

        with patch(
            "almanak.connectors.polymarket.gateway.service.get_rpc_url",
            return_value="https://polygon-rpc.example.com",
        ), pytest.raises(ValueError, match="expected polygon mainnet"):
            # VIB-3710: _sign_and_submit_setup_tx now takes a request-scoped
            # setup_txs list as a second positional arg. The chain-id assertion
            # raises BEFORE any append, so the list stays empty regardless.
            await servicer._sign_and_submit_setup_tx(
                TransactionData(to="0xabc", data="0x", gas_estimate=80_000, description="approve"),
                [],
            )

        # Critically: no signing-pre-flight RPCs were made.
        web3.eth.get_transaction_count.assert_not_called()
        web3.eth.send_raw_transaction.assert_not_called()
