"""Integration tests for Polymarket V2 connector.

Restored from V1 (deleted in the V2 cutover) and adapted to V2's contract set:
the trading collateral is now pUSD (V1 used USDC), exchanges are CTF V2 and
NegRisk V2 (V1 had V1 exchanges), and approvals span 6 legs (V1 had 4) — the
NegRisk Adapter is now an additional pUSD spender on neg-risk fills.

Tiers:
1. Read-only tests against the live V2 CLOB API (markets, orderbook, price,
   health, server time). No funds, no Anvil — just network reachability.
2. Fork-based tests against an Anvil fork of Polygon mainnet. These verify
   the V2 transaction builders produce calldata that actually executes
   on-chain against the real contracts.

To run read-only tier (network only):
    uv run pytest tests/integration/connectors/test_polymarket_integration.py \
        -v -s -m "integration and polymarket"

To run on-chain tier (requires ALCHEMY_API_KEY for Polygon fork):
    uv run pytest tests/integration/connectors/test_polymarket_integration.py \
        -v -s -m "anvil and polymarket"

Both tiers are opt-in via pytest markers (CI skips them by default).
"""

from __future__ import annotations

import subprocess
from decimal import Decimal

import pytest

from tests.conftest_gateway import AnvilFixture

pytest_plugins = ["tests.conftest_gateway"]


# =============================================================================
# Constants — V2 Polygon mainnet
# =============================================================================

# Anvil's first deterministic account.
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# V2 contract addresses are sourced from connector models so a future bump
# flows through. Imports inlined to keep the constants block self-contained.
from almanak.framework.connectors.polymarket.models import (
    COLLATERAL_ONRAMP,
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE_V2,
    PUSD,
    USDCE_POLYGON,
)

# USDC.e on Polygon uses a standard ERC20 layout: balanceOf is at slot 0.
# pUSD's ERC20 is also standard but the specific slot must be probed (or
# we can fund USDC.e and wrap to pUSD via the Onramp — the more realistic
# end-to-end path).
USDCE_BALANCE_SLOT_BASE = 0


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "success", "type": "bool"}],
        "type": "function",
    },
]

# Minimal CTF (ERC-1155) ABI for isApprovedForAll readback.
ERC1155_ABI = [
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Helpers — anvil-native funding via cast
# =============================================================================


def _anvil_set_balance(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with native MATIC (for gas)."""
    subprocess.run(
        ["cast", "rpc", "anvil_setBalance", wallet, hex(amount_wei), "--rpc-url", rpc_url],
        capture_output=True,
        check=True,
    )


def _anvil_set_token_balance(token: str, wallet: str, amount: int, rpc_url: str, slot: int = 0) -> None:
    """Set an ERC-20 balance directly via storage manipulation.

    Standard OpenZeppelin layout puts ``_balances`` at slot 0; the per-wallet
    storage key is ``keccak256(abi.encode(wallet, slot))``. Computed via
    ``cast index`` to avoid pulling in keccak in tests.
    """
    storage_slot = subprocess.run(
        ["cast", "index", "address", wallet, str(slot)],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    subprocess.run(
        [
            "cast", "rpc", "anvil_setStorageAt",
            token,
            storage_slot,
            "0x" + hex(amount)[2:].zfill(64),
            "--rpc-url", rpc_url,
        ],
        capture_output=True, check=True,
    )


def _format_units(amount: int, decimals: int = 6) -> Decimal:
    return Decimal(amount) / Decimal(10**decimals)


def _send_signed(web3, tx_dict: dict, private_key: str) -> dict:
    """Sign and send an EIP-1559 tx; wait for receipt. Polygon requires type-2."""
    from web3 import Web3

    sender = Web3.to_checksum_address(tx_dict.get("from", TEST_WALLET))
    tx_dict["chainId"] = web3.eth.chain_id
    tx_dict["nonce"] = web3.eth.get_transaction_count(sender)
    tx_dict.setdefault("gas", 500_000)

    if "gasPrice" not in tx_dict and "maxFeePerGas" not in tx_dict:
        latest = web3.eth.get_block("pending")
        base_fee = latest.get("baseFeePerGas", 30 * 10**9)
        priority_fee = 30 * 10**9
        tx_dict["maxPriorityFeePerGas"] = priority_fee
        tx_dict["maxFeePerGas"] = base_fee * 2 + priority_fee

    signed = web3.eth.account.sign_transaction(tx_dict, private_key)
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    return dict(web3.eth.wait_for_transaction_receipt(tx_hash))


# =============================================================================
# Fixtures — read-only (live API) tier
# =============================================================================


@pytest.fixture(scope="module")
def polymarket_config():
    """Config for unauthenticated read-only ops against the V2 CLOB."""
    from almanak.framework.connectors.polymarket import PolymarketConfig

    return PolymarketConfig(
        wallet_address=TEST_WALLET,
        rate_limit_enabled=False,
    )


@pytest.fixture(scope="module")
def clob_client(polymarket_config):
    """V2 CLOB client (defaults to clob.polymarket.com post-cutover)."""
    from almanak.framework.connectors.polymarket import ClobClient
    from almanak.framework.connectors.polymarket.signer import make_local_signer

    return ClobClient(polymarket_config, signer=make_local_signer(TEST_PRIVATE_KEY))


# =============================================================================
# Fixtures — Anvil fork tier
# =============================================================================


@pytest.fixture(scope="module")
def anvil_rpc_url(anvil_polygon: AnvilFixture) -> str:
    """RPC URL for the Polygon Anvil fork."""
    return anvil_polygon.get_rpc_url()


@pytest.fixture(scope="module")
def web3_polygon(anvil_rpc_url: str):
    """Web3 client against the Anvil Polygon fork (POA middleware)."""
    from web3 import Web3
    from web3.middleware import ExtraDataToPOAMiddleware

    w3 = Web3(Web3.HTTPProvider(anvil_rpc_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if w3.eth.chain_id != 137:
        pytest.skip(
            f"anvil_polygon must fork Polygon mainnet (137). Got chain_id={w3.eth.chain_id}."
        )
    return w3


@pytest.fixture(scope="module")
def funded_wallet_polygon(web3_polygon, anvil_rpc_url: str) -> str:
    """Fund TEST_WALLET with MATIC + USDC.e + pUSD for V2 on-chain tests.

    pUSD funding goes through storage-slot manipulation rather than the
    Onramp wrap path. The on-chain tests only need a non-zero balance for
    the AllowanceStatus assertion; testing the wrap path itself is a
    separate concern (the unit tests cover the calldata builders).
    """
    from web3 import Web3

    wallet = Web3.to_checksum_address(TEST_WALLET)

    # 100 MATIC for gas.
    _anvil_set_balance(wallet, 100 * 10**18, anvil_rpc_url)
    assert web3_polygon.eth.get_balance(wallet) >= 100 * 10**18

    # 10,000 USDC.e (6 decimals) — slot 0 standard layout.
    usdce_amount = 10_000 * 10**6
    _anvil_set_token_balance(USDCE_POLYGON, wallet, usdce_amount, anvil_rpc_url, slot=0)
    usdce = web3_polygon.eth.contract(address=Web3.to_checksum_address(USDCE_POLYGON), abi=ERC20_ABI)
    if usdce.functions.balanceOf(wallet).call() < usdce_amount:
        pytest.skip("USDC.e storage slot 0 funding did not take effect on this fork.")

    # 10,000 pUSD. Try slot 0 first; some forks/proxy patterns may differ.
    pusd_amount = 10_000 * 10**6
    _anvil_set_token_balance(PUSD, wallet, pusd_amount, anvil_rpc_url, slot=0)
    pusd = web3_polygon.eth.contract(address=Web3.to_checksum_address(PUSD), abi=ERC20_ABI)
    if pusd.functions.balanceOf(wallet).call() < pusd_amount:
        # pUSD may use a non-standard slot (e.g. proxy-mapped). Skip rather
        # than misreport — tier 2 still covers approvals + ensure_allowances.
        pytest.skip("pUSD balance funding via slot 0 did not take effect; needs slot probing.")

    return TEST_WALLET


@pytest.fixture(scope="module")
def funded_wallet_polygon_no_pusd(web3_polygon, anvil_rpc_url: str) -> str:
    """Fund only MATIC + USDC.e (skips pUSD).

    Use for approval-only tests that don't depend on a pUSD balance — keeps
    them runnable even if pUSD's storage layout breaks the slot-0 funding.
    """
    from web3 import Web3

    wallet = Web3.to_checksum_address(TEST_WALLET)
    _anvil_set_balance(wallet, 100 * 10**18, anvil_rpc_url)
    _anvil_set_token_balance(USDCE_POLYGON, wallet, 10_000 * 10**6, anvil_rpc_url, slot=0)

    usdce = web3_polygon.eth.contract(address=Web3.to_checksum_address(USDCE_POLYGON), abi=ERC20_ABI)
    if usdce.functions.balanceOf(wallet).call() < 10_000 * 10**6:
        pytest.skip("USDC.e storage slot 0 funding did not take effect on this fork.")

    return TEST_WALLET


@pytest.fixture(scope="module")
def pusd_contract(web3_polygon):
    from web3 import Web3
    return web3_polygon.eth.contract(address=Web3.to_checksum_address(PUSD), abi=ERC20_ABI)


@pytest.fixture(scope="module")
def usdce_contract(web3_polygon):
    from web3 import Web3
    return web3_polygon.eth.contract(address=Web3.to_checksum_address(USDCE_POLYGON), abi=ERC20_ABI)


@pytest.fixture(scope="module")
def ctf_erc1155_contract(web3_polygon):
    from web3 import Web3
    return web3_polygon.eth.contract(address=Web3.to_checksum_address(CONDITIONAL_TOKENS), abi=ERC1155_ABI)


# =============================================================================
# Tier 1 — Read-only V2 CLOB tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.polymarket
class TestPolymarketReadOnlyAPI:
    """Live V2 CLOB read-only sanity checks. Skipped if the API is unreachable."""

    def test_health_check(self, clob_client):
        """V2 /health returns ok."""
        try:
            assert clob_client.health_check() is True
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

    def test_fetch_markets(self, clob_client):
        """Active-market fetch returns at least one well-formed market."""
        from almanak.framework.connectors.polymarket import MarketFilters

        try:
            markets = clob_client.get_markets(MarketFilters(active=True, limit=5))
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

        assert len(markets) > 0, "Should have at least one active market"
        m = markets[0]
        assert m.id
        assert m.question
        assert len(m.outcomes) >= 2

    def test_fetch_orderbook(self, clob_client):
        """Pick the first CLOB-enabled market and fetch its orderbook.

        The CLOB endpoint set is independently versioned from the connector
        — Polymarket may flip the canonical hostname (e.g. clob-v2 → clob)
        ahead of a connector bump. Skip on *any* API error rather than fail
        so a URL drift surfaces as a skipped suite, not a red CI.
        """
        from almanak.framework.connectors.polymarket import MarketFilters
        from almanak.framework.connectors.polymarket.exceptions import PolymarketAPIError

        try:
            markets = clob_client.get_markets(
                MarketFilters(active=True, enable_order_book=True, limit=10)
            )
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

        market = next((m for m in markets if m.clob_token_ids), None)
        if not market:
            pytest.skip("No CLOB-enabled markets with token IDs found")

        try:
            orderbook = clob_client.get_orderbook(market.clob_token_ids[0])
        except PolymarketAPIError as e:
            pytest.skip(f"Orderbook endpoint unreachable / changed: {e}")

        assert orderbook is not None
        assert hasattr(orderbook, "bids") and hasattr(orderbook, "asks")

    def test_fetch_price(self, clob_client):
        """Bid/ask/mid on a CLOB-enabled market are in [0, 1] when set."""
        from almanak.framework.connectors.polymarket import MarketFilters
        from almanak.framework.connectors.polymarket.exceptions import PolymarketAPIError

        try:
            markets = clob_client.get_markets(
                MarketFilters(active=True, enable_order_book=True, limit=10)
            )
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

        market = next((m for m in markets if m.clob_token_ids), None)
        if not market:
            pytest.skip("No CLOB-enabled markets with token IDs found")

        try:
            price = clob_client.get_price(market.clob_token_ids[0])
        except PolymarketAPIError as e:
            pytest.skip(f"Price endpoint unreachable / changed: {e}")

        for component in (price.mid, price.bid, price.ask):
            if component is not None:
                assert Decimal("0") <= component <= Decimal("1")

    def test_get_server_time(self, clob_client):
        """V2 server_time within 5 minutes of local clock."""
        import time as _time

        try:
            server_time = clob_client.get_server_time()
        except Exception as e:
            pytest.skip(f"Polymarket API not reachable: {e}")

        assert abs(server_time - int(_time.time())) < 300


# =============================================================================
# Tier 2 — V2 on-chain (Anvil Polygon fork)
# =============================================================================


@pytest.mark.anvil
@pytest.mark.polymarket
class TestPolymarketV2OnChain:
    """Verify V2 on-chain transaction builders execute against the real
    contracts on a Polygon fork."""

    def test_approve_pusd_for_ctf_exchange_v2(
        self,
        web3_polygon,
        funded_wallet_polygon_no_pusd: str,
        pusd_contract,
    ):
        """V2 BUYs need pUSD → CTF Exchange V2 approval — without it, the
        matcher rejects fills with `the allowance is not enough`."""
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        wallet = Web3.to_checksum_address(funded_wallet_polygon_no_pusd)
        before = pusd_contract.functions.allowance(wallet, Web3.to_checksum_address(CTF_EXCHANGE_V2)).call()

        sdk = CtfSDK()
        tx_data = sdk.build_approve_collateral_tx(
            asset=PUSD,
            spender=CTF_EXCHANGE_V2,
            sender=wallet,
            amount=MAX_UINT256,
        )

        receipt = _send_signed(
            web3_polygon,
            {"from": wallet, "to": tx_data.to, "value": tx_data.value, "data": tx_data.data, "gas": tx_data.gas_estimate},
            TEST_PRIVATE_KEY,
        )
        assert receipt["status"] == 1, f"approve(pUSD → CTFv2) reverted: {receipt}"

        after = pusd_contract.functions.allowance(wallet, Web3.to_checksum_address(CTF_EXCHANGE_V2)).call()
        assert after == MAX_UINT256, f"allowance not at max: {after} (was {before})"

    def test_approve_pusd_for_neg_risk_exchange_v2(
        self,
        web3_polygon,
        funded_wallet_polygon_no_pusd: str,
        pusd_contract,
    ):
        """V2 neg-risk BUYs need pUSD → NegRisk Exchange V2 approval."""
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        wallet = Web3.to_checksum_address(funded_wallet_polygon_no_pusd)

        sdk = CtfSDK()
        tx_data = sdk.build_approve_collateral_tx(
            asset=PUSD,
            spender=NEG_RISK_EXCHANGE_V2,
            sender=wallet,
            amount=MAX_UINT256,
        )

        receipt = _send_signed(
            web3_polygon,
            {"from": wallet, "to": tx_data.to, "value": tx_data.value, "data": tx_data.data, "gas": tx_data.gas_estimate},
            TEST_PRIVATE_KEY,
        )
        assert receipt["status"] == 1, f"approve(pUSD → NegRiskv2) reverted: {receipt}"

        after = pusd_contract.functions.allowance(wallet, Web3.to_checksum_address(NEG_RISK_EXCHANGE_V2)).call()
        assert after == MAX_UINT256

    def test_approve_pusd_for_neg_risk_adapter(
        self,
        web3_polygon,
        funded_wallet_polygon_no_pusd: str,
        pusd_contract,
    ):
        """V2-only leg: NegRisk Adapter is the actual spender on neg-risk
        fills (split/merge). Missing this approval causes neg-risk BUYs to
        fail with "spender: 0xd91E80..." even when the exchange approval
        is in place."""
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        wallet = Web3.to_checksum_address(funded_wallet_polygon_no_pusd)

        sdk = CtfSDK()
        tx_data = sdk.build_approve_collateral_tx(
            asset=PUSD,
            spender=NEG_RISK_ADAPTER,
            sender=wallet,
            amount=MAX_UINT256,
        )

        receipt = _send_signed(
            web3_polygon,
            {"from": wallet, "to": tx_data.to, "value": tx_data.value, "data": tx_data.data, "gas": tx_data.gas_estimate},
            TEST_PRIVATE_KEY,
        )
        assert receipt["status"] == 1, f"approve(pUSD → NegRiskAdapter) reverted: {receipt}"

        after = pusd_contract.functions.allowance(wallet, Web3.to_checksum_address(NEG_RISK_ADAPTER)).call()
        assert after == MAX_UINT256

    def test_approve_usdce_for_onramp(
        self,
        web3_polygon,
        funded_wallet_polygon_no_pusd: str,
        usdce_contract,
    ):
        """V2 source-of-funds leg: USDC.e → CollateralOnramp lets the user
        wrap their bridged USDC into pUSD. Without this leg the gateway
        cannot top up pUSD before a BUY."""
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import MAX_UINT256, CtfSDK

        wallet = Web3.to_checksum_address(funded_wallet_polygon_no_pusd)

        sdk = CtfSDK()
        tx_data = sdk.build_approve_collateral_tx(
            asset=USDCE_POLYGON,
            spender=COLLATERAL_ONRAMP,
            sender=wallet,
            amount=MAX_UINT256,
        )

        receipt = _send_signed(
            web3_polygon,
            {"from": wallet, "to": tx_data.to, "value": tx_data.value, "data": tx_data.data, "gas": tx_data.gas_estimate},
            TEST_PRIVATE_KEY,
        )
        assert receipt["status"] == 1, f"approve(USDC.e → Onramp) reverted: {receipt}"

        after = usdce_contract.functions.allowance(wallet, Web3.to_checksum_address(COLLATERAL_ONRAMP)).call()
        assert after == MAX_UINT256

    def test_set_approval_for_all_ctf_exchange_v2(
        self,
        web3_polygon,
        funded_wallet_polygon_no_pusd: str,
        ctf_erc1155_contract,
    ):
        """SELL leg: V2 exchange pulls outcome shares (ERC-1155) on fill.
        setApprovalForAll on the CTF token authorizes that pull."""
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import CtfSDK

        wallet = Web3.to_checksum_address(funded_wallet_polygon_no_pusd)

        sdk = CtfSDK()
        tx_data = sdk.build_approve_conditional_tokens_tx(
            operator=CTF_EXCHANGE_V2,
            approved=True,
            sender=wallet,
        )

        receipt = _send_signed(
            web3_polygon,
            {"from": wallet, "to": tx_data.to, "value": tx_data.value, "data": tx_data.data, "gas": tx_data.gas_estimate},
            TEST_PRIVATE_KEY,
        )
        assert receipt["status"] == 1, f"setApprovalForAll(CTFv2, true) reverted: {receipt}"

        approved = ctf_erc1155_contract.functions.isApprovedForAll(
            wallet, Web3.to_checksum_address(CTF_EXCHANGE_V2)
        ).call()
        assert approved is True

    def test_ensure_allowances_emits_full_v2_set_against_fresh_wallet(
        self,
        web3_polygon,
        anvil_rpc_url: str,
    ):
        """End-to-end V2 idempotent setup against a fresh wallet.

        ``CtfSDK.ensure_allowances`` is the entry point the gateway uses on
        the first BUY/market order: it inspects current state and emits ONLY
        the missing approvals. For a wallet with no prior approvals this
        must produce all 6 legs in canonical order, and after submission
        ``check_allowances`` must report ``fully_approved``.

        Uses a fresh wallet (different from TEST_WALLET) so it doesn't
        collide with state set up by the per-leg tests above.
        """
        from eth_account import Account
        from web3 import Web3

        from almanak.framework.connectors.polymarket.ctf_sdk import CtfSDK

        # Deterministic-but-distinct second account.
        fresh_pk = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
        fresh_wallet = Account.from_key(fresh_pk).address

        _anvil_set_balance(fresh_wallet, 50 * 10**18, anvil_rpc_url)
        _anvil_set_token_balance(USDCE_POLYGON, fresh_wallet, 1_000 * 10**6, anvil_rpc_url, slot=0)

        sdk = CtfSDK()

        before = sdk.check_allowances(fresh_wallet, web3_polygon)
        assert not before.fully_approved, "Fresh wallet should start unapproved"

        txs = sdk.ensure_allowances(fresh_wallet, web3_polygon)
        assert len(txs) == 6, f"Expected 6-tx V2 approval set, got {len(txs)}"

        for tx in txs:
            receipt = _send_signed(
                web3_polygon,
                {"from": Web3.to_checksum_address(fresh_wallet), "to": tx.to, "value": tx.value, "data": tx.data, "gas": tx.gas_estimate},
                fresh_pk,
            )
            assert receipt["status"] == 1, f"approval tx reverted: {tx.description}"

        after = sdk.check_allowances(fresh_wallet, web3_polygon)
        assert after.fully_approved, (
            f"All 6 V2 approvals applied but fully_approved is False — state: {after}"
        )

        # Idempotent: a second pass against a now-approved wallet emits zero txs.
        assert sdk.ensure_allowances(fresh_wallet, web3_polygon) == []

    def test_check_v2_allowances_reflects_pusd_balance(
        self,
        web3_polygon,
        funded_wallet_polygon: str,
    ):
        """check_allowances surfaces both pUSD and source-asset balances —
        the gateway pre-flight reads both to decide whether to wrap."""
        from almanak.framework.connectors.polymarket.ctf_sdk import CtfSDK

        sdk = CtfSDK()
        status = sdk.check_allowances(funded_wallet_polygon, web3_polygon)

        assert status.source_asset_balance >= 10_000 * 10**6, (
            f"USDC.e balance {_format_units(status.source_asset_balance)} "
            f"below funded amount"
        )
        assert status.pusd_balance >= 10_000 * 10**6, (
            f"pUSD balance {_format_units(status.pusd_balance)} below funded amount"
        )
