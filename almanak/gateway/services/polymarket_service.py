"""PolymarketService implementation - Polymarket CLOB API proxy.

This service provides secure access to Polymarket's CLOB API:
- L1 Authentication (EIP-712) for credential creation
- L2 Authentication (HMAC-SHA256) for trading operations
- All credentials held in gateway, keeping secrets secure

The service proxies calls to:
- CLOB API: Order management, orderbooks, prices
- Gamma API: Market metadata
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import aiohttp
import grpc
from eth_account import Account
from eth_account.messages import encode_typed_data
from pydantic import SecretStr

if TYPE_CHECKING:
    from web3 import Web3

from almanak.framework.connectors.polymarket import (
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
    ApiCredentials,
    ClobClient,
    CtfSDK,
    GammaMarket,
    MarketFilters,
    OrderFilters,
    PolymarketConfig,
    SignatureType,
    TransactionData,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.rpc_provider import get_cached_web3, get_rpc_url, is_local_rpc
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Upstream cap for /data/trades — documented in the proto and confirmed against
# Polymarket's CLOB. Validated at the gateway boundary so callers see
# ``INVALID_ARGUMENT`` rather than an opaque upstream 4xx.
_TRADE_TAPE_LIMIT_MAX = 500

# Polygon mainnet chain ID (https://chainlist.org/chain/137). Polymarket V2
# contracts only exist on Polygon mainnet — sending a setup/wrap tx to any
# other chain wastes gas at best, signs a real tx against the wrong contracts
# at worst. We assert this once per process before the first send_raw_transaction.
POLYGON_MAINNET_CHAIN_ID = 137

# pUSD balance cache staleness, in blocks. Polygon block time is ~2 s, so 50
# blocks ≈ 100 s — long enough to cover the typical strategy decide loop
# without bouncing between RPC and cache, short enough that another in-flight
# wrap from a different process / wallet movement gets noticed quickly.
PUSD_CACHE_STALE_BLOCKS = 50

# Polygon enforces a 30 gwei minimum priority fee at the Heimdall layer for
# EIP-1559 transactions. Our floor is the same as the network minimum so we
# don't get silently rejected with "max priority fee per gas higher than max
# fee per gas" or "transaction underpriced".
POLYGON_MIN_PRIORITY_FEE_WEI = 30 * 10**9  # 30 gwei


class PolymarketServiceServicer(gateway_pb2_grpc.PolymarketServiceServicer):
    """Implements PolymarketService gRPC interface.

    Provides secure proxy to Polymarket CLOB API with credentials held in gateway.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize PolymarketService.

        Args:
            settings: Gateway settings (contains Polymarket credentials)
        """
        self.settings = settings
        self._http_session: aiohttp.ClientSession | None = None
        self._credentials_lock = asyncio.Lock()

        # V2 on-chain wallet auto-setup state. The first BUY/market order
        # acquires this lock, submits any missing token approvals (USDC.e
        # → Onramp, pUSD → V2 exchanges, CTF → V2 exchanges + adapter), and
        # wraps source asset → pUSD as needed. Subsequent calls are cheap
        # balance lookups behind the same lock.
        self._wallet_ready_lock = asyncio.Lock()
        self._allowances_applied = False
        # Lazily built (web3.py is sync; kept off the asyncio loop). The TYPE_CHECKING
        # import keeps mypy honest without paying the import cost at module load.
        self._polygon_web3: Web3 | None = None
        self._ctf_sdk: CtfSDK | None = None
        # Chain-id assertion runs once per process. Set to the verified value
        # (or a sentinel for accepted Anvil forks) the first time we sign.
        self._chain_id_verified: bool = False
        # pUSD balance cache for BUY orders. Avoids re-reading on-chain on
        # every order when the prior balance + the in-flight wraps already
        # cover the new ``min_pusd_units``. Keyed by wallet implicitly (one
        # servicer = one signer). Reset whenever the cache is invalidated.
        self._cached_pusd_balance: int | None = None
        self._cached_pusd_balance_block: int | None = None
        # VIB-3710: setup-tx attribution is request-scoped — each
        # ``CreateAndPostOrder`` invocation owns a local
        # ``setup_txs: list[dict[str, Any]]`` that ``_ensure_wallet_ready``
        # populates and the RPC drains into the response. NO instance-level
        # ledger: a shared ``_pending_setup_txs`` would race across concurrent
        # order RPCs (request A's approvals leaking into request B's response,
        # corrupting basis attribution across positions).
        self._private_key = settings.private_key or settings.polymarket_private_key
        self._wallet_address = self._resolve_signer_address()
        self._funder_address = self._resolve_funder_address()
        self._signature_type = (
            SignatureType.POLY_GNOSIS_SAFE
            if settings.safe_address and (settings.safe_mode or "").lower() in {"direct", "zodiac"}
            else SignatureType.EOA
        )
        self._api_key = settings.polymarket_api_key
        self._api_secret = settings.polymarket_secret
        self._api_passphrase = settings.polymarket_passphrase

        self._available = bool(self._private_key and self._wallet_address)
        self._credentials_available = bool(self._api_key and self._api_secret and self._api_passphrase)

        logger.debug(
            "PolymarketService initialized: available=%s, credentials=%s, signer=%s, funder=%s, signature_type=%s",
            self._available,
            self._credentials_available,
            self._wallet_address,
            self._funder_address,
            self._signature_type.name,
        )

    def _resolve_signer_address(self) -> str | None:
        if self.settings.eoa_address:
            return self.settings.eoa_address
        if self.settings.private_key:
            return Account.from_key(self.settings.private_key).address
        if self.settings.polymarket_private_key:
            return Account.from_key(self.settings.polymarket_private_key).address
        return None

    def _resolve_funder_address(self) -> str | None:
        if self.settings.polymarket_wallet_address:
            return self.settings.polymarket_wallet_address
        if self.settings.safe_address:
            return self.settings.safe_address
        return self._wallet_address

    def _build_client(self, *, require_signer: bool = True) -> ClobClient:
        """Build a ``ClobClient`` for use by an RPC handler.

        ``require_signer=True`` (the default) is the safe choice for any RPC
        that signs orders, derives api credentials, or otherwise needs the
        trading EOA. ``require_signer=False`` is for read-only public RPCs
        like ``GetPriceHistory`` and may be invoked on a gateway that has no
        Polymarket signer configured at all (e.g. a market-data-only deploy).
        Public endpoints don't read ``wallet_address`` / ``private_key`` from
        the config, so we pass placeholder values that satisfy
        ``PolymarketConfig``'s required-field validators without enabling any
        signing path.
        """
        if require_signer:
            if not self._available or not self._wallet_address or not self._private_key:
                raise ValueError("Polymarket signing identity is not configured in the gateway")

        api_credentials = None
        if self._credentials_available and self._api_key and self._api_secret and self._api_passphrase:
            api_credentials = ApiCredentials(
                api_key=self._api_key,
                secret=SecretStr(self._api_secret),
                passphrase=SecretStr(self._api_passphrase),
            )

        # Placeholders only used when ``require_signer=False`` and the gateway
        # has no real Polymarket signer wired. ClobClient never reads these
        # for the public endpoints (``get_price_history``, ``get_orderbook``,
        # ``get_market``); any signed call on the returned client would
        # produce an obviously-invalid signature, which is the desired
        # fail-loud behavior if a caller mis-routes a signed RPC here.
        wallet = self._wallet_address or "0x" + "0" * 40
        key = self._private_key or "0x" + "0" * 64

        config = PolymarketConfig(
            wallet_address=wallet,
            private_key=SecretStr(key),
            signature_type=self._signature_type,
            funder_address=self._funder_address if self._funder_address != self._wallet_address else None,
            api_credentials=api_credentials,
        )
        return ClobClient(config)

    async def _build_authenticated_client(self) -> ClobClient:
        """Build a CLOB client with stable gateway-owned API credentials.

        Polymarket API keys are wallet-scoped but some authenticated endpoints
        are sensitive to which API key created the order. Re-deriving a fresh
        key for each RPC can make a just-created order unreadable via
        ``GetOrder`` even though ``CreateAndPostOrder`` succeeded. Resolve or
        derive once, cache on the service, and reuse the same credentials for
        subsequent authenticated calls.
        """
        if not self._credentials_available:
            ok = await self._ensure_credentials()
            if not ok:
                raise ValueError("Polymarket API credentials could not be derived in gateway")
        return self._build_client()

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._http_session is None or self._http_session.closed:
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30.0),
                connector=connector,
            )
        return self._http_session

    async def close(self) -> None:
        """Close HTTP session."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    # =========================================================================
    # L1 Authentication (EIP-712)
    # =========================================================================

    def _build_l1_headers(self, nonce: int = 0) -> dict[str, str]:
        """Build L1 authentication headers using EIP-712 signing."""
        timestamp = str(int(time.time()))

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                **CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": CLOB_AUTH_DOMAIN,
            "message": {
                "address": self._wallet_address,
                "timestamp": timestamp,
                "nonce": nonce,
                "message": CLOB_AUTH_MESSAGE,
            },
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, self._private_key)

        # Modern eth-account returns hex without `0x`; Polymarket's
        # /auth/api-key + /auth/derive-api-key reject unprefixed signatures
        # with HTTP 401 "Invalid L1 Request headers". Always prefix.
        sig_hex = signed.signature.hex()
        if not sig_hex.startswith("0x"):
            sig_hex = "0x" + sig_hex

        return {
            "POLY_ADDRESS": self._wallet_address,
            "POLY_SIGNATURE": sig_hex,
            "POLY_TIMESTAMP": timestamp,
            "POLY_NONCE": str(nonce),
        }

    async def _ensure_credentials(self) -> bool:
        """Ensure we have API credentials, creating if needed."""
        if self._credentials_available:
            return True

        if not self._available:
            return False

        async with self._credentials_lock:
            # Re-check inside lock in case another coroutine just derived them.
            if self._credentials_available:
                return True
            return await self._derive_or_create_credentials()

    async def _derive_or_create_credentials(self) -> bool:
        """Inner credential derivation/creation (must be called while holding _credentials_lock)."""
        # Try to derive existing credentials
        try:
            session = await self._get_session()
            headers = self._build_l1_headers()

            async with session.get(f"{CLOB_BASE_URL}/auth/derive-api-key", headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        response_text = await response.text()
                        logger.warning(
                            "Failed to parse derive credentials response: %s, body: %s", e, response_text[:200]
                        )
                    else:
                        self._api_key = data.get("apiKey")
                        self._api_secret = data.get("secret")
                        self._api_passphrase = data.get("passphrase")
                        self._credentials_available = True
                        logger.info("Derived existing API credentials")
                        return True
        except (TimeoutError, aiohttp.ClientError) as e:
            logger.warning("Failed to derive credentials: %s", e)

        # Create new credentials
        try:
            session = await self._get_session()
            headers = self._build_l1_headers()

            async with session.post(f"{CLOB_BASE_URL}/auth/api-key", headers=headers) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        response_text = await response.text()
                        logger.warning(
                            "Failed to parse create credentials response: %s, body: %s", e, response_text[:200]
                        )
                    else:
                        self._api_key = data.get("apiKey")
                        self._api_secret = data.get("secret")
                        self._api_passphrase = data.get("passphrase")
                        self._credentials_available = True
                        logger.info("Created new API credentials")
                        return True
                else:
                    error_body = (await response.text())[:200]
                    logger.error("Failed to create credentials: HTTP %s, body: %s", response.status, error_body)
        except (TimeoutError, aiohttp.ClientError):
            logger.exception("Failed to create credentials")

        return False

    # =========================================================================
    # On-chain Wallet Auto-Setup (V2)
    # =========================================================================
    #
    # Polymarket V2 trading from a fresh wallet requires three setup steps:
    #
    #   1. Token approvals — USDC.e → CollateralOnramp, pUSD → CTF V2 exchange,
    #      pUSD → NegRisk V2 exchange, CTF → CTF V2 exchange, CTF → NegRisk
    #      Adapter. Five txs, each ~50–80k gas, idempotent.
    #   2. Source asset → pUSD wrap via the Onramp.
    #   3. API credentials (already handled by ``_ensure_credentials``).
    #
    # Steps 1 and 2 happen on-chain and are NOT part of the user-facing intent
    # vocabulary (no PREDICTION_BUY intent emits an approve+wrap as siblings).
    # The right home is here: the gateway service that owns Polymarket
    # trading runs the setup lazily on first BUY, behind a lock, with the
    # same private key it uses for off-chain order signing.
    #
    # Each user pays gas for setup once per wallet lifetime. After the first
    # call, ``_allowances_applied`` short-circuits step 1; step 2 only fires
    # when the wallet's pUSD balance can't cover the requested order.

    def _get_polygon_web3(self):
        """Return the gateway-shared Polygon Web3 client.

        Delegates to ``get_cached_web3`` so the underlying ``HTTPProvider``
        connection pool is reused across every gateway service that talks to
        Polygon (no per-call socket churn). web3.py is sync; sync calls are
        still dispatched via ``asyncio.to_thread`` to keep the async gRPC
        handlers non-blocking.

        ``ALMANAK_POLYMARKET_NETWORK=anvil`` (or any other non-mainnet value
        recognised by ``get_rpc_url``) routes the same lookup through the
        Anvil port mapping for local-fork testing.
        """
        if self._polygon_web3 is None:
            network = os.environ.get("ALMANAK_POLYMARKET_NETWORK", "mainnet")
            self._polygon_web3 = get_cached_web3("polygon", network=network)
        return self._polygon_web3

    def _get_ctf_sdk(self) -> CtfSDK:
        if self._ctf_sdk is None:
            self._ctf_sdk = CtfSDK()
        return self._ctf_sdk

    @staticmethod
    def _is_anvil_polymarket_setup() -> bool:
        """Whether the current process is wired up against an Anvil polygon fork.

        True when ``ALMANAK_POLYMARKET_NETWORK`` explicitly selects Anvil OR
        when the resolved Polygon RPC URL points to localhost. Used to relax
        the chain-id assertion: a forked Anvil keeps the same Polygon
        contract addresses but can return any chain ID depending on flags.
        """
        if (os.environ.get("ALMANAK_POLYMARKET_NETWORK") or "").lower() == "anvil":
            return True
        try:
            return is_local_rpc(get_rpc_url("polygon"))
        except (ValueError, KeyError):
            # No RPC configured at all — defer the real error to the
            # send_raw_transaction path; not Anvil from our perspective.
            return False

    async def _assert_polygon_chain_id(self, web3) -> None:  # noqa: ANN001
        """Verify the connected RPC is Polygon mainnet (137) before signing.

        Polymarket V2 contracts only exist on Polygon mainnet. A misconfigured
        RPC env (e.g. a generic ``RPC_URL`` pointing at Arbitrum) would
        otherwise let us silently sign setup/wrap txs against the wrong chain
        and burn gas at best, mint corrupt state at worst. Cached on the
        servicer so the eth_chainId round-trip happens at most once per process.

        Anvil polygon forks are exempt: ``_is_anvil_polymarket_setup`` returns
        ``True`` for ``ALMANAK_POLYMARKET_NETWORK=anvil`` or a localhost RPC,
        and we accept any chain ID those forks report (Anvil defaults to 31337
        unless ``--chain-id 137`` was passed).
        """
        if self._chain_id_verified:
            return

        if self._is_anvil_polymarket_setup():
            # Skip the assertion but mark verified so we don't keep checking.
            logger.debug("polymarket setup running against Anvil fork; chain-id assertion skipped")
            self._chain_id_verified = True
            return

        actual = await asyncio.to_thread(lambda: web3.eth.chain_id)
        if actual != POLYGON_MAINNET_CHAIN_ID:
            raise ValueError(
                f"Polymarket setup tx aborted: RPC reports chain {actual}, "
                f"expected polygon mainnet ({POLYGON_MAINNET_CHAIN_ID})"
            )
        self._chain_id_verified = True

    @staticmethod
    async def _build_eip1559_gas_fields(web3) -> dict[str, int]:  # noqa: ANN001
        """Compute EIP-1559 gas fields for a Polygon tx; fall back to legacy when unsupported.

        Returns a dict suitable for ``Account.sign_transaction``. Detects
        EIP-1559 support by reading ``baseFeePerGas`` from the latest block:
        when present we emit ``maxFeePerGas`` / ``maxPriorityFeePerGas``,
        otherwise we fall back to legacy ``gasPrice`` for Anvil instances that
        run pre-London or with EIP-1559 disabled.

        Polygon enforces a 30 gwei minimum priority fee at the validator layer
        (POLYGON_MIN_PRIORITY_FEE_WEI). We use ``max_priority_fee`` when the
        node estimates higher, otherwise the floor — never below.
        """
        latest = await asyncio.to_thread(web3.eth.get_block, "latest")
        base_fee = latest.get("baseFeePerGas") if isinstance(latest, dict) else getattr(latest, "baseFeePerGas", None)
        if base_fee is None:
            # Pre-London / Anvil-without-EIP-1559 → legacy gasPrice.
            gas_price = await asyncio.to_thread(lambda: web3.eth.gas_price)
            return {"gasPrice": int(gas_price)}

        # max_priority_fee is an estimate from the node. Some providers return
        # 0 or unrealistically low values for Polygon; clamp to the network
        # minimum so the validator doesn't drop the tx.
        try:
            estimated_priority = int(await asyncio.to_thread(lambda: web3.eth.max_priority_fee))
        except Exception:  # noqa: BLE001 — eth_maxPriorityFeePerGas isn't universal; floor below
            estimated_priority = 0
        max_priority_fee = max(estimated_priority, POLYGON_MIN_PRIORITY_FEE_WEI)
        # Battle-tested formula: 2 * baseFee + priority. Covers a single
        # base-fee doubling (Polygon's EIP-1559 baseFee changes by at most
        # 12.5% per block) so the tx remains includable for many blocks.
        max_fee = 2 * int(base_fee) + max_priority_fee
        return {
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority_fee,
        }

    async def _sign_and_submit_setup_tx(
        self,
        tx_data: TransactionData,
        setup_txs: list[dict[str, Any]],
    ) -> str:
        """Sign a setup transaction with the gateway's key and broadcast.

        Returns the tx hash (0x-prefixed). Waits for the receipt and raises
        if the tx reverted — setup must succeed before any order proceeds.

        Asserts the connected RPC is Polygon mainnet (or an accepted Anvil
        fork) before broadcasting, and uses EIP-1559 gas fields with a
        Polygon-safe priority floor (30 gwei). Falls back to legacy
        ``gasPrice`` when the chain doesn't expose ``baseFeePerGas``.

        VIB-3710: after the receipt confirms, append a record
        ``{tx_hash, description, gas_used, gas_price_wei, total_cost_wei}``
        to the caller-supplied ``setup_txs`` list so the calling order RPC
        can attribute the MATIC gas spend to the position whose first BUY
        triggered the setup. The list is request-scoped (owned by the
        ``CreateAndPostOrder`` invocation) so concurrent order RPCs never
        cross-contaminate each other's attribution. The price is derived
        from EIP-1559 ``effectiveGasPrice`` when the receipt exposes it
        (post-London), with a fallback to the ``gasPrice`` / ``maxFeePerGas``
        we put on the tx so legacy / Anvil receipts still produce a non-None
        value.
        """
        web3 = self._get_polygon_web3()
        wallet = self._wallet_address
        if not wallet or not self._private_key:
            raise ValueError("Polymarket auto-setup requires a configured signer")

        await self._assert_polygon_chain_id(web3)

        nonce = await asyncio.to_thread(web3.eth.get_transaction_count, wallet)
        chain_id = await asyncio.to_thread(lambda: web3.eth.chain_id)
        gas_fields = await self._build_eip1559_gas_fields(web3)

        tx = {
            "from": wallet,
            "to": web3.to_checksum_address(tx_data.to),
            "data": tx_data.data,
            "value": tx_data.value,
            "gas": tx_data.gas_estimate,
            "nonce": nonce,
            "chainId": chain_id,
            **gas_fields,
        }
        signed = Account.sign_transaction(tx, self._private_key)
        tx_hash = await asyncio.to_thread(web3.eth.send_raw_transaction, signed.raw_transaction)
        tx_hex = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)
        if not tx_hex.startswith("0x"):
            tx_hex = "0x" + tx_hex
        receipt = await asyncio.to_thread(web3.eth.wait_for_transaction_receipt, tx_hash, 120)
        if receipt.status != 1:
            raise ValueError(f"Polymarket setup tx reverted: {tx_hex} ({tx_data.description})")
        logger.info("polymarket setup tx confirmed: %s — %s", tx_hex, tx_data.description)

        # VIB-3710: extract gas accounting from the receipt and pin it to the
        # in-flight order. ``effectiveGasPrice`` is post-London canon; fall back
        # to whatever price we placed on the tx (maxFeePerGas under EIP-1559,
        # gasPrice under legacy) when the receipt does not expose it. Any
        # arithmetic failure (mock receipts, missing fields) silently records
        # 0 so accounting can still see the tx happened — under-attribution is
        # safer than crashing the order RPC after the chain spend.
        try:
            gas_used = int(getattr(receipt, "gasUsed", 0) or 0)
        except (TypeError, ValueError):
            gas_used = 0
        gas_price_wei = 0
        try:
            eff_price = getattr(receipt, "effectiveGasPrice", None)
            if eff_price is None and isinstance(receipt, dict):
                eff_price = receipt.get("effectiveGasPrice")
            if eff_price is not None:
                gas_price_wei = int(eff_price)
            else:
                # Pre-London / Anvil: prefer maxFeePerGas as the EIP-1559
                # upper bound paid; fall back to legacy gasPrice we set.
                gas_price_wei = int(gas_fields.get("maxFeePerGas") or gas_fields.get("gasPrice") or 0)
        except (TypeError, ValueError):
            gas_price_wei = 0
        total_cost_wei = gas_used * gas_price_wei
        setup_txs.append(
            {
                "tx_hash": tx_hex,
                "description": tx_data.description or "",
                "gas_used": gas_used,
                "gas_price_wei": str(gas_price_wei),
                "total_cost_wei": str(total_cost_wei),
            }
        )

        return tx_hex

    async def _ensure_wallet_ready(self, min_pusd_units: int = 0) -> list[dict[str, Any]]:
        """Idempotent on-chain wallet setup for Polymarket V2 trading.

        Returns a request-scoped ``setup_txs`` list — one entry per approval /
        wrap submitted on this call. Always returns a list (possibly empty);
        the caller embeds the records into the order RPC response so gas spent
        is attributed exactly to the order whose first BUY paid for it. Each
        invocation owns its own list, so concurrent ``CreateAndPostOrder`` /
        ``CreateAndPostMarketOrder`` calls cannot leak attribution into each
        other's responses.

        - First call: submits the V2 5-tx approval set (only the missing legs).
        - SELL orders (``min_pusd_units == 0``): no pUSD balance read — the
          maker spends shares (CTF), not pUSD. Allowances still need to be in
          place for the V2 exchange to pull shares, so the approval pass runs.
        - BUY orders: cache the on-chain pUSD balance on the servicer; only
          re-read on cache miss, when the cache is stale by more than
          ``PUSD_CACHE_STALE_BLOCKS``, or when the cached value can't cover the
          requested order. After a wrap, the cache is updated to
          ``cached + wrap_amount`` so the next BUY doesn't pay another RPC
          round-trip just to confirm what we know we just deposited.

        Behind a single async lock so concurrent first-orders coalesce. Raises
        ``ValueError`` if the source asset balance is insufficient to wrap to
        the required pUSD amount, so the calling RPC fails fast with a clear
        message.

        Auto-setup only supports the EOA-funded path: balances/allowances are
        checked on ``self._wallet_address`` and setup txs are signed by that
        same EOA. In a Safe / ``polymarket_wallet_address`` deployment the
        funder is a different account, so prepping the signer's wallet would
        leave the actual funder unprepared. Refuse early in that case rather
        than silently approving the wrong account.
        """
        # Request-scoped ledger: never an instance attribute. Each invocation
        # builds its own list, hands it to ``_sign_and_submit_setup_tx`` for
        # population, and returns it to the caller. Concurrent calls cannot
        # cross-contaminate.
        setup_txs: list[dict[str, Any]] = []

        if not self._wallet_address or not self._private_key:
            return setup_txs  # signing disabled — no auto-setup possible

        if self._funder_address and self._funder_address.lower() != self._wallet_address.lower():
            raise ValueError(
                "Polymarket auto-setup currently supports only EOA deployments where "
                f"funder_address == signer (got funder={self._funder_address}, signer={self._wallet_address}). "
                "Pre-fund and pre-approve the funder before placing orders, or run the gateway "
                "with the funder EOA's key."
            )

        async with self._wallet_ready_lock:
            ctf = self._get_ctf_sdk()
            web3 = self._get_polygon_web3()
            wallet = self._wallet_address

            if not self._allowances_applied:
                tx_data_list = await asyncio.to_thread(ctf.ensure_allowances, wallet, web3)
                if tx_data_list:
                    logger.info("Applying %d Polymarket V2 approvals for %s", len(tx_data_list), wallet)
                    for tx_data in tx_data_list:
                        await self._sign_and_submit_setup_tx(tx_data, setup_txs)
                self._allowances_applied = True

            # SELL short-circuit: no pUSD math, no balance read, return after
            # allowances are in place. Saves one ERC20 balanceOf call per SELL.
            if min_pusd_units <= 0:
                return setup_txs

            pusd_balance = await self._get_pusd_balance_cached(ctf, web3, wallet, min_pusd_units)
            if pusd_balance < min_pusd_units:
                deficit = min_pusd_units - pusd_balance
                source_balance = await asyncio.to_thread(ctf.get_source_asset_balance, wallet, web3)
                if source_balance < deficit:
                    raise ValueError(
                        f"Insufficient source asset for wrap: need {deficit / 10**6:.4f} more "
                        f"(have {source_balance / 10**6:.4f} {ctf.source_asset[:10]}..., "
                        f"pUSD {pusd_balance / 10**6:.4f}); fund the wallet."
                    )
                logger.info(
                    "Wrapping %.4f source-asset → pUSD to cover order (current pUSD: %.4f)",
                    deficit / 10**6,
                    pusd_balance / 10**6,
                )
                wrap_tx = ctf.build_wrap_to_pusd_tx(wallet, deficit)
                await self._sign_and_submit_setup_tx(wrap_tx, setup_txs)
                # Optimistic cache update — receipt confirmed the wrap landed
                # (``_sign_and_submit_setup_tx`` raises on revert), so the new
                # pUSD balance is at least cached + deficit. Re-anchor to the
                # current block so the staleness counter starts fresh.
                self._cached_pusd_balance = pusd_balance + deficit
                try:
                    raw_block = await asyncio.to_thread(lambda: web3.eth.block_number)
                    self._cached_pusd_balance_block = int(raw_block)
                except Exception:  # noqa: BLE001
                    self._cached_pusd_balance_block = None

        return setup_txs

    async def _get_pusd_balance_cached(  # noqa: ANN001
        self,
        ctf: CtfSDK,
        web3,
        wallet: str,
        min_pusd_units: int,
    ) -> int:
        """Return the wallet's pUSD balance, using the per-instance cache when fresh.

        Re-reads on-chain when:
          1. There is no cached value yet.
          2. The cached value is below ``min_pusd_units`` (we'd otherwise wrap
             unnecessarily, or, worse, refuse a BUY that an outside transfer
             has already covered).
          3. The cache was set more than ``PUSD_CACHE_STALE_BLOCKS`` blocks
             ago (catches outside transfers / consumption from a different
             process, without paying RPC for every order).

        Falls back to a fresh read if reading the current block number fails —
        we'd rather pay the extra RPC round-trip than serve a stale cache after
        a transient block-number lookup error.
        """
        # Read the latest block number once. If it isn't a real int (e.g. an
        # RPC failure mid-call, or a test fake that returns a MagicMock for
        # ``eth.block_number``), drop the staleness check rather than serve a
        # cache we can't reason about.
        # Any failure reading block_number (RPC error, MagicMock fixture in
        # tests, etc.) falls through to a fresh on-chain read — better than
        # serving a cache we can't reason about. Catch broadly because RPC
        # backends throw a wide range of exceptions and we don't want a
        # transient infrastructure issue to break the pUSD path.
        try:
            raw_block = await asyncio.to_thread(lambda: web3.eth.block_number)
            current_block = int(raw_block)
        except Exception:  # noqa: BLE001
            current_block = None

        cache_fresh = (
            self._cached_pusd_balance is not None
            and self._cached_pusd_balance >= min_pusd_units
            and current_block is not None
            and self._cached_pusd_balance_block is not None
            and (current_block - self._cached_pusd_balance_block) <= PUSD_CACHE_STALE_BLOCKS
        )
        if cache_fresh:
            return self._cached_pusd_balance  # type: ignore[return-value]

        balance = await asyncio.to_thread(ctf.get_pusd_balance, wallet, web3)
        self._cached_pusd_balance = balance
        self._cached_pusd_balance_block = current_block
        return balance

    async def _fetch_market_for_token(self, client: ClobClient, token_id: str) -> GammaMarket:
        """Resolve the GammaMarket that owns ``token_id``.

        V2 ``build_limit_order`` / ``build_market_order`` require a
        ``GammaMarket`` for tick-size + min-size validation AND neg-risk
        exchange routing (``market.neg_risk`` chooses CTFv2 vs NegRisk V2).
        """
        markets = await asyncio.to_thread(client.get_markets, MarketFilters(clob_token_ids=[token_id], limit=1))
        if not markets:
            raise ValueError(f"No Polymarket market found for token_id={token_id}")
        return markets[0]

    @staticmethod
    def _required_pusd_units_for_buy(price: str, size: str) -> int:
        """Compute pUSD token units (6 decimals) required to cover a BUY order.

        BUY: maker spends pUSD = price × size. SELL: maker spends shares,
        no pUSD needed (returns 0).
        """
        try:
            usd = (Decimal(price) * Decimal(size)).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            if usd <= 0:
                return 0
            return int((usd * (10**6)).to_integral_value())
        except (InvalidOperation, ValueError):
            return 0

    # =========================================================================
    # L2 Authentication (HMAC-SHA256)
    # =========================================================================

    def _build_l2_signature(self, method: str, path: str, timestamp: str, body: str = "") -> str:
        """Build HMAC-SHA256 signature for L2 authentication.

        Raises:
            ValueError: If api_secret is not valid base64
        """
        message = f"{timestamp}{method}{path}{body}"
        try:
            secret_bytes = base64.b64decode(self._api_secret)  # type: ignore[arg-type]
        except Exception as e:
            err_msg = f"Invalid Polymarket API secret: not valid base64 - {e}"
            raise ValueError(err_msg) from e
        signature = hmac.new(
            secret_bytes,
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(signature).decode("utf-8")

    def _build_l2_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        """Build L2 authentication headers."""
        missing = []
        if not self._wallet_address:
            missing.append("wallet_address")
        if not self._api_key:
            missing.append("api_key")
        if not self._api_secret:
            missing.append("api_secret")
        if not self._api_passphrase:
            missing.append("api_passphrase")
        if missing:
            raise ValueError(f"Polymarket L2 credentials missing: {', '.join(missing)}")

        timestamp = str(int(time.time()))
        signature = self._build_l2_signature(method, path, timestamp, body)

        return {
            "POLY_ADDRESS": str(self._wallet_address),
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": str(self._api_key),
            "POLY_PASSPHRASE": str(self._api_passphrase),
        }

    # =========================================================================
    # HTTP Helpers
    # =========================================================================

    async def _request(
        self,
        method: str,
        base_url: str,
        endpoint: str,
        params: dict | None = None,
        json_body: dict | None = None,
        authenticated: bool = False,
    ) -> tuple[bool, dict | None, str | None]:
        """Make HTTP request.

        Returns:
            Tuple of (success, data, error)
        """
        session = await self._get_session()
        url = f"{base_url}{endpoint}"

        headers = {"Content-Type": "application/json"}

        path = endpoint
        if params:
            path = f"{path}?{urlencode(params)}"

        body = ""
        if json_body:
            body = json.dumps(json_body, separators=(",", ":"))

        if authenticated:
            if not await self._ensure_credentials():
                return False, None, "Polymarket credentials not configured"
            try:
                auth_headers = self._build_l2_headers(method, path, body)
            except ValueError as e:
                return False, None, str(e)
            headers.update(auth_headers)

        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                data=body if json_body else None,
                headers=headers,
            ) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError, ValueError) as e:
                        return False, None, f"JSON parse error: {e}"
                    return True, data, None
                else:
                    error_text = await response.text()
                    return False, None, f"HTTP {response.status}: {error_text[:500]}"
        except (TimeoutError, aiohttp.ClientError) as e:
            return False, None, str(e)

    # =========================================================================
    # Market Data RPCs
    # =========================================================================

    @staticmethod
    def _market_response_from_gamma(data: dict) -> gateway_pb2.PolymarketMarketResponse:
        outcomes_raw = data.get("outcomes")
        outcome_prices_raw = data.get("outcomePrices")
        token_ids_raw = data.get("clobTokenIds")
        tags_raw = data.get("tags")

        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else list(outcomes_raw or [])
        except (TypeError, ValueError):
            outcomes = []
        try:
            outcome_prices = (
                [str(value) for value in json.loads(outcome_prices_raw)]
                if isinstance(outcome_prices_raw, str)
                else [str(value) for value in (outcome_prices_raw or [])]
            )
        except (TypeError, ValueError):
            outcome_prices = []
        try:
            token_ids = (
                [str(value) for value in json.loads(token_ids_raw)]
                if isinstance(token_ids_raw, str)
                else [str(value) for value in (token_ids_raw or [])]
            )
        except (TypeError, ValueError):
            token_ids = []
        try:
            tags = json.loads(tags_raw) if isinstance(tags_raw, str) else list(tags_raw or [])
        except (TypeError, ValueError):
            tags = []

        return gateway_pb2.PolymarketMarketResponse(
            condition_id=data.get("conditionId", ""),
            question_id=data.get("questionID", data.get("questionId", "")),
            tokens=token_ids,
            active=data.get("active", False),
            closed=data.get("closed", False),
            accepting_orders=data.get("acceptingOrders", data.get("active", False)),
            minimum_order_size=str(data.get("orderMinSize", "5")),
            minimum_tick_size=str(data.get("orderPriceMinTickSize", "0.01")),
            success=True,
            market_id=str(data.get("id", "")),
            question=data.get("question", ""),
            slug=data.get("slug", ""),
            outcomes=outcomes,
            outcome_prices=outcome_prices,
            clob_token_ids=token_ids,
            volume=str(data.get("volume", "0")),
            volume_24hr=str(data.get("volume24hr", "0")),
            liquidity=str(data.get("liquidity", "0")),
            end_date=data.get("endDate", ""),
            enable_order_book=data.get("enableOrderBook", False),
            maker_base_fee_bps=str(data.get("makerBaseFee", "0")),
            taker_base_fee_bps=str(data.get("takerBaseFee", "0")),
            best_bid=str(data.get("bestBid", "")),
            best_ask=str(data.get("bestAsk", "")),
            last_trade_price=str(data.get("lastTradePrice", "")),
            event_id=str(data.get("eventId", "")),
            event_slug=data.get("eventSlug", ""),
            group_slug=data.get("groupItemSlug", data.get("group_slug", "")),
            tags=[str(tag) for tag in tags],
            raw_json=json.dumps(data, separators=(",", ":")),
        )

    async def GetMarket(
        self,
        request: gateway_pb2.PolymarketGetMarketRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketResponse:
        """Get market by slug, market ID, or condition ID."""
        if request.slug:
            success, data, error = await self._request(
                "GET",
                GAMMA_BASE_URL,
                "/markets",
                params={"slug": request.slug, "limit": "1"},
            )
            if not success:
                return gateway_pb2.PolymarketMarketResponse(success=False, error=error or "Market not found")
            items: list[dict] = data if isinstance(data, list) else []
            if not items:
                return gateway_pb2.PolymarketMarketResponse(success=False, error="Market not found")
            return self._market_response_from_gamma(items[0])

        success, data, error = await self._request(
            "GET",
            GAMMA_BASE_URL,
            f"/markets/{request.condition_id}",
        )
        if success and isinstance(data, dict):
            return self._market_response_from_gamma(data)

        success, data, error = await self._request(
            "GET",
            GAMMA_BASE_URL,
            "/markets",
            params={"condition_ids": request.condition_id, "limit": "1"},
        )
        if not success:
            return gateway_pb2.PolymarketMarketResponse(success=False, error=error or "Market not found")
        items = data if isinstance(data, list) else []
        if not items:
            return gateway_pb2.PolymarketMarketResponse(success=False, error="Market not found")
        return self._market_response_from_gamma(items[0])

    async def GetMarkets(
        self,
        request: gateway_pb2.PolymarketGetMarketsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMarketsResponse:
        """Get list of markets from the Gamma API."""
        if request.next_cursor:
            return gateway_pb2.PolymarketMarketsResponse(
                success=False,
                error="Cursor pagination is not yet supported by GetMarkets",
            )
        params: dict[str, str] = {}
        if request.filters_json:
            try:
                raw_filters = json.loads(request.filters_json)
            except json.JSONDecodeError:
                return gateway_pb2.PolymarketMarketsResponse(success=False, error="Invalid filters_json")
            for key, value in raw_filters.items():
                if value is None:
                    continue
                if isinstance(value, list):
                    params[key] = ",".join(str(item) for item in value)
                elif isinstance(value, bool):
                    params[key] = str(value).lower()
                else:
                    params[key] = str(value)

        success, data, error = await self._request("GET", GAMMA_BASE_URL, "/markets", params=params or None)

        if not success:
            return gateway_pb2.PolymarketMarketsResponse(success=False, error=error or "")
        items: list[dict] = data if isinstance(data, list) else []
        markets = [self._market_response_from_gamma(item) for item in items]

        return gateway_pb2.PolymarketMarketsResponse(
            markets=markets,
            next_cursor="",
            success=True,
        )

    async def GetSimplifiedMarkets(
        self,
        request: gateway_pb2.PolymarketGetSimplifiedMarketsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketSimplifiedMarketsResponse:
        """Get simplified market list."""
        params = {}
        if request.next_cursor:
            params["next_cursor"] = request.next_cursor

        success, data, error = await self._request("GET", CLOB_BASE_URL, "/simplified-markets", params=params)

        if not success:
            return gateway_pb2.PolymarketSimplifiedMarketsResponse(success=False, error=error or "")

        # Guard against data being None (e.g., JSON null response)
        if data is None:
            items = []
        elif isinstance(data, list):
            items = data
        else:
            items = data.get("data", [])

        markets = []
        for item in items:
            markets.append(
                gateway_pb2.PolymarketSimplifiedMarket(
                    condition_id=item.get("condition_id", ""),
                    tokens=[str(t) for t in item.get("tokens", [])],
                    min_incentive_size=str(item.get("min_incentive_size", "0")),
                    max_incentive_spread=str(item.get("max_incentive_spread", "0")),
                    active=item.get("active", False),
                    closed=item.get("closed", False),
                )
            )

        next_cursor = ""
        if isinstance(data, dict):
            next_cursor = data.get("next_cursor", "")

        return gateway_pb2.PolymarketSimplifiedMarketsResponse(
            markets=markets,
            next_cursor=next_cursor,
            success=True,
        )

    # =========================================================================
    # Order Book RPCs
    # =========================================================================

    async def GetOrderBook(
        self,
        request: gateway_pb2.PolymarketOrderBookRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderBookResponse:
        """Get order book for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/book",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketOrderBookResponse(success=False, error=error or "Order book not found")

        bids = [
            gateway_pb2.PolymarketOrderBookLevel(price=str(b.get("price", "0")), size=str(b.get("size", "0")))
            for b in data.get("bids", [])
        ]
        asks = [
            gateway_pb2.PolymarketOrderBookLevel(price=str(a.get("price", "0")), size=str(a.get("size", "0")))
            for a in data.get("asks", [])
        ]

        return gateway_pb2.PolymarketOrderBookResponse(
            market=data.get("market", ""),
            asset_id=data.get("asset_id", ""),
            hash=data.get("hash", ""),
            timestamp=data.get("timestamp", 0),
            bids=bids,
            asks=asks,
            success=True,
        )

    async def GetMidpoint(
        self,
        request: gateway_pb2.PolymarketMidpointRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketMidpointResponse:
        """Get midpoint price for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/midpoint",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketMidpointResponse(success=False, error=error or "Midpoint not found")

        return gateway_pb2.PolymarketMidpointResponse(
            midpoint=str(data.get("mid", "0")),
            success=True,
        )

    async def GetPrice(
        self,
        request: gateway_pb2.PolymarketPriceRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPriceResponse:
        """Get price for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/price",
            params={"token_id": request.token_id, "side": request.side},
        )

        if not success or not data:
            return gateway_pb2.PolymarketPriceResponse(success=False, error=error or "Price not found")

        return gateway_pb2.PolymarketPriceResponse(
            price=str(data.get("price", "0")),
            success=True,
        )

    async def GetSpread(
        self,
        request: gateway_pb2.PolymarketSpreadRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketSpreadResponse:
        """Get spread for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/spread",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketSpreadResponse(success=False, error=error or "Spread not found")

        return gateway_pb2.PolymarketSpreadResponse(
            spread=str(data.get("spread", "0")),
            success=True,
        )

    async def GetTickSize(
        self,
        request: gateway_pb2.PolymarketTickSizeRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketTickSizeResponse:
        """Get tick size for a token."""
        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/tick-size",
            params={"token_id": request.token_id},
        )

        if not success or not data:
            return gateway_pb2.PolymarketTickSizeResponse(success=False, error=error or "Tick size not found")

        return gateway_pb2.PolymarketTickSizeResponse(
            tick_size=str(data.get("minimum_tick_size", "0.01")),
            success=True,
        )

    # =========================================================================
    # Order Management RPCs
    # =========================================================================

    async def CreateAndPostOrder(
        self,
        request: gateway_pb2.PolymarketCreateOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderResponse:
        """Create and post a limit order via the gateway-owned signer."""
        if not self._available:
            return gateway_pb2.PolymarketOrderResponse(
                success=False,
                error="Polymarket signer not configured in gateway",
            )

        try:
            price = Decimal(request.price)
            size = Decimal(request.size)
            side = request.side.upper()
            if side not in ("BUY", "SELL"):
                return gateway_pb2.PolymarketOrderResponse(
                    success=False,
                    error=f"Invalid side '{request.side}': must be 'BUY' or 'SELL'",
                )

            client = await self._build_authenticated_client()
            try:
                # V2 build_limit_order requires a GammaMarket for tick + neg-risk routing.
                # Resolve the market BEFORE running on-chain wallet setup —
                # _ensure_wallet_ready submits real approvals/wraps that mutate
                # state and burn gas, so a typoed/unknown token_id must fail
                # fast here instead of after we've already paid for setup txs.
                market = await self._fetch_market_for_token(client, request.token_id)

                # V2 on-chain wallet auto-setup. For BUY, pre-flight wraps source
                # asset → pUSD if the wallet doesn't hold enough collateral. SELL
                # consumes shares (CTF), so pUSD isn't required — but allowances
                # still need to be in place for the V2 exchange to pull shares.
                # Returns a request-scoped list of setup-tx records so concurrent
                # order RPCs cannot leak attribution into each other's responses.
                min_pusd = self._required_pusd_units_for_buy(request.price, request.size) if side == "BUY" else 0
                setup_txs_records = await self._ensure_wallet_ready(min_pusd_units=min_pusd)

                response = await asyncio.to_thread(
                    client.create_and_post_order,
                    token_id=request.token_id,
                    price=price,
                    size=size,
                    side=side,
                    market=market,
                    time_in_force=request.time_in_force or "GTC",
                    expiration=request.expiration if request.expiration > 0 else 0,
                )
            finally:
                client.close()
            # VIB-3710: setup_txs_records is the request-scoped list owned by
            # this RPC invocation — no shared mutable state to drain, so each
            # order's response carries only the gas it actually paid for.
            setup_txs_proto = [
                gateway_pb2.PolymarketSetupTx(
                    tx_hash=record["tx_hash"],
                    description=record["description"],
                    gas_used=int(record["gas_used"]),
                    gas_price_wei=record["gas_price_wei"],
                    total_cost_wei=record["total_cost_wei"],
                )
                for record in setup_txs_records
            ]
            # VIB-3710: surface operator fee from the OrderResponse model. The
            # underlying ClobClient.create_and_post_order parses POST /order's
            # raw JSON into OrderResponse; ``fee_pusd`` reads either the
            # explicit ``fee_pusd`` field or the legacy ``fee`` field on that
            # response (see OrderResponse.from_api_response). When neither is
            # present (orders that have not yet matched, or a CLOB API that
            # omits the field), surface "" so the wire shape is unambiguous —
            # the strategy-side parser maps "" to None.
            fee_pusd_response = getattr(response, "fee_pusd", None)
            fee_pusd_str = str(fee_pusd_response) if fee_pusd_response is not None else ""
            return gateway_pb2.PolymarketOrderResponse(
                order_id=response.order_id,
                status=response.status.value,
                size_matched=str(response.filled_size),
                price=str(response.price),
                size=str(response.size),
                avg_fill_price=str(response.avg_fill_price) if response.avg_fill_price is not None else "",
                created_at=response.created_at.isoformat() if response.created_at else "",
                success=True,
                setup_txs=setup_txs_proto,
                fee_pusd=fee_pusd_str,
            )
        except (InvalidOperation, ValueError) as e:
            # No shared ledger to drain — setup_txs_records is request-scoped
            # and was either consumed in the response above or is unreferenced
            # if we never reached the success branch. Any setup txs that
            # confirmed before this exception are local to this call only.
            return gateway_pb2.PolymarketOrderResponse(success=False, error=str(e))
        except Exception as e:
            logger.exception("Failed to create order through gateway Polymarket client")
            return gateway_pb2.PolymarketOrderResponse(success=False, error=str(e))

    async def CreateAndPostMarketOrder(
        self,
        request: gateway_pb2.PolymarketMarketOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderResponse:
        """Create and post a market order against the current top-of-book.

        V2 NOTE — implemented as an FOK limit order. Polymarket's CLOB has no
        separate "market order" primitive; "market" semantics are produced by
        sending a Fill-or-Kill limit at the current best cross-side price.
        Either the entire size matches at-or-better than that price within the
        single match cycle, or nothing matches and the order is killed.

        Cross-side pricing — Polymarket CLOB convention (verified against
        ``ClobClient.get_price`` and ``OrderBook.best_bid`` / ``best_ask`` in
        ``almanak/framework/connectors/polymarket/{clob_client,models}.py``):

            ``GET /price?side=BUY``  -> best BID  (highest buyer's price)
            ``GET /price?side=SELL`` -> best ASK  (lowest seller's price)

        The ``side`` parameter names "the side of the book to read from", not
        "the trade direction of the caller". So to price a market BUY (which
        crosses the ASK / lifts an offer) we must call ``side=SELL``; to price
        a market SELL (which crosses the BID / hits a bid) we must call
        ``side=BUY``. This is the opposite of an intuitive ``price_side =
        side`` mapping — that mistake samples the wrong side of the spread
        and silently lets the worst_price guard pass even when the executable
        price is far worse.

        ``worst_price`` enforcement is two-layer:
            (a) Submission-time guard: the sampled top-of-book price must be
                at least as good as ``worst_price``, otherwise we never sign
                or submit. This is a single-level / single-sample check — it
                does NOT walk the book, so it cannot catch slippage from
                depth being thinner than ``size``.
            (b) Match-time guard: the FOK semantics on the CLOB ensure the
                whole order fills at-or-better than the limit price (which
                is the sampled top-of-book). If the book moved between
                sample and match, the FOK kills rather than partially
                filling at a worse price.

        Args:
            request: PolymarketMarketOrderRequest. ``amount`` is denominated
                in pUSD for BUY (converted to token size by dividing by the
                sampled price, ``ROUND_DOWN``) and in tokens for SELL.
                ``worst_price`` is optional but recommended.
        """
        if not self._available:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Polymarket not configured")

        # Validate side explicitly
        side = request.side.upper() if request.side else ""
        if side not in ("BUY", "SELL"):
            return gateway_pb2.PolymarketOrderResponse(
                success=False, error=f"Invalid side: must be BUY or SELL, got '{request.side}'"
            )

        # Parse and validate amount
        try:
            amount = Decimal(request.amount)
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(
                success=False, error=f"Invalid amount format: '{request.amount}'"
            )
        if amount <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Amount must be positive")

        # Cross-side price sampling. See the docstring: a market BUY needs the
        # ASK, which Polymarket returns from ``/price?side=SELL``. A market
        # SELL needs the BID, returned from ``/price?side=BUY``. Hence the
        # swap below — this is INTENTIONAL, not a typo.
        price_side = "SELL" if side == "BUY" else "BUY"

        price_success, price_data, price_error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/price",
            params={"token_id": request.token_id, "side": price_side},
        )

        if not price_success or not price_data:
            return gateway_pb2.PolymarketOrderResponse(success=False, error=price_error or "Could not get price")

        # Parse price from API response
        try:
            price = Decimal(str(price_data.get("price", "0")))
        except InvalidOperation:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Invalid price format from API")

        # Validate price is positive before using it for calculations
        if price <= 0:
            return gateway_pb2.PolymarketOrderResponse(success=False, error="Invalid price: price must be positive")

        # worst_price guard — submission-time check on the sampled top-of-book
        # price (single-level, NOT depth-aware). Match-time FOK semantics on
        # the CLOB enforce the same bound on every fill of this order. See
        # the docstring for the two-layer rationale.
        if request.worst_price:
            try:
                worst = Decimal(request.worst_price)
            except InvalidOperation:
                return gateway_pb2.PolymarketOrderResponse(
                    success=False, error=f"Invalid worst_price format: '{request.worst_price}'"
                )
            if side == "BUY" and price > worst:
                return gateway_pb2.PolymarketOrderResponse(
                    success=False,
                    error=f"Best ask {price} exceeds worst_price {worst} for BUY",
                )
            if side == "SELL" and price < worst:
                return gateway_pb2.PolymarketOrderResponse(
                    success=False,
                    error=f"Best bid {price} below worst_price {worst} for SELL",
                )

        # For market orders, request.amount semantics differ by side:
        # - BUY: amount is in USDC, need to convert to token size
        # - SELL: amount is in tokens (size)
        if side == "BUY":
            # Convert USDC amount to token size by dividing by price
            # Round down to avoid overspending
            token_size = (amount / price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
            size_str = str(token_size)
        else:
            # SELL: amount is already in tokens (use parsed Decimal for consistency)
            size_str = str(amount)

        # Create the order with the current market price.
        # V2: ``fee_rate_bps`` and on-chain ``nonce`` are gone (operator-set
        # fees, ``timestamp`` replaces nonce). Proto fields kept for wire
        # compat but not threaded through.
        create_request = gateway_pb2.PolymarketCreateOrderRequest(
            token_id=request.token_id,
            price=str(price),
            size=size_str,
            side=side,
            expiration=request.expiration,
            time_in_force="FOK",  # Market orders use Fill-or-Kill
        )

        return await self.CreateAndPostOrder(create_request, context)

    async def CancelOrder(
        self,
        request: gateway_pb2.PolymarketCancelOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel a single order."""
        try:
            client = await self._build_authenticated_client()
            try:
                await asyncio.to_thread(client.cancel_order, request.order_id)
            finally:
                client.close()
            return gateway_pb2.PolymarketCancelResponse(canceled=[request.order_id], not_canceled=[], success=True)
        except Exception as e:
            return gateway_pb2.PolymarketCancelResponse(
                canceled=[],
                not_canceled=[request.order_id],
                success=False,
                error=str(e),
            )

    async def CancelOrders(
        self,
        request: gateway_pb2.PolymarketCancelOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel multiple orders."""
        canceled: list[str] = []
        not_canceled: list[str] = []
        client = await self._build_authenticated_client()
        try:
            for order_id in request.order_ids:
                try:
                    await asyncio.to_thread(client.cancel_order, order_id)
                    canceled.append(order_id)
                except Exception:
                    not_canceled.append(order_id)
        finally:
            client.close()
        return gateway_pb2.PolymarketCancelResponse(
            canceled=canceled, not_canceled=not_canceled, success=not not_canceled
        )

    async def CancelAll(
        self,
        request: gateway_pb2.PolymarketCancelAllRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketCancelResponse:
        """Cancel all orders, optionally scoped to market_id and/or asset_id."""
        client = await self._build_authenticated_client()
        try:
            open_orders = await asyncio.to_thread(
                client.get_open_orders, OrderFilters(market=request.market_id or None)
            )
            # Apply asset_id filter client-side (OpenOrder.market stores the token/asset id).
            if request.asset_id:
                open_orders = [o for o in open_orders if o.market == request.asset_id]
            order_ids = [order.order_id for order in open_orders]
            if order_ids:
                await asyncio.to_thread(client.cancel_orders, order_ids)
            return gateway_pb2.PolymarketCancelResponse(canceled=order_ids, not_canceled=[], success=True)
        except Exception as e:
            return gateway_pb2.PolymarketCancelResponse(success=False, error=str(e))
        finally:
            client.close()

    # =========================================================================
    # Position and Trade RPCs
    # =========================================================================

    async def GetPositions(
        self,
        _request: gateway_pb2.PolymarketGetPositionsRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPositionsResponse:
        """Get positions for the wallet."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(client.get_positions)
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketPositionsResponse(success=False, error=str(e))

        positions = [
            gateway_pb2.PolymarketPosition(
                asset=p.token_id,
                condition_id=p.condition_id,
                size=str(p.size),
                avg_price=str(p.avg_price),
                realized_pnl=str(p.realized_pnl),
                cur_price=str(p.current_price),
                market_id=p.market_id,
                token_id=p.token_id,
                outcome=p.outcome,
                market_question=p.market_question,
            )
            for p in data
        ]
        return gateway_pb2.PolymarketPositionsResponse(positions=positions, success=True)

    async def GetOpenOrders(
        self,
        request: gateway_pb2.PolymarketGetOpenOrdersRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOpenOrdersResponse:
        """Get open orders."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(
                    client.get_open_orders,
                    OrderFilters(market=request.market_id or None),
                )
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketOpenOrdersResponse(success=False, error=str(e))

        orders = [
            gateway_pb2.PolymarketOpenOrder(
                order_id=o.order_id,
                market=o.market,
                side=o.side,
                price=str(o.price),
                original_size=str(o.size),
                size_matched=str(o.filled_size),
                expiration=str(o.expiration or ""),
                created_at=o.created_at.isoformat() if o.created_at else "",
            )
            for o in data
        ]
        return gateway_pb2.PolymarketOpenOrdersResponse(orders=orders, success=True)

    async def GetTradesHistory(
        self,
        request: gateway_pb2.PolymarketGetTradesRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketTradesResponse:
        """Get trade history."""
        params = {}
        if request.market_id:
            params["market"] = request.market_id
        if request.asset_id:
            params["asset_id"] = request.asset_id
        if request.limit > 0:
            params["limit"] = str(request.limit)
        if request.before:
            params["before"] = request.before
        if request.after:
            params["after"] = request.after

        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/trades",
            params=params if params else None,
            authenticated=True,
        )

        if not success:
            return gateway_pb2.PolymarketTradesResponse(success=False, error=error or "")

        trades = []
        trade_list = data if isinstance(data, list) else data.get("data", []) if data else []
        for t in trade_list:
            trades.append(
                gateway_pb2.PolymarketTrade(
                    trade_id=t.get("id", t.get("trade_id", "")),
                    market=t.get("market", ""),
                    asset_id=t.get("asset_id", ""),
                    side=t.get("side", ""),
                    price=str(t.get("price", "0")),
                    size=str(t.get("size", "0")),
                    fee_rate_bps=str(t.get("fee_rate_bps", "0")),
                    status=t.get("status", ""),
                    match_time=t.get("match_time", ""),
                    transaction_hash=t.get("transaction_hash", ""),
                    bucket_index=str(t.get("bucket_index", "")),
                )
            )

        next_cursor = ""
        if isinstance(data, dict):
            next_cursor = data.get("next_cursor", "")

        return gateway_pb2.PolymarketTradesResponse(trades=trades, next_cursor=next_cursor, success=True)

    async def GetOrder(
        self,
        request: gateway_pb2.PolymarketGetOrderRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketOrderInfoResponse:
        """Get a specific order by ID."""
        try:
            client = await self._build_authenticated_client()
            try:
                data = await asyncio.to_thread(client.get_order, request.order_id)
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketOrderInfoResponse(success=False, error=str(e))
        if data is None:
            return gateway_pb2.PolymarketOrderInfoResponse(success=False, error="Order not found")
        return gateway_pb2.PolymarketOrderInfoResponse(
            order_id=data.order_id,
            market=data.market,
            side=data.side,
            price=str(data.price),
            original_size=str(data.size),
            size_matched=str(data.filled_size),
            expiration=str(data.expiration or ""),
            created_at=data.created_at.isoformat() if data.created_at else "",
            success=True,
        )

    # =========================================================================
    # Historical Data RPCs (VIB-3695)
    # =========================================================================

    async def GetPriceHistory(
        self,
        request: gateway_pb2.PolymarketGetPriceHistoryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketPriceHistoryResponse:
        """Proxy ``ClobClient.get_price_history`` (public ``/prices-history``).

        Mutual-exclusion of ``interval`` vs ``start_ts``+``end_ts`` is enforced
        at this gateway boundary (per the security model: never delegate
        argument validation to downstream layers) — invalid inputs map to
        ``INVALID_ARGUMENT`` rather than the generic upstream ``ValueError``.

        Public endpoint — uses ``_build_client(require_signer=False)`` so a
        market-data-only gateway (no Polymarket signer wired) can still serve
        this RPC. ``ClobClient.get_price_history`` is a plain HTTP fetch and
        never reads ``wallet_address`` / ``private_key`` from the config.
        """
        # Treat zero-valued proto fields as "not set" so callers can omit them
        # (proto3 has no `optional` distinction for primitives by default and
        # we deliberately did not mark them ``optional`` to keep the wire
        # format simple).
        interval = request.interval or None
        start_ts = request.start_ts or None
        end_ts = request.end_ts or None
        fidelity = request.fidelity or None

        # Validate at the gateway boundary. ``interval`` and ``start_ts``+``end_ts``
        # are mutually exclusive at the source endpoint; surface a clean
        # ``INVALID_ARGUMENT`` rather than letting the upstream 400 leak as a
        # generic exception. Partial range (one of start_ts/end_ts set without
        # the other) is also rejected since the upstream silently treats it as
        # a half-bounded range that can return wildly inconsistent windows.
        if not request.token_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "token_id is required")
        if interval is not None and (start_ts is not None or end_ts is not None):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "interval and start_ts/end_ts are mutually exclusive",
            )
        if (start_ts is None) != (end_ts is None):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "start_ts and end_ts must both be set (or both omitted)",
            )

        try:
            client = self._build_client(require_signer=False)
            try:
                history = await asyncio.to_thread(
                    client.get_price_history,
                    request.token_id,
                    interval,
                    start_ts,
                    end_ts,
                    fidelity,
                )
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketPriceHistoryResponse(success=False, error=str(e))

        prices = [
            gateway_pb2.PolymarketHistoricalPrice(
                timestamp=int(p.timestamp.timestamp()),
                price=str(p.price),
            )
            for p in history.prices
        ]
        return gateway_pb2.PolymarketPriceHistoryResponse(
            token_id=history.token_id,
            interval=history.interval,
            prices=prices,
            start_time=int(history.start_time.timestamp()) if history.start_time else 0,
            end_time=int(history.end_time.timestamp()) if history.end_time else 0,
            success=True,
        )

    async def GetTradeTape(
        self,
        request: gateway_pb2.PolymarketGetTradeTapeRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketTradeTapeResponse:
        """Proxy ``ClobClient.get_trade_tape`` (authenticated ``/data/trades``).

        Authenticated upstream path: use ``_build_authenticated_client`` so the
        gateway's API credentials are attached. ``token_id`` and ``limit`` are
        both optional on the wire; default ``limit`` to 100 to match the SDK
        when the caller leaves the field unset.

        ``limit`` is validated at the gateway boundary against the upstream's
        documented cap (500). Out-of-range values map to ``INVALID_ARGUMENT``
        instead of leaking as a generic upstream error.
        """
        # Validate at the gateway boundary; the upstream caps at 500 (per the
        # proto comment) but doesn't always fail loudly on overshoot, so we
        # reject here. Negative values are rejected (request.limit is int32).
        if request.limit < 0 or request.limit > _TRADE_TAPE_LIMIT_MAX:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"limit must be in [0, {_TRADE_TAPE_LIMIT_MAX}], got {request.limit}",
            )

        try:
            token_id = request.token_id or None
            limit = request.limit if request.limit > 0 else 100

            client = await self._build_authenticated_client()
            try:
                trades = await asyncio.to_thread(client.get_trade_tape, token_id, limit)
            finally:
                client.close()
        except Exception as e:
            return gateway_pb2.PolymarketTradeTapeResponse(success=False, error=str(e))

        proto_trades = [
            gateway_pb2.PolymarketHistoricalTrade(
                id=t.id,
                token_id=t.token_id,
                side=t.side,
                price=str(t.price),
                size=str(t.size),
                timestamp=int(t.timestamp.timestamp()),
                maker=t.maker or "",
                taker=t.taker or "",
                # The model only carries id/token_id/side/price/size/ts/
                # maker/taker today; the proto reserves market_id / asset_id /
                # outcome for future provider-side enrichment without forcing
                # another proto rev.
                market_id="",
                asset_id=t.token_id,
                outcome="",
            )
            for t in trades
        ]
        return gateway_pb2.PolymarketTradeTapeResponse(trades=proto_trades, success=True)

    # =========================================================================
    # Balance RPCs
    # =========================================================================

    async def GetBalanceAllowance(
        self,
        request: gateway_pb2.PolymarketBalanceAllowanceRequest,
        _context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PolymarketBalanceAllowanceResponse:
        """Get balance and allowance."""
        params = {"asset_type": request.asset_type or "COLLATERAL"}
        if request.token_id:
            params["token_id"] = request.token_id

        success, data, error = await self._request(
            "GET",
            CLOB_BASE_URL,
            "/balance-allowance",
            params=params,
            authenticated=True,
        )

        if not success or not data:
            return gateway_pb2.PolymarketBalanceAllowanceResponse(success=False, error=error or "Could not get balance")

        return gateway_pb2.PolymarketBalanceAllowanceResponse(
            balance=str(data.get("balance", "0")),
            allowance=str(data.get("allowance", "0")),
            success=True,
        )
