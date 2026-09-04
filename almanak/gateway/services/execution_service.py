"""ExecutionService implementation - handles intent compilation and execution.

This service provides intent compilation and transaction execution for strategy
containers via gRPC. All signing, simulation, and submission happens here in
the gateway; strategy containers never see private keys.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import grpc
import pydantic

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.execution.solana.route_refresh import (
    SolanaRouteRefresher,
    SolanaRouteRefreshRequest,
    SolanaRouteRefreshResult,
)
from almanak.framework.execution.submission import (
    ReplayPolicy,
    SubmissionProvenance,
    SubmissionTransactionEvidence,
    TransactionRole,
    certify_submission_transactions,
    execution_plan_hash,
)
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ValidationError,
    validate_chain,
    validate_tx_hash,
)

logger = logging.getLogger(__name__)

# TTL for cached compilers (5 minutes) - prevents stale price data in long-running services
COMPILER_CACHE_TTL_SECONDS = 300

# Intent types that require real prices on mainnet (VIB-523).
# Normalized: uppercase, underscores stripped, so both "lp_open" and "lpopen" match.
PRICE_SENSITIVE_INTENT_TYPES = frozenset(
    {
        "SWAP",
        "LPOPEN",
        "LPCLOSE",
        "SUPPLY",
        "REPAY",
        "BORROW",
        "WITHDRAW",
        "PERPOPEN",
        "PERPCLOSE",
        # ALM-3183: FLASH_LOAN was absent, so a flash loan with an empty
        # price_map skipped the gate entirely and its nested swap callbacks
        # sized amountOutMinimum against placeholder prices. The callbacks are
        # covered without extra recursion here because
        # ``_extract_token_symbols_from_intent`` delegates to the shared
        # ``extract_token_symbols``, which already walks ``callback_intents`` --
        # so the gate self-serves (or fails closed on) the callback legs too.
        "FLASHLOAN",
    }
)

# Close-type intents whose payload legitimately carries no token symbols
# (LPCloseIntent has only position_id/pool; PerpCloseIntent may too), so the
# mainnet price gate cannot self-serve prices for them. VIB-6301: these are
# allowed to compile without prices — refusing would strand capital, since an
# empty extraction is the *normal* case for an LP close and teardown must never
# be blocked from reducing on-chain risk (blueprint 14) — but they compile with
# a real-but-empty oracle, never with fabricated placeholder prices.
#
# Deny-list failure mode after VIB-6301: a new close verb that is added to
# PRICE_SENSITIVE_INTENT_TYPES but not here fails *closed* (safe) instead of
# inheriting a fabricated oracle. Must remain a subset of
# PRICE_SENSITIVE_INTENT_TYPES — the gate never reaches it otherwise.
PRICE_OPTIONAL_CLOSE_INTENT_TYPES = frozenset({"LPCLOSE", "PERPCLOSE"})
_SYNTHETIC_PEG_PRICE_SOURCES = frozenset({"stablecoin_peg", "stablecoin_fallback"})


@dataclass(frozen=True)
class _FetchedPriceBatch:
    """Self-served prices plus provenance needed by the compiler boundary."""

    prices: dict[str, Decimal]
    sources: dict[str, str]
    peg_tokens: frozenset[str]


class ReceiptSetSerializationError(RuntimeError):
    """The gateway could not preserve one receipt per transaction result."""


def _submission_provenance_to_proto(value: Any) -> gateway_pb2.SubmissionProvenance.ValueType:
    """Serialize provenance without upgrading missing/unknown evidence."""
    parsed = SubmissionProvenance.parse(value)
    return {
        SubmissionProvenance.NOT_ATTEMPTED: gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED,
        SubmissionProvenance.ATTEMPTED: gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED,
    }.get(parsed, gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED)


def _submission_transactions_to_proto(
    values: list[SubmissionTransactionEvidence],
) -> list[gateway_pb2.SubmissionTransactionEvidence]:
    """Serialize conservative role evidence without upgrading unknown values."""
    return [
        gateway_pb2.SubmissionTransactionEvidence(
            tx_id=item.tx_id,
            role={
                TransactionRole.SETUP_APPROVAL: gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL,
                TransactionRole.ACTION: gateway_pb2.EXECUTION_TRANSACTION_ROLE_ACTION,
            }.get(item.role, gateway_pb2.EXECUTION_TRANSACTION_ROLE_UNSPECIFIED),
            replay_policy=(
                gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY
                if item.replay_policy is ReplayPolicy.RECOMPILE_ONLY
                else gateway_pb2.REPLAY_POLICY_NEVER
            ),
        )
        for item in values
    ]


def _serialize_evm_transaction_results(transaction_results: list[Any]) -> tuple[list[str], bytes]:
    """Serialize an EVM result without ever dropping an individual receipt.

    The response's ``tx_hashes`` and ``receipts`` arrays are positional peers.
    Returning unequal cardinalities lets the strategy container associate a
    receipt with the wrong hash, or persist aggregate gas without the legs that
    substantiate it. Any missing hash, missing receipt, conversion failure, or
    non-object payload therefore fails the entire RPC result closed.
    """
    tx_hashes: list[str] = []
    receipts_data: list[dict[str, Any]] = []
    for index, transaction_result in enumerate(transaction_results):
        raw_hash = getattr(transaction_result, "tx_hash", None)
        tx_hash = raw_hash.strip() if isinstance(raw_hash, str) else ""
        if not tx_hash:
            raise ReceiptSetSerializationError(f"transaction result {index} has no non-blank tx_hash")

        receipt = getattr(transaction_result, "receipt", None)
        if receipt is None:
            raise ReceiptSetSerializationError(f"transaction result {index} ({tx_hash}) has no receipt")

        try:
            receipt_data = receipt.to_dict() if hasattr(receipt, "to_dict") else dict(receipt)
        except Exception as exc:
            raise ReceiptSetSerializationError(
                f"receipt {index} ({tx_hash}) could not be converted to a dictionary: {exc}"
            ) from exc
        if not isinstance(receipt_data, dict):
            raise ReceiptSetSerializationError(
                f"receipt {index} ({tx_hash}) serialized as {type(receipt_data).__qualname__}, expected dict"
            )

        tx_hashes.append(tx_hash)
        receipts_data.append(receipt_data)

    if len(tx_hashes) != len(receipts_data):  # pragma: no cover - construction makes this defensive
        raise ReceiptSetSerializationError(
            f"receipt-set cardinality mismatch: {len(tx_hashes)} tx hashes != {len(receipts_data)} receipts"
        )
    from almanak.framework.execution.reconciliation import complete_receipt_set_error

    if receipt_error := complete_receipt_set_error(tx_hashes, receipts_data):
        raise ReceiptSetSerializationError(receipt_error)
    try:
        receipts_bytes = json.dumps(receipts_data).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReceiptSetSerializationError(f"receipt set is not JSON serializable: {exc}") from exc
    return tx_hashes, receipts_bytes


def _known_evm_transaction_hashes(transaction_results: list[Any]) -> list[str]:
    """Return every usable hash when the receipt set cannot be trusted."""
    known_hashes: list[str] = []
    for transaction_result in transaction_results:
        raw_hash = getattr(transaction_result, "tx_hash", None)
        if isinstance(raw_hash, str) and raw_hash.strip():
            known_hashes.append(raw_hash.strip())
    return known_hashes


class _GatewaySolanaRouteRefresher:
    """Gateway-side Solana route refresher backed by connector capabilities."""

    def __init__(self) -> None:
        from almanak.connectors._base.gateway_capabilities import GatewaySolanaRouteRefreshCapability
        from almanak.connectors._gateway_registry import GATEWAY_REGISTRY

        providers = GATEWAY_REGISTRY.capability_providers(GatewaySolanaRouteRefreshCapability)  # type: ignore[type-abstract]
        self._providers: dict[str, Any] = {}
        for provider in providers:
            # ``provider.protocol`` is declared on the base ``GatewayConnector``; the capability
            # Protocol intentionally only contributes ``refresh_solana_route``. Normalize the key
            # so dispatch is case/whitespace-insensitive (mirrors the pool-reader registry).
            self._providers[str(provider.protocol).strip().lower()] = provider  # type: ignore[attr-defined]

    def refresh_route(self, request: SolanaRouteRefreshRequest) -> SolanaRouteRefreshResult:
        """Refresh a Solana route through the connector that owns it."""
        provider = self._providers.get(request.protocol.strip().lower())
        if provider is None:
            available = ", ".join(sorted(self._providers)) or "none"
            raise ValueError(
                f"No gateway Solana route refresh capability registered for protocol {request.protocol!r}. "
                f"Available: {available}"
            )
        result = provider.refresh_solana_route(request)
        if isinstance(result, SolanaRouteRefreshResult):
            return result
        if isinstance(result, dict):
            return SolanaRouteRefreshResult.from_mapping(result)
        raise TypeError(f"Solana route refresh provider returned unsupported result {type(result).__qualname__}")


class ExecutionServiceServicer(gateway_pb2_grpc.ExecutionServiceServicer):
    """Implements ExecutionService gRPC interface.

    Provides intent compilation and execution for strategy containers:
    - CompileIntent: Compile an intent into an action bundle
    - Execute: Sign, submit, and confirm transactions
    - GetTransactionStatus: Check transaction status
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize ExecutionService.

        Args:
            settings: Gateway settings with private keys and RPC config.
        """
        self.settings = settings
        self._orchestrator_cache: dict[str, object] = {}
        self._orchestrator_locks: dict[str, asyncio.Lock] = {}
        self._orchestrator_default_gas_caps: dict[str, int] = {}
        # Cache IntentCompiler per chain/wallet pair with TTL to prevent stale prices
        # Format: {cache_key: (compiler, created_timestamp)}
        self._compiler_cache: dict[str, tuple[object, float]] = {}
        self._compiler_locks: dict[str, asyncio.Lock] = {}
        self._solana_rpc_cache: dict[str, object] = {}
        self._solana_route_refresher: SolanaRouteRefresher | None = None
        self._initialized = False
        self.wallet_registry: Any = None
        self.market_servicer: object | None = None  # Set by GatewayServer after creation
        self._registered_chains: set[str] | None = None
        self._registered_chain_wallets: dict[str, str] | None = None
        # Chains for which we have already logged a public-RPC-fallback ERROR,
        # so the alert fires once per chain per gateway lifetime, not per request.
        self._public_rpc_warned_chains: set[str] = set()

    def _get_solana_route_refresher(self) -> SolanaRouteRefresher:
        """Return the gateway connector-backed Solana route refresher."""
        if self._solana_route_refresher is None:
            self._solana_route_refresher = _GatewaySolanaRouteRefresher()
        return self._solana_route_refresher

    async def _fetch_prices_for_tokens(self, tokens: list[str], chain: str) -> _FetchedPriceBatch:
        """Fetch prices from the gateway's own market service for the given tokens.

        Used as a fallback when the caller doesn't provide prices (e.g., multi-chain
        execution where the price_map isn't propagated from the market snapshot).
        The market service uses chain-agnostic sources (CoinGecko, Binance) so
        prices like USDC=$1 and WETH=$2100 are valid regardless of chain.

        Returns:
            Prices plus their sources and exact synthetic-peg identities. All
            fields are empty if the market service is unavailable.
        """
        if not self.market_servicer:
            return _FetchedPriceBatch({}, {}, frozenset())

        # Ensure the market service's price aggregator is initialized.
        # It uses lazy init (_ensure_initialized) which only runs on first
        # GetMarketSnapshot/GetPrice call. If the first price-sensitive intent
        # compiles before any market query, the aggregator would be None.
        ensure_init = getattr(self.market_servicer, "_ensure_initialized", None)
        if ensure_init:
            await ensure_init()

        prices: dict[str, Decimal] = {}
        sources: dict[str, str] = {}
        peg_tokens: set[str] = set()
        aggregator_for = getattr(self.market_servicer, "_aggregator_for", None)
        aggregator = (
            aggregator_for(chain)
            if aggregator_for is not None
            else getattr(
                self.market_servicer,
                "_price_aggregator",
                None,
            )
        )
        if not aggregator:
            return _FetchedPriceBatch({}, {}, frozenset())

        for token in tokens:
            try:
                resolver = getattr(self.market_servicer, "_resolve_token_for_pricing", None)
                resolved_token = await resolver(token, chain) if resolver is not None else None
                result = await aggregator.get_aggregated_price(token, "USD", resolved_token=resolved_token)
                if result and result.price:
                    source = str(getattr(result, "source", ""))
                    token_ref = getattr(resolved_token, "token_ref", None)
                    identity = getattr(token_ref, "identity_key", None)
                    expected_identity = (
                        f"{identity[0]}:{identity[1]}" if isinstance(identity, tuple) and len(identity) == 2 else None
                    )
                    raw_peg_tokens = getattr(result, "peg_tokens", ())
                    reported_peg_tokens = (
                        {str(item) for item in raw_peg_tokens}
                        if isinstance(raw_peg_tokens, list | tuple | set | frozenset)
                        else set()
                    )
                    is_legacy_synthetic = source.lower() in _SYNTHETIC_PEG_PRICE_SOURCES
                    if reported_peg_tokens or is_legacy_synthetic:
                        # A synthetic price must name exactly the identity that
                        # this request resolved. Foreign or identity-free
                        # provenance is not auditable and cannot enter the
                        # compiler as though it were a measured scalar.
                        if expected_identity is None or (
                            reported_peg_tokens and reported_peg_tokens != {expected_identity}
                        ):
                            logger.error(
                                "Discarding synthetic price source=%s for %s on %s: "
                                "expected exact identity %s, reported %s",
                                source,
                                token,
                                chain,
                                expected_identity,
                                sorted(reported_peg_tokens),
                            )
                            continue
                        peg_tokens.add(expected_identity)
                    key = token.upper()
                    prices[key] = Decimal(str(result.price))
                    sources[key] = source
            except Exception as e:
                logger.warning("Self-serve price fetch failed for %s on %s: %s", token, chain, e)
        return _FetchedPriceBatch(prices, sources, frozenset(peg_tokens))

    @staticmethod
    def _extract_token_symbols_from_intent(intent: object, *, default_chain: str | None = None) -> list[str]:
        """Extract token symbols from an intent object for price fetching.

        Delegates to the canonical ``extract_token_symbols`` helper in
        ``almanak.framework.runner.token_extraction`` — the same parser
        StrategyRunner uses on the client side — so runner and gateway
        cannot drift on which fields (or which pool suffixes) count as
        token symbols. In particular, trailing pool-type suffixes like
        ``"volatile"``/``"stable"``/``"concentrated"``/``"cl"`` are correctly
        filtered out.
        """
        from almanak.framework.runner.token_extraction import extract_token_symbols

        return extract_token_symbols(intent, default_chain=default_chain)

    async def _ensure_initialized(self) -> None:
        """Lazy initialization of execution components."""
        if self._initialized:
            return

        self._initialized = True
        logger.debug("ExecutionService initialized")

    def _get_compiler(self, chain: str, wallet_address: str):
        """Get or create IntentCompiler for a chain/wallet pair.

        The IntentCompiler requires chain, wallet_address, and rpc_url to perform
        on-chain queries (allowance checks, balance queries). Each chain/wallet
        combination needs its own compiler instance.

        Compilers are cached with a TTL to avoid expensive re-initialization (RPC
        setup, chain config). Real prices are applied per-request in CompileIntent()
        via the price_map field.

        ALM-3183: this used to construct with ``allow_placeholder_prices=True`` and
        no oracle, on the reasoning quoted above -- "real prices are applied
        per-request, so placeholders are safe here". That reasoning holds only when
        a per-request price actually arrives. It does not arrive when ``price_map``
        is empty AND ``_apply_compile_prices`` does not reach the mainnet gate,
        which happens for any intent type outside PRICE_SENSITIVE_INTENT_TYPES and
        for every intent on a non-mainnet network. FLASH_LOAN was exactly that hole:
        its nested swap callbacks call ``compiler.compile(...)`` and would size
        ``amountOutMinimum`` against the hardcoded table (ETH=$2000, unknown=$1).

        The compiler now starts with a REAL-BUT-EMPTY oracle, so the fallback is a
        loud per-intent compile failure instead of a fabricated price. The supported
        path is unchanged: ``update_prices(price_map)`` still installs real prices,
        and the mainnet gate still self-serves or fails closed.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")
            wallet_address: Wallet address for queries

        Returns:
            IntentCompiler configured for the specified chain/wallet
        """
        from almanak.framework.execution.fork_signal import is_managed_fork_network
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        now = time.time()

        # Check cache with TTL
        if cache_key in self._compiler_cache:
            compiler, created_at = self._compiler_cache[cache_key]
            if now - created_at < COMPILER_CACHE_TTL_SECONDS:
                return compiler
            else:
                logger.debug(f"Compiler cache expired for {cache_key}, recreating...")
                del self._compiler_cache[cache_key]

        # Get RPC URL for the chain
        network = self.settings.network
        rpc_url = get_rpc_url(chain, network=network)
        self._warn_if_resolved_to_public_rpc(chain, rpc_url)

        # ALM-3183: real-but-empty oracle, placeholders DISALLOWED (see docstring).
        # gateway_internal_preflight: this compiler runs INSIDE the gateway, so
        # it has no gateway_client to lend to the connector risk-parameter
        # pre-flights. Without the flag those pre-flights fail open on the one
        # path the production runner actually uses — the runner compiles through
        # execution.CompileIntent, not in-process. Measured on Aave V3 Mantle
        # after governance zeroed ltv: the collateral-eligibility guard never
        # ran and the emitted bundle reverted on-chain (VIB-6111).
        # managed_fork: the gateway's configured network IS the positive fork
        # declaration (ALM-3184). Declaring it here means the production compile
        # path never falls back to probing, and — critically — a mainnet gateway
        # declares False, so the swap price-impact guard cannot be disabled by an
        # RPC URL that merely looks local (a proxy on :8545, a hostname
        # containing "anvil").
        config = IntentCompilerConfig(
            allow_placeholder_prices=False,
            gateway_internal_preflight=True,
            managed_fork=is_managed_fork_network(network),
        )
        compiler = IntentCompiler(
            chain=chain,
            wallet_address=wallet_address,
            price_oracle={},
            rpc_url=rpc_url,
            config=config,
            chain_wallets=self._registered_chain_wallets,
            venue_verification_gateway_factory=self._venue_verification_gateway_factory(chain),
        )

        self._compiler_cache[cache_key] = (compiler, now)
        logger.info(f"Created IntentCompiler for chain={chain}, wallet={wallet_address[:10]}...")
        return compiler

    def _venue_verification_gateway_factory(self, chain: str):
        """Return a lazy chain-bound adapter factory; ordinary compiles never invoke it."""
        network = self.settings.network

        def build():
            from almanak.gateway.services.venue_verification_gateway import GatewayRpcVenueVerificationGateway

            return GatewayRpcVenueVerificationGateway(chain=chain, network=network)

        return build

    def _warn_if_resolved_to_public_rpc(self, chain: str, rpc_url: str) -> None:
        """Emit a one-time ERROR when the hosted gateway resolved ``chain`` to a public RPC.

        The gateway-side IntentCompiler built here has no gateway_client to defer
        to (it IS the gateway), so it resolves RPC URLs directly via get_rpc_url().
        When no credentialed provider (Alchemy key, Tenderly key, chain-specific
        or generic RPC URL) is configured, resolution falls through to free public
        RPC — a real, rate-limited egress from the gateway pod. This is distinct
        from the strategy-container "free public RPC" log in VIB-4429, which is
        harmless noise; here the gateway genuinely has no credentials. Log loudly
        once per chain so Infra can alert and fix the gateway pod env.

        We inspect the *resolved* URL rather than re-deriving the provider-selection
        priority list, so this check cannot drift out of sync with
        ``_auto_select_provider`` and naturally covers every provider path
        (Alchemy / Tenderly / custom / generic) and every chain in
        ``PUBLIC_RPC_URLS``. See VIB-4429.
        """
        chain_lower = chain.lower()
        if chain_lower in self._public_rpc_warned_chains:
            return

        from almanak.framework.deployment import is_hosted
        from almanak.gateway.utils.rpc_provider import PUBLIC_RPC_URLS

        if not is_hosted():
            return
        if rpc_url != PUBLIC_RPC_URLS.get(chain_lower):
            return

        self._public_rpc_warned_chains.add(chain_lower)
        logger.error(
            "Hosted gateway resolved chain=%s to a free public RPC — no credentialed "
            "provider is configured, so gateway-side intent compilation is subject to "
            "rate limits. Set ALMANAK_GATEWAY_ALCHEMY_API_KEY, TENDERLY_API_KEY_%s, or "
            "%s_RPC_URL in the gateway pod env. See VIB-4429.",
            chain_lower,
            chain_lower.upper(),
            chain_lower.upper(),
        )

    def _is_safe_address(self, wallet_address: str) -> bool:
        """Check if a wallet address matches the configured Safe address."""
        if not self.settings.safe_address or not self.settings.safe_mode:
            return False
        return wallet_address.lower() == self.settings.safe_address.lower()

    def _create_signer(self, wallet_address: str):
        """Create the appropriate signer based on wallet address.

        If wallet_address matches the configured Safe address, creates a
        ZodiacSigner or plugin (zodiac mode) or DirectSafeSigner (direct mode).
        Otherwise creates a LocalKeySigner.
        """
        from almanak.framework.execution.signer import LocalKeySigner

        if self._is_safe_address(wallet_address):
            from almanak.framework.execution.signer.safe.config import SafeSignerConfig, SafeWalletConfig

            safe_mode = self.settings.safe_mode or "direct"

            if safe_mode == "zodiac":
                # Zodiac mode: prefer explicit EOA_ADDRESS (platform deployments use
                # remote signer with no local key). Fall back to deriving from private
                # key only when EOA_ADDRESS is not set.
                if self.settings.eoa_address:
                    eoa_address = self.settings.eoa_address
                elif self.settings.private_key:
                    from eth_account import Account

                    eoa_address = Account.from_key(self.settings.private_key).address
                else:
                    raise ValueError("Zodiac mode requires either EOA_ADDRESS or PRIVATE_KEY (to derive EOA)")
            else:
                private_key = self.settings.private_key
                if not private_key:
                    raise ValueError("PRIVATE_KEY not configured in gateway settings")
                from eth_account import Account

                eoa_address = Account.from_key(private_key).address

            assert self.settings.safe_address is not None  # guarded by _is_safe_address
            wallet_config = SafeWalletConfig(
                safe_address=self.settings.safe_address,
                eoa_address=eoa_address,
                zodiac_roles_address=self.settings.zodiac_roles_address if safe_mode == "zodiac" else None,
            )
            safe_config = SafeSignerConfig(
                mode=safe_mode,
                wallet_config=wallet_config,
                private_key=self.settings.private_key,
                signer_service_url=self.settings.signer_service_url if safe_mode == "zodiac" else None,
                signer_service_jwt=self.settings.signer_service_jwt if safe_mode == "zodiac" else None,
            )

            from almanak.framework.execution.signer.safe import create_safe_signer

            signer = create_safe_signer(safe_config)
            logger.info("Using %s for wallet %s", type(signer).__name__, wallet_address[:10])
            return signer

        # Non-Safe EOA wallet
        private_key = self.settings.private_key
        if not private_key:
            raise ValueError("PRIVATE_KEY not configured in gateway settings")
        return LocalKeySigner(private_key=private_key)

    def _create_signer_from_resolved(self, wallet: "Any"):
        """Create a signer from a wallet registry resolved wallet (ResolvedWallet).

        Switches on wallet.kind:
          - 'zodiac' -> create_safe_signer
          - 'direct' -> LocalKeySigner
          - 'squads' -> NotImplementedError (Solana multisig, not yet supported)
        """
        from almanak.framework.execution.signer import LocalKeySigner

        kind = getattr(wallet, "kind", "direct")
        if kind == "zodiac":
            from almanak.framework.execution.signer.safe import create_safe_signer
            from almanak.framework.execution.signer.safe.config import SafeSignerConfig, SafeWalletConfig

            eoa_address = wallet.config.get("eoa_address", "")
            if not eoa_address:
                # Prefer explicit EOA_ADDRESS over private key derivation
                # (same precedence as _create_signer zodiac path)
                if self.settings.eoa_address:
                    eoa_address = self.settings.eoa_address
                elif self.settings.private_key:
                    from eth_account import Account

                    eoa_address = Account.from_key(self.settings.private_key).address
                else:
                    raise ValueError(
                        f"Zodiac wallet {wallet.account_address} requires eoa_address in config "
                        "or PRIVATE_KEY/EOA_ADDRESS in gateway settings"
                    )

            wallet_config = SafeWalletConfig(
                safe_address=wallet.account_address,
                eoa_address=eoa_address,
                zodiac_roles_address=wallet.config.get("zodiac_roles_address"),
            )
            safe_config = SafeSignerConfig(
                mode="zodiac",
                wallet_config=wallet_config,
                private_key=self.settings.private_key,
                signer_service_url=self.settings.signer_service_url,
                signer_service_jwt=self.settings.signer_service_jwt,
            )
            signer = create_safe_signer(safe_config)
            logger.info(
                "Using %s for resolved wallet %s (chain=%s)",
                type(signer).__name__,
                wallet.account_address[:10],
                wallet.chain,
            )
            return signer
        elif kind == "direct":
            pk = getattr(wallet, "private_key", None) or self.settings.private_key
            if not pk:
                raise ValueError("Direct wallet requires private_key")
            return LocalKeySigner(private_key=pk)
        elif kind == "squads":
            raise NotImplementedError("Squads multisig wallet support is not yet implemented")
        else:
            raise ValueError(f"Unknown wallet kind: {kind}")

    async def _get_orchestrator(self, chain: str, wallet_address: str):
        """Get or create execution orchestrator for a chain.

        If wallet_address matches the configured Safe address, the orchestrator
        uses a DirectSafeSigner instead of LocalKeySigner.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")
            wallet_address: Wallet address for signing

        Returns:
            ExecutionOrchestrator for the specified chain
        """
        from almanak.framework.execution.fork_signal import is_managed_fork_network
        from almanak.framework.execution.orchestrator import ExecutionOrchestrator
        from almanak.framework.execution.simulator import create_simulator
        from almanak.framework.execution.submitter import PublicMempoolSubmitter
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"{chain}:{wallet_address}"
        if cache_key in self._orchestrator_cache:
            return self._orchestrator_cache[cache_key]

        network = self.settings.network
        rpc_url = get_rpc_url(chain, network=network)

        # Resolve wallet from registry for per-chain signer selection.
        # If a registry is configured and resolves a wallet, signer errors must
        # fail closed (no fallback to default signer) to prevent funds being
        # routed through the wrong wallet.
        signer = None
        if self.wallet_registry is not None:
            try:
                resolved = self.wallet_registry.resolve(chain)
                if resolved is not None:
                    signer = self._create_signer_from_resolved(resolved)
            except KeyError:
                # Chain not in registry — fall through to default signer
                logger.debug(f"Chain {chain} not in wallet registry, using default signer")
            except Exception as e:
                # Signer construction failed for a resolved wallet — fail closed
                raise ValueError(
                    f"Wallet registry resolved chain '{chain}' but signer creation failed: {e}. "
                    f"Refusing to fall back to default signer to prevent wallet mismatch."
                ) from e
        if signer is None:
            signer = self._create_signer(wallet_address)
        submitter = PublicMempoolSubmitter(rpc_url=rpc_url)
        simulator = create_simulator(rpc_url=rpc_url)

        orchestrator = ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain=chain,
            rpc_url=rpc_url,
            # ALM-3184: the configured network is the positive fork declaration.
            # A mainnet gateway declares False, so Enso's 5% slippage widening
            # can no longer be granted by an RPC URL that merely looks local.
            managed_fork=is_managed_fork_network(network),
        )

        self._orchestrator_cache[cache_key] = orchestrator
        self._orchestrator_locks[cache_key] = asyncio.Lock()
        self._orchestrator_default_gas_caps[cache_key] = orchestrator.tx_risk_config.max_gas_price_gwei
        return orchestrator

    async def _get_solana_planner(self, chain: str, wallet_address: str):
        """Get or create SolanaExecutionPlanner for a Solana chain.

        Args:
            chain: Chain name (e.g., "solana")
            wallet_address: Solana wallet address (base58)

        Returns:
            SolanaExecutionPlanner instance
        """
        from almanak.framework.execution.solana.planner import SolanaExecutionPlanner
        from almanak.gateway.utils import get_rpc_url

        cache_key = f"solana:{chain}:{wallet_address}"
        if cache_key in self._orchestrator_cache:
            return self._orchestrator_cache[cache_key]

        network = self.settings.network
        rpc_url = get_rpc_url(chain, network=network)

        private_key = self.settings.solana_private_key
        if not private_key:
            raise ValueError(
                "SOLANA_PRIVATE_KEY not configured. "
                "Set ALMANAK_GATEWAY_SOLANA_PRIVATE_KEY or SOLANA_PRIVATE_KEY env var."
            )

        planner = SolanaExecutionPlanner(
            wallet_address=wallet_address,
            rpc_url=rpc_url,
            private_key=private_key,
            route_refresher=self._get_solana_route_refresher(),
        )

        self._orchestrator_cache[cache_key] = planner
        logger.info(f"Created SolanaExecutionPlanner for chain={chain}, wallet={wallet_address[:8]}...")
        return planner

    def _validate_compile_request(
        self,
        request: gateway_pb2.CompileIntentRequest,
        context: grpc.aio.ServicerContext,
    ) -> tuple[str, str, gateway_pb2.CompilationResult | None]:
        """Validate intent_type / chain / wallet_address before initialization.

        Returns (chain, wallet_address, error_result); error_result is None on
        success.
        """
        if not request.intent_type:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("intent_type is required")
            return "", "", gateway_pb2.CompilationResult(success=False, error="intent_type required")

        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return "", "", gateway_pb2.CompilationResult(success=False, error=str(e))

        wallet_address = request.wallet_address
        if wallet_address:
            try:
                from almanak.gateway.validation import validate_address_for_chain

                wallet_address = validate_address_for_chain(wallet_address, chain, "wallet_address")
            except ValidationError as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(str(e))
                return "", "", gateway_pb2.CompilationResult(success=False, error=str(e))

        return chain, wallet_address, None

    def _parse_price_map(
        self,
        request: gateway_pb2.CompileIntentRequest,
        intent_type: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[dict[str, Decimal] | None, gateway_pb2.CompilationResult | None]:
        """Parse and validate the client-supplied price_map.

        Returns (parsed_prices, error_result). parsed_prices stays None when
        the request carried no price_map (placeholder-price compat path);
        invalid client input returns INVALID_ARGUMENT, not INTERNAL.
        """
        price_map_raw = dict(request.price_map) if request.price_map else {}
        if not price_map_raw:
            return None, None
        try:
            parsed_prices: dict[str, Decimal] = {}
            for symbol, price_str in price_map_raw.items():
                price = Decimal(price_str)
                if not price.is_finite() or price <= 0:
                    raise ValueError(f"{symbol} price must be finite and > 0, got {price_str}")
                parsed_prices[symbol] = price
        except (ValueError, ArithmeticError) as e:
            error_msg = f"Invalid price_map value: {e}"
            logger.warning(f"CompileIntent rejected for {intent_type}: {error_msg}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return None, gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="INVALID_PRICE_MAP",
            )
        return parsed_prices, None

    def _deserialize_intent(
        self,
        request: gateway_pb2.CompileIntentRequest,
        intent_type: str,
        context: grpc.aio.ServicerContext,
    ) -> tuple[Any, gateway_pb2.CompilationResult | None]:
        """Deserialize intent_data JSON and build the intent object.

        json.JSONDecodeError deliberately propagates to the caller's outer
        except (INTERNAL); only intent construction errors map to
        INVALID_ARGUMENT.
        """
        intent_data = json.loads(request.intent_data.decode("utf-8"))

        # Create intent object from type and data.
        # Catch pydantic.ValidationError (e.g., SafeDecimal rejection of raw floats)
        # and ValueError (e.g., unknown intent_type) — both are client input errors and
        # should surface as INVALID_ARGUMENT, not INTERNAL.
        try:
            intent = self._create_intent(intent_type, intent_data)
        except (pydantic.ValidationError, ValueError) as e:
            error_msg = str(e)
            error_code = "INVALID_INTENT_DATA" if isinstance(e, pydantic.ValidationError) else "INVALID_INTENT_TYPE"
            logger.warning(f"CompileIntent rejected for {intent_type}: {error_msg}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return None, gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code=error_code,
            )
        return intent, None

    async def _enforce_mainnet_price_gate(
        self,
        compiler: Any,
        intent: Any,
        intent_type: str,
    ) -> gateway_pb2.CompilationResult | None:
        """VIB-523: fail-closed price gate for price-sensitive mainnet intents.

        On mainnet, fail compilation for price-sensitive intents if no real
        prices are available, instead of silently using placeholder prices
        with incorrect slippage calculations. First try self-serving prices
        from the gateway's own market service.
        """
        intent_tokens = self._extract_token_symbols_from_intent(intent, default_chain=getattr(compiler, "chain", None))
        self_served = (
            await self._fetch_prices_for_tokens(intent_tokens, getattr(compiler, "chain", ""))
            if intent_tokens
            else _FetchedPriceBatch({}, {}, frozenset())
        )
        # Preserve the long-standing private seam used by lightweight gateway
        # fixtures and embedders that override this fetcher with a plain price
        # dict. Such values carry no synthetic provenance, so treating their
        # source set as empty is accurate; the production implementation above
        # always returns the richer batch.
        if isinstance(self_served, dict):
            self_served = _FetchedPriceBatch(self_served, {}, frozenset())
        # Require prices for ALL extracted tokens to prevent partial placeholder usage
        all_covered = intent_tokens and all(t.upper() in self_served.prices for t in intent_tokens)
        # LP_CLOSE/PERP_CLOSE with only position_id may have no extractable
        # tokens. The original rationale here was "these operations don't need
        # prices (decreaseLiquidity/collect)" — true of an LP close, and NOT
        # true of a perp close: VIB-6219 made GMX derive a real protective
        # `acceptablePrice` from the index price. So the branch below no longer
        # claims the close needs no price; it claims only that the gateway
        # cannot tell, and leaves the refusal to whoever actually reads one.
        # Only bypass the price gate for close-type intents; all others must
        # fail-closed to prevent compiling with placeholder prices.
        normalized_type = self._normalize_intent_type(intent_type).upper()
        if not intent_tokens and normalized_type in PRICE_OPTIONAL_CLOSE_INTENT_TYPES:
            # VIB-6301: let the close proceed, but hand the compiler a
            # real-but-empty oracle instead of the fabricated placeholder table
            # (ETH=$2000, unknown=$1). Empty ≠ Zero: an unpriceable symbol now
            # raises instead of silently pricing at a made-up number, while real
            # pegs (USDC/USDT) still resolve through the known-stablecoin path.
            if hasattr(compiler, "update_prices"):
                compiler.update_prices({})
            logger.info(
                f"No token symbols extractable from {intent_type} intent — "
                f"skipping price gate for close-type intent, letting compiler "
                f"proceed with an empty (non-placeholder) price oracle."
            )
        elif not intent_tokens:
            error_msg = (
                f"No real prices available for {intent_type} compilation on mainnet. "
                f"Could not extract token symbols from intent to self-serve prices. "
                f"Refusing to compile with placeholder prices."
            )
            logger.warning(error_msg)
            return gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="NO_PRICES_AVAILABLE",
            )
        elif all_covered and hasattr(compiler, "update_prices"):
            if self_served.peg_tokens:
                seed_peg_fallbacks = getattr(compiler, "_seed_peg_fallbacks", None)
                if not callable(seed_peg_fallbacks):
                    error_msg = (
                        f"Synthetic peg provenance cannot be preserved for {intent_type} compilation. "
                        "Refusing to treat a gateway fallback as a measured price."
                    )
                    logger.error(error_msg)
                    return gateway_pb2.CompilationResult(
                        success=False,
                        error=error_msg,
                        error_code="NO_PRICES_AVAILABLE",
                    )
                seed_peg_fallbacks(self_served.peg_tokens)
            compiler.update_prices(self_served.prices)
            logger.info(
                "Self-served %d prices for %s compilation: %s (sources=%s, peg_tokens=%s)",
                len(self_served.prices),
                intent_type,
                list(self_served.prices),
                self_served.sources,
                sorted(self_served.peg_tokens),
            )
        else:
            error_msg = (
                f"No real prices available for {intent_type} compilation on mainnet. "
                f"Price oracle returned no data (CoinGecko rate-limited or Chainlink "
                f"unavailable). Refusing to compile with placeholder prices. "
                f"Retry after price sources recover."
            )
            logger.warning(error_msg)
            return gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="NO_PRICES_AVAILABLE",
            )
        return None

    async def _apply_compile_prices(
        self,
        compiler: Any,
        intent: Any,
        intent_type: str,
        parsed_prices: dict[str, Decimal] | None,
    ) -> gateway_pb2.CompilationResult | None:
        """Apply client prices, or enforce the mainnet fail-closed price gate."""
        if parsed_prices and hasattr(compiler, "update_prices"):
            compiler.update_prices(parsed_prices)
            logger.debug(f"Applied {len(parsed_prices)} real prices for compilation: {list(parsed_prices.keys())}")
        elif (
            self.settings.network == "mainnet"
            and self._normalize_intent_type(intent_type).upper() in PRICE_SENSITIVE_INTENT_TYPES
        ):
            return await self._enforce_mainnet_price_gate(compiler, intent, intent_type)
        return None

    def _build_compilation_response(
        self,
        compilation_result: Any,
        intent_type: str,
    ) -> gateway_pb2.CompilationResult:
        """Map the framework CompilationResult onto the proto response."""
        from almanak.framework.intents.compiler import CompilationStatus

        if compilation_result.status != CompilationStatus.SUCCESS:
            error_msg = compilation_result.error or "Compilation failed"
            logger.warning(f"CompileIntent failed for {intent_type}: {error_msg}")
            return gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="COMPILATION_FAILED",
            )

        if compilation_result.action_bundle is None:
            return gateway_pb2.CompilationResult(
                success=False,
                error="Compilation succeeded but no action bundle produced",
                error_code="NO_ACTION_BUNDLE",
            )

        # Serialize action bundle (include sensitive_data for gateway roundtrip,
        # e.g. Raydium NFT mint keypair needed for co-signing during Execute)
        bundle_dict = compilation_result.action_bundle.to_dict()
        if compilation_result.action_bundle.sensitive_data:
            bundle_dict["_sensitive_data"] = compilation_result.action_bundle.sensitive_data
        bundle_bytes = json.dumps(bundle_dict).encode("utf-8")

        return gateway_pb2.CompilationResult(
            success=True,
            action_bundle=bundle_bytes,
        )

    async def CompileIntent(
        self,
        request: gateway_pb2.CompileIntentRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.CompilationResult:
        """Compile an intent into an action bundle.

        Args:
            request: Compile request with intent_type, intent_data, chain, wallet
            context: gRPC context

        Returns:
            CompilationResult with action bundle or error
        """
        # Validate inputs BEFORE initialization
        intent_type = request.intent_type
        chain, wallet_address, invalid = self._validate_compile_request(request, context)
        if invalid is not None:
            return invalid

        await self._ensure_initialized()

        # Validate and parse price_map before entering the main try block
        # so invalid client input returns INVALID_ARGUMENT, not INTERNAL.
        parsed_prices, invalid = self._parse_price_map(request, intent_type, context)
        if invalid is not None:
            return invalid

        try:
            intent, invalid = self._deserialize_intent(request, intent_type, context)
            if invalid is not None:
                return invalid

            # Get compiler for this chain/wallet pair
            cache_key = f"{chain}:{wallet_address}"
            compiler = self._get_compiler(chain, wallet_address)
            compiler_lock = self._compiler_locks.setdefault(cache_key, asyncio.Lock())

            # Serialize override+compile+restore per cached compiler to
            # prevent concurrent requests from seeing each other's prices.
            async with compiler_lock:
                original_oracle = getattr(compiler, "price_oracle", None)
                # ALM-3183: default False, not True. The old default restored a
                # compiler whose attribute was somehow missing INTO placeholder
                # mode -- a fail-open in the restore path of the very lock that
                # exists to stop requests seeing each other's prices.
                original_placeholders = getattr(compiler, "_using_placeholders", False)

                gate_error = await self._apply_compile_prices(compiler, intent, intent_type, parsed_prices)
                if gate_error is not None:
                    return gate_error

                try:
                    compilation_result = compiler.compile(intent=intent)
                finally:
                    if hasattr(compiler, "restore_prices"):
                        compiler.restore_prices(original_oracle, original_placeholders)

            return self._build_compilation_response(compilation_result, intent_type)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"CompileIntent failed for {intent_type}: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.CompilationResult(
                success=False,
                error=error_msg,
                error_code="COMPILATION_FAILED",
            )

    def _create_intent(self, intent_type: str, intent_data: dict[str, Any]):
        """Create intent object from type and data.

        Args:
            intent_type: Intent type name (e.g., "swap", "lp_open")
            intent_data: Intent parameters

        Returns:
            Intent object
        """
        from almanak.framework.intents import BridgeIntent
        from almanak.framework.intents.vocabulary import (
            BorrowIntent,
            FlashLoanIntent,
            HoldIntent,
            LPCloseIntent,
            LPOpenIntent,
            PerpCloseIntent,
            PerpOpenIntent,
            PredictionBuyIntent,
            PredictionRedeemIntent,
            PredictionSellIntent,
            RepayIntent,
            StakeIntent,
            SupplyIntent,
            SwapIntent,
            UnstakeIntent,
            UnwrapNativeIntent,
            WithdrawIntent,
            WrapNativeIntent,
        )

        # Canonical class lookup keys match derivation:
        # type(intent).__name__.lower().replace("intent", "")
        # e.g. SwapIntent -> "swap", LPOpenIntent -> "lpopen".
        intent_classes = {
            "swap": SwapIntent,
            "hold": HoldIntent,
            "lpopen": LPOpenIntent,
            "lpclose": LPCloseIntent,
            "borrow": BorrowIntent,
            "repay": RepayIntent,
            "supply": SupplyIntent,
            "withdraw": WithdrawIntent,
            "perpopen": PerpOpenIntent,
            "perpclose": PerpCloseIntent,
            "flashloan": FlashLoanIntent,
            "stake": StakeIntent,
            "unstake": UnstakeIntent,
            "predictionbuy": PredictionBuyIntent,
            "predictionsell": PredictionSellIntent,
            "predictionredeem": PredictionRedeemIntent,
            "bridge": BridgeIntent,
            "wrapnative": WrapNativeIntent,
            "unwrapnative": UnwrapNativeIntent,
        }

        normalized_intent_type = self._normalize_intent_type(intent_type)
        intent_class = intent_classes.get(normalized_intent_type)
        if not intent_class:
            raise ValueError(f"Unknown intent type: {intent_type}")

        # Use deserialize() to properly handle JSON string -> Python type coercion
        # (e.g., ISO datetime strings -> datetime objects, string -> Decimal).
        # Direct construction fails because AlmanakImmutableModel uses strict=True.
        return intent_class.deserialize(intent_data)  # type: ignore[attr-defined]

    @staticmethod
    def _normalize_intent_type(intent_type: str) -> str:
        """Normalize intent type to canonical lookup key.

        Accepts legacy and canonical aliases, for example:
        - swap / SWAP
        - lp_open / lpopen / LP_OPEN
        - lp_close / lpclose / LP_CLOSE
        - perp_open / perpopen / PERP_OPEN
        - perp_close / perpclose / PERP_CLOSE
        """
        return intent_type.strip().lower().replace("-", "").replace("_", "")

    async def Execute(
        self,
        request: gateway_pb2.ExecuteRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.ExecutionResult:
        """Execute an action bundle.

        Args:
            request: Execute request with action bundle and options
            context: gRPC context

        Returns:
            ExecutionResult with tx hashes, gas used, receipts
        """
        # Validate inputs BEFORE initialization
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecutionResult(
                success=False,
                error=str(e),
                submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED,
            )

        try:
            from almanak.gateway.validation import validate_address_for_chain

            wallet_address = validate_address_for_chain(request.wallet_address, chain, "wallet_address")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.ExecutionResult(
                success=False,
                error=str(e),
                submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_NOT_ATTEMPTED,
            )

        await self._ensure_initialized()

        # Route to chain-family-specific execution. ``chain`` is pre-validated
        # by ``validate_chain``; ``try_resolve`` is defensive against an
        # allowlist drift returning a name no chain has registered for.
        chain_descriptor = ChainRegistry.try_resolve(chain)
        if chain_descriptor is not None and chain_descriptor.family is ChainFamily.SOLANA:
            return await self._execute_solana(request, context, chain, wallet_address)

        return await self._execute_evm(request, context, chain, wallet_address)

    async def _execute_solana(
        self,
        request: gateway_pb2.ExecuteRequest,
        context: grpc.aio.ServicerContext,
        chain: str,
        wallet_address: str,
    ) -> gateway_pb2.ExecutionResult:
        """Execute a Solana action bundle via SolanaExecutionPlanner."""
        try:
            from almanak.framework.models.reproduction_bundle import ActionBundle

            bundle_data = json.loads(request.action_bundle.decode("utf-8"))
            # Restore sensitive_data (e.g. additional_signers) from the gateway roundtrip
            sensitive_data = bundle_data.pop("_sensitive_data", {})
            action_bundle = ActionBundle.from_dict(bundle_data)
            if sensitive_data:
                action_bundle.sensitive_data = sensitive_data

            planner = await self._get_solana_planner(chain, wallet_address)

            exec_context = {
                "deployment_id": request.deployment_id,
                "intent_id": request.intent_id,
                "chain": chain,
                "wallet_address": wallet_address,
                "dry_run": request.dry_run,
            }

            outcome = await planner.execute_actions([action_bundle], exec_context)

            receipts_bytes = json.dumps(outcome.receipts).encode("utf-8")

            # Propagate actual fees from the planner result.
            # total_fee_native is in SOL; convert to lamports for the proto field.
            fee_lamports = int(outcome.total_fee_native * 1_000_000_000)
            plan_hash = execution_plan_hash(action_bundle)
            submission_transactions = certify_submission_transactions(action_bundle, outcome.tx_ids)

            return gateway_pb2.ExecutionResult(
                success=outcome.success,
                tx_hashes=outcome.tx_ids,
                total_gas_used=fee_lamports,
                receipts=receipts_bytes,
                execution_id="",
                error=outcome.error or "",
                submission_provenance=_submission_provenance_to_proto(outcome.submission_provenance),
                execution_plan_hash=plan_hash,
                submission_transactions=_submission_transactions_to_proto(submission_transactions),
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Solana Execute failed: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.ExecutionResult(
                success=False,
                error=error_msg,
                error_code="EXECUTION_FAILED",
                submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED,
            )

    async def _execute_evm(
        self,
        request: gateway_pb2.ExecuteRequest,
        context: grpc.aio.ServicerContext,
        chain: str,
        wallet_address: str,
    ) -> gateway_pb2.ExecutionResult:
        """Execute an EVM action bundle via ExecutionOrchestrator."""
        try:
            from almanak.framework.models.reproduction_bundle import ActionBundle

            bundle_data = json.loads(request.action_bundle.decode("utf-8"))
            action_bundle = ActionBundle.from_dict(bundle_data)

            # Get orchestrator for chain
            orchestrator = await self._get_orchestrator(chain, wallet_address)
            cache_key = f"{chain}:{wallet_address}"
            orchestrator_lock = self._orchestrator_locks.setdefault(cache_key, asyncio.Lock())
            default_gas_cap = self._orchestrator_default_gas_caps.setdefault(
                cache_key,
                orchestrator.tx_risk_config.max_gas_price_gwei,
            )

            # Build execution context
            from almanak.framework.execution.orchestrator import ExecutionContext

            # For Anvil (local fork) networks, always enable simulation so that the
            # LocalSimulator handles gas estimation. The default simulation_enabled=False
            # path (_maybe_estimate_gas_limits) calls eth_estimateGas against the public
            # RPC, which fails with "missing trie node" for storage slots that exist only
            # in the Anvil fork's local state (e.g., ERC1155 LP tokens minted by LP_OPEN).
            # LocalSimulator uses snapshot+execute to estimate gas against actual fork state.
            is_anvil_network = self.settings.network == "anvil"
            effective_simulation_enabled = request.simulation_enabled or is_anvil_network

            if is_anvil_network and not request.simulation_enabled:
                logger.debug(
                    "Anvil network: enabling simulation to use LocalSimulator "
                    "for accurate gas estimation of post-state-change transactions"
                )

            exec_context = ExecutionContext(
                deployment_id=request.deployment_id,
                intent_id=request.intent_id,
                chain=chain,
                wallet_address=wallet_address,
                simulation_enabled=effective_simulation_enabled,
                dry_run=request.dry_run,
            )

            # Execute with per-orchestrator serialization so request-specific gas caps
            # do not race or leak across concurrent requests.
            async with orchestrator_lock:
                orchestrator.tx_risk_config.max_gas_price_gwei = (
                    request.max_gas_price_gwei if request.max_gas_price_gwei > 0 else default_gas_cap
                )
                try:
                    result = await orchestrator.execute(action_bundle, exec_context)
                finally:
                    orchestrator.tx_risk_config.max_gas_price_gwei = default_gas_cap

            from almanak.framework.execution.signer.safe.base import SafeSigner

            atomic_safe_batch = isinstance(orchestrator.signer, SafeSigner) and len(action_bundle.transactions) > 1

            # Preserve the positional 1:1 tx-hash/receipt contract. The old
            # loop skipped an individual receipt after a conversion failure
            # while retaining every hash and returning success=True, making the
            # downstream receipt set silently incomplete.
            transaction_results = result.transaction_results or []
            try:
                tx_hashes, receipts_bytes = _serialize_evm_transaction_results(transaction_results)
            except ReceiptSetSerializationError as exc:
                # Transactions may already be mined. Return an application-level
                # failure with every known hash so callers do not interpret a
                # transport INTERNAL as permission to blindly resubmit the
                # bundle. Partial receipts are omitted as a set: positional
                # association is no longer trustworthy once one leg is missing.
                known_hashes = _known_evm_transaction_hashes(transaction_results)
                logger.error("EVM receipt set incomplete after execution: %s", exc)
                plan_hash = execution_plan_hash(action_bundle)
                return gateway_pb2.ExecutionResult(
                    success=False,
                    tx_hashes=known_hashes,
                    total_gas_used=result.total_gas_used or 0,
                    receipts=b"",
                    execution_id=result.correlation_id or "",
                    error=str(exc),
                    error_code="RECEIPT_SET_INCOMPLETE",
                    submission_provenance=_submission_provenance_to_proto(
                        getattr(result, "submission_provenance", SubmissionProvenance.UNSPECIFIED)
                    ),
                    execution_plan_hash=plan_hash,
                    submission_transactions=_submission_transactions_to_proto(
                        certify_submission_transactions(
                            action_bundle,
                            known_hashes,
                            transaction_indices=[
                                getattr(transaction_result, "transaction_index", None)
                                for transaction_result in transaction_results
                                if isinstance(getattr(transaction_result, "tx_hash", None), str)
                                and transaction_result.tx_hash.strip()
                            ],
                            atomic_batch=atomic_safe_batch,
                        )
                    ),
                )

            plan_hash = execution_plan_hash(action_bundle)
            return gateway_pb2.ExecutionResult(
                success=result.success,
                tx_hashes=tx_hashes,
                total_gas_used=result.total_gas_used or 0,
                receipts=receipts_bytes,
                execution_id=result.correlation_id or "",
                error=result.error or "",
                submission_provenance=_submission_provenance_to_proto(
                    getattr(result, "submission_provenance", SubmissionProvenance.UNSPECIFIED)
                ),
                execution_plan_hash=plan_hash,
                submission_transactions=_submission_transactions_to_proto(
                    certify_submission_transactions(
                        action_bundle,
                        tx_hashes,
                        transaction_indices=[
                            getattr(transaction_result, "transaction_index", None)
                            for transaction_result in transaction_results
                        ],
                        atomic_batch=atomic_safe_batch,
                    )
                ),
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Execute failed: {error_msg}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.ExecutionResult(
                success=False,
                error=error_msg,
                error_code="EXECUTION_FAILED",
                submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED,
            )

    async def GetTransactionStatus(
        self,
        request: gateway_pb2.TxStatusRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TxStatus:
        """Get transaction status.

        Routes to Solana or EVM status lookup based on chain family.

        Args:
            request: Status request with tx_hash and chain
            context: gRPC context

        Returns:
            TxStatus with confirmation status
        """
        # Validate chain first (needed for chain-aware tx_hash validation)
        try:
            chain = validate_chain(request.chain or "arbitrum")
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.TxStatus(status="invalid", error=str(e))

        # Validate tx_hash format (chain-aware: base58 for Solana, hex for EVM)
        try:
            tx_hash = validate_tx_hash(request.tx_hash, chain=chain)
        except ValidationError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.TxStatus(status="invalid", error=str(e))

        # Route to chain-family-specific status lookup
        from almanak.gateway.validation import is_solana_chain

        if is_solana_chain(chain):
            return await self._get_solana_tx_status(tx_hash, chain, context)

        return await self._get_evm_tx_status(tx_hash, chain, context)

    def _get_solana_rpc_client(self, chain: str):
        """Get or create a cached SolanaRpcClient for a Solana chain."""
        if chain in self._solana_rpc_cache:
            return self._solana_rpc_cache[chain]

        from almanak.framework.execution.solana.rpc import SolanaRpcClient, SolanaRpcConfig
        from almanak.gateway.utils import get_rpc_url

        rpc_url = get_rpc_url(chain, network=self.settings.network)
        rpc_client = SolanaRpcClient(SolanaRpcConfig(rpc_url=rpc_url))
        self._solana_rpc_cache[chain] = rpc_client
        return rpc_client

    async def _get_solana_tx_status(
        self,
        signature: str,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TxStatus:
        """Get Solana transaction status via getSignatureStatuses."""
        try:
            rpc_client = self._get_solana_rpc_client(chain)

            statuses = await rpc_client.get_signature_statuses([signature], search_transaction_history=True)
            status = statuses[0] if statuses else None

            if status is None:
                return gateway_pb2.TxStatus(status="pending")

            err = status.get("err")
            confirmation_status = status.get("confirmationStatus", "")
            slot = status.get("slot", 0)

            if err is not None:
                return gateway_pb2.TxStatus(
                    status="reverted",
                    block_number=slot,
                    error=f"Transaction failed: {err}",
                )

            # Map Solana commitment levels to status
            if confirmation_status in ("confirmed", "finalized"):
                return gateway_pb2.TxStatus(
                    status="confirmed",
                    confirmations=1 if confirmation_status == "confirmed" else 32,
                    block_number=slot,
                )
            elif confirmation_status == "processed":
                return gateway_pb2.TxStatus(
                    status="pending",
                    block_number=slot,
                )
            else:
                return gateway_pb2.TxStatus(status="pending")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Solana GetTransactionStatus failed for {signature}: {error_msg}")

            if "not found" in error_msg.lower():
                return gateway_pb2.TxStatus(status="pending")

            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.TxStatus(status="unknown", error=error_msg)

    async def _get_evm_tx_status(
        self,
        tx_hash: str,
        chain: str,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.TxStatus:
        """Get EVM transaction status via eth_getTransactionReceipt."""
        try:
            from web3 import AsyncHTTPProvider, AsyncWeb3

            from almanak.gateway.utils import get_rpc_url
            from almanak.gateway.utils.ssl_context import build_ssl_context

            rpc_url = get_rpc_url(chain, network=self.settings.network)
            w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"ssl": build_ssl_context()}))

            # Get transaction receipt
            receipt = await w3.eth.get_transaction_receipt(tx_hash)  # type: ignore[arg-type]

            if receipt is None:
                return gateway_pb2.TxStatus(status="pending")

            # Check status
            if receipt["status"] == 1:
                current_block = await w3.eth.block_number
                confirmations = current_block - receipt["blockNumber"]

                return gateway_pb2.TxStatus(
                    status="confirmed",
                    confirmations=confirmations,
                    block_number=receipt["blockNumber"],
                    gas_used=receipt["gasUsed"],
                )
            else:
                return gateway_pb2.TxStatus(
                    status="reverted",
                    block_number=receipt["blockNumber"],
                    gas_used=receipt["gasUsed"],
                    error="Transaction reverted",
                )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"GetTransactionStatus failed for {tx_hash}: {error_msg}")

            # If tx not found, it's likely still pending
            if "not found" in error_msg.lower():
                return gateway_pb2.TxStatus(status="pending")

            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(error_msg)
            return gateway_pb2.TxStatus(status="unknown", error=error_msg)
