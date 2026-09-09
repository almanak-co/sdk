"""Connector-owned compiler for ERC-4626 vault deposit/redeem intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, cast

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.connectors.morpho_vault.sdk import SUPPORTED_CHAINS as _METAMORPHO_SUPPORTED_CHAINS
from almanak.connectors.morpho_vault.sdk import (
    VAULT_VERSION_V1,
    VAULT_VERSION_V2,
    ForceDeallocateRefusedError,
    VaultGatedError,
    VaultIlliquidError,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.vocabulary import VaultDepositIntent, VaultRedeemIntent

logger = logging.getLogger(__name__)


class MorphoVaultCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compile ERC-4626 vault intents through the vault adapter registry."""

    protocols: ClassVar[frozenset[str]] = frozenset({"metamorpho", "morpho_vault"})
    intents: ClassVar[frozenset[IntentType]] = frozenset({IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM})
    # Single source of truth for the MetaMorpho chain universe lives in
    # ``morpho_vault.sdk.SUPPORTED_CHAINS`` and the vault registry consumes it
    # via ``_register_builtin_adapters``. Mirror it here (lowercased) so the
    # compiler-registry / CI gate stays in sync without a second hand-edited list.
    chains: ClassVar[frozenset[str]] = frozenset(c.lower() for c in _METAMORPHO_SUPPORTED_CHAINS)

    def compile(self, ctx: BaseCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.VAULT_DEPOSIT:
            return self.compile_deposit(ctx, intent)
        if intent_type == IntentType.VAULT_REDEEM:
            return self.compile_redeem(ctx, intent)
        return self._unsupported(intent)

    def compile_deposit(self, ctx: BaseCompilerContext, intent: VaultDepositIntent) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        transactions: list[TransactionData] = []

        try:
            if intent.amount == "all":
                return _failed(
                    intent.intent_id,
                    "amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                )
            amount_decimal = cast(Decimal, intent.amount)
            if amount_decimal <= Decimal("0"):
                return _failed(intent.intent_id, "Vault deposit amount must be positive")

            gateway_error = _require_gateway(ctx, intent.intent_id)
            if gateway_error is not None:
                return gateway_error

            chain_error = _validate_vault_chain(intent.protocol, ctx.chain, intent.intent_id)
            if chain_error is not None:
                return chain_error

            adapter = _build_adapter(ctx, intent.protocol)
            vault_version = _vault_version(adapter, intent.vault_address)
            asset_address = adapter.sdk.get_vault_asset(intent.vault_address)
            asset_token = ctx.services.resolve_token(asset_address, ctx.chain)
            if asset_token is None:
                return _failed(intent.intent_id, f"Cannot resolve vault asset token: {asset_address}")
            if vault_version == VAULT_VERSION_V2:
                # V2 has no maxDeposit signal (returns 0 by design); the
                # deposit-side refusals are sendAssetsGate(sender) and
                # receiveSharesGate(receiver).
                try:
                    adapter.sdk.check_deposit_gate(intent.vault_address, ctx.wallet_address, ctx.wallet_address)
                except VaultGatedError as exc:
                    return _failed(intent.intent_id, str(exc))

            amount_wei = int(amount_decimal * Decimal(10**asset_token.decimals))
            if amount_wei <= 0:
                return _failed(
                    intent.intent_id,
                    (
                        f"Vault deposit amount {amount_decimal} {asset_token.symbol} is below the "
                        f"minimum unit (decimals={asset_token.decimals}) and rounds to 0 wei."
                    ),
                )
            transactions.extend(ctx.services.build_approve_tx(asset_token.address, intent.vault_address, amount_wei))

            deposit_tx_data = adapter.sdk.build_deposit_tx(
                vault_address=intent.vault_address,
                assets=amount_wei,
                receiver=ctx.wallet_address,
            )
            transactions.append(
                TransactionData(
                    to=deposit_tx_data["to"],
                    value=deposit_tx_data["value"],
                    data=deposit_tx_data["data"],
                    gas_estimate=deposit_tx_data["gas_estimate"],
                    description=f"Deposit {amount_decimal} {asset_token.symbol} into {intent.protocol} vault {intent.vault_address[:10]}...",
                    tx_type="vault_deposit",
                )
            )

            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            result.action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_DEPOSIT.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "vault_version": vault_version,
                    "asset_address": asset_token.address,
                    "asset_symbol": asset_token.symbol,
                    "deposit_amount": str(amount_decimal),
                    "deposit_amount_wei": str(amount_wei),
                    "chain": ctx.chain,
                },
            )
            logger.info(
                "Compiled VAULT_DEPOSIT: %s %s into %s vault %s...",
                amount_decimal,
                asset_token.symbol,
                intent.protocol,
                intent.vault_address[:10],
            )
            return result
        except Exception as exc:
            logger.exception("Failed to compile VAULT_DEPOSIT intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
            return result

    def compile_redeem(self, ctx: BaseCompilerContext, intent: VaultRedeemIntent) -> CompilationResult:
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)
        try:
            gateway_error = _require_gateway(ctx, intent.intent_id)
            if gateway_error is not None:
                return gateway_error

            chain_error = _validate_vault_chain(intent.protocol, ctx.chain, intent.intent_id)
            if chain_error is not None:
                return chain_error

            adapter = _build_adapter(ctx, intent.protocol)
            vault_version = _vault_version(adapter, intent.vault_address)
            if intent.shares == "all":
                # v1: maxRedeem (a hair below balanceOf; redeem(balanceOf) reverts).
                # V2: balanceOf — V2's max* views return 0 by design, so maxRedeem
                # would report "No shares to redeem" for every funded wallet;
                # liquidity is proven by the simulation below instead.
                if vault_version == VAULT_VERSION_V2:
                    shares_wei = adapter.sdk.get_balance_of(intent.vault_address, ctx.wallet_address)
                else:
                    shares_wei = adapter.sdk.get_max_redeem(intent.vault_address, ctx.wallet_address)
                if shares_wei <= 0:
                    return _failed(intent.intent_id, "No shares to redeem")
            else:
                shares_decimal = cast(Decimal, intent.shares)
                share_decimals = adapter.sdk.get_decimals(intent.vault_address)
                shares_wei = int(shares_decimal * Decimal(10**share_decimals))

            if shares_wei <= 0:
                return _failed(intent.intent_id, "Redeem shares must be positive")

            force_txs: list[TransactionData] = []
            force_meta: dict[str, Any] | None = None
            if vault_version == VAULT_VERSION_V2:
                try:
                    adapter.sdk.check_redeem_gates(intent.vault_address, ctx.wallet_address, ctx.wallet_address)
                except VaultGatedError as exc:
                    return _failed(intent.intent_id, str(exc))
                try:
                    adapter.sdk.simulate_redeem(
                        intent.vault_address, shares_wei, receiver=ctx.wallet_address, owner=ctx.wallet_address
                    )
                except VaultIlliquidError as illiquid:
                    if not getattr(intent, "allow_force_deallocate", False):
                        return _failed(
                            intent.intent_id,
                            f"{illiquid} A penalised forced exit is possible but not enabled: set "
                            "allow_force_deallocate=True on the vault_redeem intent (cap the cost with "
                            "max_force_deallocate_penalty_bps) once the user has accepted the penalty.",
                        )
                    forced = _plan_forced_exit(adapter, intent, ctx, shares_wei)
                    if isinstance(forced, CompilationResult):
                        return forced
                    force_txs, force_meta, shares_wei = forced

            redeem_tx_data = adapter.sdk.build_redeem_tx(
                vault_address=intent.vault_address,
                shares=shares_wei,
                receiver=ctx.wallet_address,
                owner=ctx.wallet_address,
            )
            redeem_tx = TransactionData(
                to=redeem_tx_data["to"],
                value=redeem_tx_data["value"],
                data=redeem_tx_data["data"],
                gas_estimate=redeem_tx_data["gas_estimate"],
                description=f"Redeem {'all' if intent.shares == 'all' else intent.shares} shares from {intent.protocol} vault {intent.vault_address[:10]}...",
                tx_type="vault_redeem",
            )

            transactions = [*force_txs, redeem_tx]
            result.transactions = transactions
            result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
            metadata: dict[str, Any] = {
                "protocol": intent.protocol,
                "vault_address": intent.vault_address,
                "vault_version": vault_version,
                "shares_wei": str(shares_wei),
                "redeem_all": intent.shares == "all",
                "chain": ctx.chain,
            }
            if force_meta is not None:
                metadata["force_deallocate"] = force_meta
                # Penalty burns happen before the redeem. Sequential EOA confirm
                # can land the burn and then fail the redeem; Safe MultiSend is
                # the existing atomic path. The orchestrator refuses EOA.
                metadata["requires_atomic"] = True
            result.action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_REDEEM.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata=metadata,
            )
            logger.info(
                "Compiled VAULT_REDEEM: %s shares from vault %s...",
                "all" if intent.shares == "all" else intent.shares,
                intent.vault_address[:10],
            )
            return result
        except Exception as exc:
            logger.exception("Failed to compile VAULT_REDEEM intent: %s", exc)
            result.status = CompilationStatus.FAILED
            result.error = str(exc)
            return result


def _plan_forced_exit(
    adapter: Any, intent: VaultRedeemIntent, ctx: BaseCompilerContext, shares_wei: int
) -> CompilationResult | tuple[list[TransactionData], dict[str, Any], int]:
    """Build the opt-in ``forceDeallocate`` legs that make a V2 redeem coverable.

    Returns the leg transactions, the metadata block, and the (possibly reduced)
    share count the trailing ``redeem`` must request — the penalty is burned
    from the redeemer before the redeem lands. Returns a FAILED
    ``CompilationResult`` when the SDK refuses the plan (cannot cover, penalty
    above the intent's cap) or a leg does not simulate.
    """
    max_bps = int(getattr(intent, "max_force_deallocate_penalty_bps", 10))
    try:
        plan = adapter.sdk.plan_force_deallocate(intent.vault_address, shares_wei, ctx.wallet_address, max_bps)
    except ForceDeallocateRefusedError as exc:
        return _failed(intent.intent_id, str(exc))
    if not plan.needed or not plan.legs:
        # The simulation said illiquid but the live math finds no shortfall:
        # state moved under us. Refuse rather than send a redeem we could not prove.
        return _failed(
            intent.intent_id,
            "Redeem simulation reverted but no liquidity shortfall is measurable; retry rather than force an exit.",
        )
    txs: list[TransactionData] = []
    for leg in plan.legs:
        try:
            adapter.sdk.simulate_force_deallocate(
                intent.vault_address, leg.adapter, leg.market_params_data, leg.assets, ctx.wallet_address
            )
        except VaultIlliquidError as exc:
            return _failed(intent.intent_id, str(exc))
        tx = adapter.sdk.build_force_deallocate_tx(
            intent.vault_address, leg.adapter, leg.market_params_data, leg.assets, ctx.wallet_address
        )
        txs.append(
            TransactionData(
                to=tx["to"],
                value=tx["value"],
                data=tx["data"],
                gas_estimate=tx["gas_estimate"],
                description=(
                    f"Force-deallocate {leg.assets} wei from market {leg.market_id[:10]}... "
                    f"(penalty {leg.penalty_assets} wei) to cover the redeem"
                ),
                tx_type="vault_force_deallocate",
            )
        )
    logger.warning(
        "VAULT_REDEEM on %s: forcing exit of %d wei across %d market(s) at %d bps penalty (cap %d bps)",
        intent.vault_address[:10],
        plan.shortfall_assets,
        len(plan.legs),
        plan.penalty_bps,
        max_bps,
    )
    meta = {
        "needed_assets": str(plan.needed_assets),
        "idle_assets": str(plan.idle_assets),
        "liquidity_market_capacity": str(plan.liquidity_market_capacity),
        "shortfall_assets": str(plan.shortfall_assets),
        "legs": [
            {
                "adapter": leg.adapter,
                "market_id": leg.market_id,
                "assets": str(leg.assets),
                "penalty_assets": str(leg.penalty_assets),
                "penalty_shares": str(leg.penalty_shares),
            }
            for leg in plan.legs
        ],
        "total_penalty_assets": str(plan.total_penalty_assets),
        "total_penalty_shares": str(plan.total_penalty_shares),
        "penalty_bps": plan.penalty_bps,
        "max_penalty_bps": max_bps,
        "redeem_shares_after_penalty": str(plan.redeem_shares),
    }
    return txs, meta, plan.redeem_shares


def _vault_version(adapter: Any, vault_address: str) -> str:
    """On-chain generation of ``vault_address`` via the adapter's SDK.

    ``detect_vault_version`` raises ``UnsupportedVaultError`` for a contract
    that is neither generation — that propagates as a FAILED compilation with
    the SDK's own message, never a silent v1 default. Adapters whose SDK lacks
    the probe (registry test doubles) are treated as v1, the legacy behaviour.
    """
    detect = getattr(adapter.sdk, "detect_vault_version", None)
    if detect is None:
        return VAULT_VERSION_V1
    version = detect(vault_address)
    return VAULT_VERSION_V2 if version == VAULT_VERSION_V2 else VAULT_VERSION_V1


def _build_adapter(ctx: BaseCompilerContext, protocol: str) -> Any:
    from almanak.connectors._strategy_base.vaults import build_vault_adapter

    # Cache by (protocol, chain, wallet_address) so deposit + redeem in the
    # same compilation context share one adapter and one set of cached vault
    # reads, matching the pattern used by Jupiter / Kamino / Polymarket.
    cache_key = ("morpho_vault_adapter", protocol.lower(), ctx.chain, ctx.wallet_address)
    adapter = ctx.cache.get(cache_key)
    if adapter is not None:
        return adapter
    adapter = build_vault_adapter(
        protocol,
        chain=ctx.chain,
        wallet_address=ctx.wallet_address,
        gateway_client=ctx.gateway_client,
        token_resolver=ctx.token_resolver,
    )
    ctx.cache[cache_key] = adapter
    return adapter


def _require_gateway(ctx: BaseCompilerContext, intent_id: str) -> CompilationResult | None:
    if ctx.gateway_client is not None and ctx.gateway_client.is_connected:
        return None
    return _failed(intent_id, "A connected GatewayClient is required for vault compilation (on-chain reads).")


def _validate_vault_chain(protocol: str, chain: str, intent_id: str) -> CompilationResult | None:
    from almanak.connectors._strategy_base.vaults import is_vault_chain_supported, supported_vault_chains

    if is_vault_chain_supported(protocol, chain):
        return None
    try:
        supported = supported_vault_chains(protocol)
    except KeyError:
        return _failed(
            intent_id,
            (
                f"Vault protocol '{protocol}' is not supported "
                "(no vault adapter registered). Register the adapter or correct the intent's protocol field before retrying."
            ),
        )
    supported_str = ", ".join(sorted(supported)) if supported else "(none declared)"
    return _failed(
        intent_id,
        (
            f"Vault protocol '{protocol}' is not supported on chain '{chain}'. "
            f"Supported chains: {supported_str}. File a vault registry / native connector ticket for the missing chain before retrying."
        ),
    )


def _failed(intent_id: str, error: str) -> CompilationResult:
    return CompilationResult(status=CompilationStatus.FAILED, error=error, intent_id=intent_id)


__all__ = ["MorphoVaultCompiler"]
