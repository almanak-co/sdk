"""Connector-owned compiler for ERC-4626 vault deposit/redeem intents."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, cast

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.connectors.morpho_vault.sdk import SUPPORTED_CHAINS as _METAMORPHO_SUPPORTED_CHAINS
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
            asset_address = adapter.sdk.get_vault_asset(intent.vault_address)
            asset_token = ctx.services.resolve_token(asset_address, ctx.chain)
            if asset_token is None:
                return _failed(intent.intent_id, f"Cannot resolve vault asset token: {asset_address}")

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
            if intent.shares == "all":
                shares_wei = adapter.sdk.get_max_redeem(intent.vault_address, ctx.wallet_address)
                if shares_wei <= 0:
                    return _failed(intent.intent_id, "No shares to redeem")
            else:
                shares_decimal = cast(Decimal, intent.shares)
                share_decimals = adapter.sdk.get_decimals(intent.vault_address)
                shares_wei = int(shares_decimal * Decimal(10**share_decimals))

            if shares_wei <= 0:
                return _failed(intent.intent_id, "Redeem shares must be positive")

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

            result.transactions = [redeem_tx]
            result.total_gas_estimate = redeem_tx.gas_estimate
            result.action_bundle = ActionBundle(
                intent_type=IntentType.VAULT_REDEEM.value,
                transactions=[redeem_tx.to_dict()],
                metadata={
                    "protocol": intent.protocol,
                    "vault_address": intent.vault_address,
                    "shares_wei": str(shares_wei),
                    "redeem_all": intent.shares == "all",
                    "chain": ctx.chain,
                },
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


def _build_adapter(ctx: BaseCompilerContext, protocol: str) -> Any:
    from almanak.framework.connectors.vaults import build_vault_adapter

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
    from almanak.framework.connectors.vaults import is_vault_chain_supported, supported_vault_chains

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
