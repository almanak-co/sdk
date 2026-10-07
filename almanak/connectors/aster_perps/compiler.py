"""Aster Pro compiler: PERP_OPEN / PERP_CLOSE become off-chain order requests.

Aster Pro is an off-chain order book, so compilation produces an
``ActionBundle`` with no transactions and an ``order_request`` the Aster
execution handler submits through the gateway. Venue size rules (step size,
minimum notional) depend on the live mark price and are enforced gateway-side
at submission; this compiler validates everything that is knowable from the
intent alone.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from eth_abi import encode as abi_encode

from almanak.connectors._strategy_base.base.compiler import BasePerpCompiler, PerpCompilerContext
from almanak.connectors.aster_perps.addresses import FUTURES_BROKER_ID
from almanak.connectors.aster_perps.markets import MARGIN_ASSET, client_order_id, to_symbol
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

if TYPE_CHECKING:
    from almanak.framework.intents.perp_intents import (
        PerpCloseIntent,
        PerpDepositIntent,
        PerpOpenIntent,
        PerpWithdrawIntent,
    )

logger = logging.getLogger(__name__)

PROTOCOL = "aster_perps"
# Account home: deposits, withdrawals and the strategy wallet live on BSC.
SUPPORTED_CHAINS = frozenset({"bsc"})
MAX_LEVERAGE = Decimal("125")
# deposit(address currency, uint256 amount, uint256 broker)
_DEPOSIT_SELECTOR = "0x0efe6a8b"
_DEPOSIT_GAS = 120_000


def _failed(intent_id: str, error: str) -> CompilationResult:
    return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent_id, error=error)


def _is_margin_asset(token: str, chain: str) -> bool:
    if token.upper() == MARGIN_ASSET:
        return True
    from almanak.framework.data.tokens.resolver import get_token_resolver

    try:
        return get_token_resolver().resolve(token, chain).symbol.upper() == MARGIN_ASSET
    except Exception:  # noqa: BLE001 — any unresolvable reference is simply not the margin asset
        return False


def _offchain(intent_type: IntentType, intent_id: str, metadata: dict[str, Any]) -> CompilationResult:
    bundle = ActionBundle(
        intent_type=intent_type.value,
        transactions=[],
        metadata={"protocol": PROTOCOL, "intent_id": intent_id, **metadata},
    )
    result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
    result.action_bundle = bundle
    result.transactions = []
    result.total_gas_estimate = 0
    return result


class AsterPerpsCompiler(BasePerpCompiler):
    """Compile Aster Pro perp intents into gateway-submitted market orders."""

    protocols: ClassVar[frozenset[str]] = frozenset({PROTOCOL})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_DEPOSIT, IntentType.PERP_WITHDRAW}
    )
    chains: ClassVar[frozenset[str]] = SUPPORTED_CHAINS

    def compile_perp_open(self, ctx: PerpCompilerContext, intent: PerpOpenIntent) -> CompilationResult:
        intent_id = intent.intent_id
        if ctx.chain not in SUPPORTED_CHAINS:
            return _failed(intent_id, f"Aster Pro accounts are funded from BSC; got chain {ctx.chain!r}")
        if not _is_margin_asset(intent.collateral_token, ctx.chain):
            return _failed(
                intent_id,
                f"Aster Pro margin is {MARGIN_ASSET} held in the Aster account; got collateral_token "
                f"{intent.collateral_token!r}",
            )
        if intent.trigger_price is not None:
            return _failed(
                intent_id, "Aster Pro connector supports market orders only (trigger_price is not supported)"
            )
        leverage = intent.leverage
        if leverage != leverage.to_integral_value() or not Decimal("1") <= leverage <= MAX_LEVERAGE:
            return _failed(intent_id, f"Aster Pro leverage must be a whole number from 1 to 125; got {leverage}")
        try:
            symbol = to_symbol(intent.market)
            order_id = client_order_id(intent_id, leg="open")
        except ValueError as exc:
            return _failed(intent_id, str(exc))
        logger.info(
            "Compiled Aster PERP_OPEN %s %s notional=$%s leverage=%sx",
            symbol,
            "long" if intent.is_long else "short",
            intent.size_usd,
            leverage,
        )
        return _offchain(
            IntentType.PERP_OPEN,
            intent_id,
            {
                "order_request": {
                    "symbol": symbol,
                    "is_long": intent.is_long,
                    "notional_usd": str(intent.size_usd),
                    "close_position": False,
                    "leverage": int(leverage),
                    "client_order_id": order_id,
                    "max_slippage": str(intent.max_slippage),
                },
                "market": intent.market,
                "collateral_amount": str(intent.collateral_amount),
            },
        )

    def compile_perp_close(self, ctx: PerpCompilerContext, intent: PerpCloseIntent) -> CompilationResult:
        intent_id = intent.intent_id
        if ctx.chain not in SUPPORTED_CHAINS:
            return _failed(intent_id, f"Aster Pro accounts are funded from BSC; got chain {ctx.chain!r}")
        if intent.size_usd is not None:
            return _failed(intent_id, "Aster Pro connector closes the whole position; omit size_usd")
        if intent.position_id is not None:
            return _failed(intent_id, "Aster Pro positions are keyed by market; omit position_id")
        try:
            symbol = to_symbol(intent.market)
            order_id = client_order_id(intent_id, leg="close")
        except ValueError as exc:
            return _failed(intent_id, str(exc))
        logger.info("Compiled Aster PERP_CLOSE %s %s", symbol, "long" if intent.is_long else "short")
        return _offchain(
            IntentType.PERP_CLOSE,
            intent_id,
            {
                "order_request": {
                    "symbol": symbol,
                    "is_long": intent.is_long,
                    "notional_usd": "",
                    "close_position": True,
                    "leverage": 0,
                    "client_order_id": order_id,
                    "max_slippage": str(intent.max_slippage),
                },
                "market": intent.market,
            },
        )

    def compile_perp_deposit(self, ctx: PerpCompilerContext, intent: PerpDepositIntent) -> CompilationResult:
        """Approve the vault and ``deposit(token, amount, FUTURES_BROKER_ID)`` on-chain."""
        from almanak.connectors._strategy_base.address_registry import AddressRegistry

        intent_id = intent.intent_id
        if ctx.chain not in SUPPORTED_CHAINS:
            return _failed(intent_id, f"Aster Pro deposits are made on BSC; got chain {ctx.chain!r}")
        if intent.amount == "all":
            return _failed(intent_id, "amount='all' must be resolved before compilation")
        if not _is_margin_asset(intent.asset, ctx.chain):
            return _failed(intent_id, f"Aster Pro margin is {MARGIN_ASSET}; got asset {intent.asset!r}")
        vault = AddressRegistry.resolve_contract_address(PROTOCOL, ctx.chain, "vault")
        token = ctx.services.resolve_token(intent.asset, ctx.chain)
        if not vault or token is None:
            return _failed(intent_id, f"Aster vault or {intent.asset} unresolved on {ctx.chain}")
        amount_wei = int(Decimal(str(intent.amount)) * (Decimal(10) ** token.decimals))
        if amount_wei <= 0:
            return _failed(intent_id, f"deposit amount {intent.amount} rounds to zero")
        try:
            transactions = list(ctx.services.build_approve_tx(token.address, vault, amount_wei))
        except ValueError as exc:
            return _failed(intent_id, str(exc))
        calldata = abi_encode(["address", "uint256", "uint256"], [token.address, amount_wei, FUTURES_BROKER_ID])
        transactions.append(
            TransactionData(
                to=vault,
                value=0,
                data=_DEPOSIT_SELECTOR + calldata.hex(),
                gas_estimate=_DEPOSIT_GAS,
                description=f"Deposit {intent.amount} {token.symbol} into Aster Pro account",
                tx_type="perp_deposit",
            )
        )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent_id)
        result.transactions = transactions
        result.total_gas_estimate = sum(tx.gas_estimate for tx in transactions)
        result.action_bundle = ActionBundle(
            intent_type=IntentType.PERP_DEPOSIT.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "protocol": PROTOCOL,
                "intent_id": intent_id,
                "asset": token.symbol,
                "token_address": token.address,
                "amount": str(intent.amount),
                "amount_wei": str(amount_wei),
                "vault": vault,
                "chain": ctx.chain,
            },
        )
        return result

    def compile_perp_withdraw(self, ctx: PerpCompilerContext, intent: PerpWithdrawIntent) -> CompilationResult:
        """Off-chain withdrawal of free margin back to the strategy wallet.

        An unresolved ``amount="all"`` withdraws the account's full withdrawable
        balance, read by the gateway from the venue at submission.
        """
        intent_id = intent.intent_id
        if ctx.chain not in SUPPORTED_CHAINS:
            return _failed(intent_id, f"Aster Pro withdrawals settle on BSC; got chain {ctx.chain!r}")
        if not _is_margin_asset(intent.asset, ctx.chain):
            return _failed(intent_id, f"Aster Pro margin is {MARGIN_ASSET}; got asset {intent.asset!r}")
        if intent.destination is not None and intent.destination.lower() != ctx.wallet_address.lower():
            return _failed(intent_id, "Aster Pro withdrawals are paid only to the strategy wallet")
        return _offchain(
            IntentType.PERP_WITHDRAW,
            intent_id,
            {
                "withdraw_request": {
                    "asset": MARGIN_ASSET,
                    "amount": str(intent.amount),
                    "client_request_id": client_order_id(intent_id, leg="withdraw"),
                },
            },
        )


__all__ = ["PROTOCOL", "SUPPORTED_CHAINS", "AsterPerpsCompiler"]
