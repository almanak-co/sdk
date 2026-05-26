"""Balancer V2 Vault flash-loan calldata builder.

Folded out of ``framework/intents/compiler_flash_loan.py`` so the
flash-loan orchestrator no longer carries protocol-specific code.

Balancer Vault flash loans charge zero fees, which is why they are the
default choice for arbitrage-style FlashLoanIntent flows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.framework.intents.compiler_adapters import BalancerAdapter
from almanak.framework.intents.compiler_models import TokenInfo, TransactionData

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler


def build_balancer_flash_loan(
    compiler: IntentCompiler,
    token_info: TokenInfo,
    amount_wei: int,
    callback_params: bytes,
    callback_gas_total: int,
) -> dict:
    """Build a Balancer Vault flash loan transaction (zero fee).

    Returns:
        Dict with transaction, pool_address (vault), premium_bps (0), premium_amount (0), total_repay.
        On unsupported chain, returns ``{"error": ...}``.
    """
    adapter = BalancerAdapter(compiler.chain, "balancer")
    vault_address = adapter.get_vault_address()

    if vault_address == "0x0000000000000000000000000000000000000000":
        return {"error": f"Balancer Vault not available on chain: {compiler.chain}"}

    flash_loan_calldata = adapter.get_flash_loan_simple_calldata(
        recipient=compiler.wallet_address,
        token=token_info.address,
        amount=amount_wei,
        user_data=callback_params,
    )

    premium_bps = 0
    premium_amount = 0
    total_repay = amount_wei

    flash_loan_tx = TransactionData(
        to=vault_address,
        value=0,
        data="0x" + flash_loan_calldata.hex(),
        gas_estimate=adapter.estimate_flash_loan_simple_gas() + callback_gas_total,
        description=(
            f"Flash loan {compiler._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} "
            f"via Balancer (zero fee)"
        ),
        tx_type="flash_loan",
    )

    return {
        "transaction": flash_loan_tx,
        "pool_address": vault_address,
        "premium_bps": premium_bps,
        "premium_amount": premium_amount,
        "total_repay": total_repay,
    }


__all__ = ["build_balancer_flash_loan"]
