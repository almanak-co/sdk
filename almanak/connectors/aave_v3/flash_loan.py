"""Aave V3 flash-loan calldata builder.

Folded out of ``framework/intents/compiler_flash_loan.py`` so the
flash-loan orchestrator no longer carries protocol-specific code.
The orchestrator stays framework-side (it composes callback intents
across protocols); only the Aave-V3-specific calldata + premium math
lives here.

Aave V3 premium is 9 bps (0.09%) on the borrowed amount.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.framework.intents.compiler_adapters import AaveV3Adapter
from almanak.framework.intents.compiler_models import TokenInfo, TransactionData

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler


def build_aave_flash_loan(
    compiler: IntentCompiler,
    token_info: TokenInfo,
    amount_wei: int,
    callback_params: bytes,
    callback_gas_total: int,
) -> dict:
    """Build an Aave V3 flash loan transaction.

    Returns:
        Dict with transaction, pool_address, premium_bps, premium_amount, total_repay.
        On unsupported chain, returns ``{"error": ...}``.
    """
    adapter = AaveV3Adapter(compiler.chain, "aave_v3")
    pool_address = adapter.get_pool_address()

    if pool_address == "0x0000000000000000000000000000000000000000":
        return {"error": f"Aave V3 not available on chain: {compiler.chain}"}

    flash_loan_calldata = adapter.get_flash_loan_simple_calldata(
        receiver_address=compiler.wallet_address,
        asset=token_info.address,
        amount=amount_wei,
        params=callback_params,
    )

    premium_bps = 9
    premium_amount = (amount_wei * premium_bps) // 10000
    total_repay = amount_wei + premium_amount

    flash_loan_tx = TransactionData(
        to=pool_address,
        value=0,
        data="0x" + flash_loan_calldata.hex(),
        gas_estimate=adapter.estimate_flash_loan_simple_gas() + callback_gas_total,
        description=(
            f"Flash loan {compiler._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} "
            f"via Aave V3 (premium: {compiler._format_amount(premium_amount, token_info.decimals)} {token_info.symbol})"
        ),
        tx_type="flash_loan",
    )

    return {
        "transaction": flash_loan_tx,
        "pool_address": pool_address,
        "premium_bps": premium_bps,
        "premium_amount": premium_amount,
        "total_repay": total_repay,
    }


__all__ = ["build_aave_flash_loan"]
