"""Morpho Blue flash-loan calldata builder.

Folded out of ``framework/intents/compiler_flash_loan.py`` so the
flash-loan orchestrator no longer carries protocol-specific code.

Morpho Blue flash loans charge zero fees, which is why they are the
default choice for PT leverage looping on Morpho Blue markets.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.framework.connectors.flash_loan.selector import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_SUPPORTED_CHAINS,
)
from almanak.framework.intents.compiler_models import TokenInfo, TransactionData

if TYPE_CHECKING:
    from almanak.framework.intents.compiler import IntentCompiler

# Morpho Blue `flashLoan(address token, uint256 assets, bytes calldata data)`
_MORPHO_FLASH_LOAN_SELECTOR = "0xe0232b42"


def build_morpho_flash_loan(
    compiler: IntentCompiler,
    token_info: TokenInfo,
    amount_wei: int,
    callback_params: bytes,
    callback_gas_total: int,
) -> dict:
    """Build a Morpho Blue flash loan transaction (zero fee).

    Returns:
        Dict with transaction, pool_address, premium_bps (0), premium_amount (0), total_repay.
        On unsupported chain, returns ``{"error": ...}``.
    """
    # MORPHO_BLUE_ADDRESSES lists chains where Morpho Blue is *deployed*, but
    # flash-loan enablement is a separate, more conservative set: only chains
    # where fee behaviour and callback semantics have been validated.
    if compiler.chain not in MORPHO_SUPPORTED_CHAINS:
        return {"error": f"Morpho Blue flash loans not enabled on chain: {compiler.chain}"}
    morpho_address = MORPHO_BLUE_ADDRESSES.get(compiler.chain)
    if not morpho_address:
        return {"error": f"Morpho Blue not available on chain: {compiler.chain}"}

    from web3 import Web3

    w3 = Web3()
    calldata = (
        _MORPHO_FLASH_LOAN_SELECTOR
        + w3.codec.encode(
            ["address", "uint256", "bytes"],
            [w3.to_checksum_address(token_info.address), amount_wei, callback_params],
        ).hex()
    )

    premium_bps = 0
    premium_amount = 0
    total_repay = amount_wei

    flash_loan_tx = TransactionData(
        to=morpho_address,
        value=0,
        data=calldata,
        gas_estimate=200_000 + callback_gas_total,
        description=(
            f"Flash loan {compiler._format_amount(amount_wei, token_info.decimals)} {token_info.symbol} "
            f"via Morpho Blue (zero fee)"
        ),
        tx_type="flash_loan",
    )

    return {
        "transaction": flash_loan_tx,
        "pool_address": morpho_address,
        "premium_bps": premium_bps,
        "premium_amount": premium_amount,
        "total_repay": total_repay,
    }


__all__ = ["build_morpho_flash_loan"]
