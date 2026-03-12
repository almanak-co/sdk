"""Jupiter DEX Aggregator Data Models.

Dataclasses for Jupiter Swap API v1 requests and responses.
Jupiter is the primary DEX aggregator on Solana, routing across
Raydium, Orca, Meteora, and other Solana AMMs.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class JupiterRoutePlan:
    """A single step in a Jupiter swap route.

    Attributes:
        amm_key: Address of the AMM pool used
        label: Human-readable AMM name (e.g., "Raydium", "Orca")
        input_mint: Input token mint address
        output_mint: Output token mint address
        in_amount: Input amount for this hop (in smallest units)
        out_amount: Output amount for this hop (in smallest units)
        fee_amount: Fee amount for this hop
        fee_mint: Mint of the fee token
        percent: Percentage of the total swap routed through this hop
    """

    amm_key: str = ""
    label: str = ""
    input_mint: str = ""
    output_mint: str = ""
    in_amount: str = "0"
    out_amount: str = "0"
    fee_amount: str = "0"
    fee_mint: str = ""
    percent: float = 100.0

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "JupiterRoutePlan":
        """Create from Jupiter API route plan entry."""
        swap_info = data.get("swapInfo", {})
        return cls(
            amm_key=swap_info.get("ammKey", ""),
            label=swap_info.get("label", ""),
            input_mint=swap_info.get("inputMint", ""),
            output_mint=swap_info.get("outputMint", ""),
            in_amount=swap_info.get("inAmount", "0"),
            out_amount=swap_info.get("outAmount", "0"),
            fee_amount=swap_info.get("feeAmount", "0"),
            fee_mint=swap_info.get("feeMint", ""),
            percent=data.get("percent", 100.0),
        )


@dataclass
class JupiterQuote:
    """Quote response from Jupiter Swap API v1.

    Attributes:
        input_mint: Input token mint address
        output_mint: Output token mint address
        in_amount: Input amount in smallest units
        out_amount: Expected output amount in smallest units
        other_amount_threshold: Minimum output amount (after slippage)
        price_impact_pct: Price impact as a percentage string (e.g., "0.12")
        route_plan: List of route steps
        slippage_bps: Slippage tolerance in basis points
        raw_response: Full API response for debugging
    """

    input_mint: str
    output_mint: str
    in_amount: str
    out_amount: str
    other_amount_threshold: str = "0"
    price_impact_pct: str = "0"
    route_plan: list[JupiterRoutePlan] = field(default_factory=list)
    slippage_bps: int = 50
    raw_response: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "JupiterQuote":
        """Create JupiterQuote from API response.

        Args:
            data: Jupiter /quote API response dict

        Returns:
            Parsed JupiterQuote
        """
        route_plan_data = data.get("routePlan", [])
        route_plan = [JupiterRoutePlan.from_api_response(step) for step in route_plan_data]

        return cls(
            input_mint=data.get("inputMint", ""),
            output_mint=data.get("outputMint", ""),
            in_amount=data.get("inAmount", "0"),
            out_amount=data.get("outAmount", "0"),
            other_amount_threshold=data.get("otherAmountThreshold", "0"),
            price_impact_pct=data.get("priceImpactPct", "0"),
            route_plan=route_plan,
            slippage_bps=data.get("slippageBps", 50),
            raw_response=data,
        )

    def get_price_impact_float(self) -> float:
        """Get price impact as a float.

        Returns:
            Price impact percentage (e.g., 0.12 for 0.12%)
        """
        try:
            return float(self.price_impact_pct)
        except (ValueError, TypeError):
            return 0.0

    def get_out_amount_int(self) -> int:
        """Get output amount as integer."""
        return int(self.out_amount)

    def get_in_amount_int(self) -> int:
        """Get input amount as integer."""
        return int(self.in_amount)


@dataclass
class JupiterSwapTransaction:
    """Swap transaction response from Jupiter Swap API v1.

    The Jupiter /swap endpoint returns a serialized VersionedTransaction
    in base64 format, ready for signing and submission.

    Attributes:
        swap_transaction: Base64-encoded serialized VersionedTransaction
        last_valid_block_height: Last blockhash validity slot
        priority_fee_lamports: Priority fee included in the transaction
        quote: The quote used to generate this transaction
    """

    swap_transaction: str  # base64-encoded serialized transaction
    last_valid_block_height: int = 0
    priority_fee_lamports: int = 0
    quote: JupiterQuote | None = None

    @classmethod
    def from_api_response(
        cls,
        data: dict[str, Any],
        quote: JupiterQuote | None = None,
    ) -> "JupiterSwapTransaction":
        """Create JupiterSwapTransaction from API response.

        Args:
            data: Jupiter /swap API response dict
            quote: The quote that produced this transaction

        Returns:
            Parsed JupiterSwapTransaction
        """
        return cls(
            swap_transaction=data.get("swapTransaction", ""),
            last_valid_block_height=data.get("lastValidBlockHeight", 0),
            priority_fee_lamports=data.get("prioritizationFeeLamports", 0),
            quote=quote,
        )
