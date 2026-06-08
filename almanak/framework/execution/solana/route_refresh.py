"""Protocol-clean Solana route refresh contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class SolanaRouteRefreshRequest:
    """Inputs needed to refresh a stale Solana route before signing."""

    protocol: str
    metadata: dict[str, Any]
    wallet_address: str
    rpc_url: str | None = None


@dataclass(frozen=True)
class SolanaRouteRefreshResult:
    """Fresh Solana transaction material returned by a route provider."""

    serialized_transaction: str
    amount_out: str | None = None
    price_impact_pct: str | None = None
    chain_family: str = "SOLANA"
    tx_type: str = "swap"
    last_valid_block_height: int = 0
    priority_fee_lamports: int = 0
    description: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> SolanaRouteRefreshResult:
        """Build a typed result from legacy transaction dictionaries."""
        serialized_transaction = value.get("serialized_transaction")
        if not serialized_transaction:
            raise ValueError("Solana route refresh returned no serialized_transaction")
        return cls(
            serialized_transaction=str(serialized_transaction),
            amount_out=None if value.get("amount_out") is None else str(value.get("amount_out")),
            price_impact_pct=None if value.get("price_impact_pct") is None else str(value.get("price_impact_pct")),
            chain_family=str(value.get("chain_family", "SOLANA")),
            tx_type=str(value.get("tx_type", "swap")),
            last_valid_block_height=int(value.get("last_valid_block_height") or 0),
            priority_fee_lamports=int(value.get("priority_fee_lamports") or 0),
            description=str(value.get("description", "")),
        )

    def to_transaction_dict(self) -> dict[str, Any]:
        """Return the legacy transaction dict shape used by the planner."""
        data: dict[str, Any] = {
            "serialized_transaction": self.serialized_transaction,
            "chain_family": self.chain_family,
            "tx_type": self.tx_type,
            "last_valid_block_height": self.last_valid_block_height,
            "priority_fee_lamports": self.priority_fee_lamports,
        }
        if self.amount_out is not None:
            data["amount_out"] = self.amount_out
        if self.price_impact_pct is not None:
            data["price_impact_pct"] = self.price_impact_pct
        if self.description:
            data["description"] = self.description
        return data


@runtime_checkable
class SolanaRouteRefresher(Protocol):
    """Refresh stale Solana route transaction material."""

    def refresh_route(self, request: SolanaRouteRefreshRequest) -> SolanaRouteRefreshResult: ...


__all__ = [
    "SolanaRouteRefreshRequest",
    "SolanaRouteRefreshResult",
    "SolanaRouteRefresher",
]
