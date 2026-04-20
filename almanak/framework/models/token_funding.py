"""Token funding configuration for strategy deployment.

Defines the ``token_funding`` field in ``config.json`` — a structured list
that tells the platform (and ``strat new``) exactly which tokens to fund,
how much, and on which chain.

Example config.json::

    {
        "token_funding": [
            {
                "symbol": "WETH",
                "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "chain": "arbitrum",
                "amount": "1.0",
                "amount_type": "token"
            },
            {
                "symbol": "USDC",
                "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "amount": "5000",
                "amount_type": "usd"
            }
        ]
    }
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any

from pydantic import ConfigDict, field_validator, model_validator

from almanak.framework.models.base import AlmanakImmutableModel

logger = logging.getLogger(__name__)


class AmountType(StrEnum):
    """How to interpret the ``amount`` field."""

    TOKEN = "token"
    """Native token units (e.g. 1.0 WETH = 1 ether)."""

    USD = "usd"
    """US dollar value (e.g. 5000 = $5,000 worth of the token)."""

    PERCENTAGE = "percentage"
    """Percentage of the token balance held at that point in time."""


class TokenFunding(AlmanakImmutableModel):
    """A single token funding requirement for a strategy.

    Every entry in ``token_funding`` represents: "fund this token with
    this amount before the strategy starts."
    """

    # Override strict=True from AlmanakImmutableModel so that plain strings
    # from JSON-loaded dicts are accepted for the AmountType enum field.
    model_config = ConfigDict(
        strict=False,
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        use_enum_values=True,
    )

    symbol: str
    address: str
    chain: str | None = None
    amount: str
    amount_type: AmountType

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("address")
    @classmethod
    def validate_address(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("0x") or len(v) != 42:
            raise ValueError(f"Invalid ERC-20 address: {v}")
        try:
            int(v, 16)
        except ValueError as err:
            raise ValueError(f"Invalid ERC-20 address (not hex): {v}") from err
        return v

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: str) -> str:
        v = v.strip()
        try:
            d = Decimal(v)
        except InvalidOperation as err:
            raise ValueError(f"amount must be a valid number, got: {v!r}") from err
        if d < 0:
            raise ValueError(f"amount must be non-negative, got: {v}")
        return v

    @field_validator("chain")
    @classmethod
    def normalize_chain(cls, v: str | None) -> str | None:
        return v.strip().lower() if v else None

    @model_validator(mode="after")
    def validate_percentage_range(self) -> TokenFunding:
        if self.amount_type == AmountType.PERCENTAGE:
            d = Decimal(self.amount)
            if d > 100:
                raise ValueError(f"percentage amount must be 0-100, got: {self.amount}")
        return self


def parse_token_funding(
    raw: Any,
    strategy_chain: str | None = None,
) -> list[TokenFunding] | None:
    """Parse and validate a ``token_funding`` value from config.

    Args:
        raw: The raw value from ``config["token_funding"]``.
        strategy_chain: Default chain to apply when entries omit ``chain``.

    Returns:
        Validated list of ``TokenFunding`` objects, or ``None`` if the
        input is missing, malformed, or uses an unrecognised format.
        Never raises — logs warnings instead (Phase 1 non-breaking).
    """
    if raw is None:
        return None

    if not isinstance(raw, list):
        logger.warning("token_funding must be a list, got %s — ignoring", type(raw).__name__)
        return None

    if not raw:
        logger.warning("token_funding list is empty — ignoring")
        return None

    # Check for old string-list format (e.g. ["WETH", "USDC"])
    if isinstance(raw[0], str):
        logger.warning(
            "token_funding contains plain strings — expected list of objects with "
            "symbol, address, amount, amount_type fields. Ignoring."
        )
        return None

    results: list[TokenFunding] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            logger.warning("token_funding[%d] is not a dict — skipping", i)
            continue
        try:
            # Fill default chain if not specified
            if "chain" not in entry and strategy_chain:
                entry = {**entry, "chain": strategy_chain}
            results.append(TokenFunding.model_validate(entry))
        except Exception as e:  # noqa: BLE001
            logger.warning("token_funding[%d] validation failed: %s — skipping", i, e)

    return results if results else None
