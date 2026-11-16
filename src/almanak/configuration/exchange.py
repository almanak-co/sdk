"""Author trading and liquidity settings expose only applicable choices."""

from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from almanak.models import Model


class Exchange(Model):
    account: str
    pool: dict
    contracts: dict
    capital: dict
    entry_swap_bps: int | None = None
    # Archived inputs sized Roles quotas with these; the legacy loader drops them because
    # grants are scope-only (blueprint 27). Current inputs omit them.
    maximum0: dict | None = None
    maximum1: dict | None = None
    hold_seconds: str | int | None = None
    slippage_bps: int
    deadline_seconds: str | int
    native_asset: str
    minimum_native_balance: str | dict


class Trading(Exchange):
    kind: Literal["trading"]


class Liquidity(Exchange):
    kind: Literal["liquidity"]
    range: dict
    collect_seconds: str | int | None = None


ALLOCATION: TypeAdapter = TypeAdapter(Annotated[Trading | Liquidity, Field(discriminator="kind")])
