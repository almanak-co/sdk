"""VIB-5346 safety guard: NFT-identity LP connectors reject ``amount="all"``.

For uniswap_v3 / uniswap_v4 / traderjoe_v2 the LP_CLOSE ``position_id`` is an
NFT/identity token-id, NOT a fungible amount. Letting the runner resolve
minted-liquidity wei into that slot would target a wrong / nonexistent token-id.
The connector compilers HARD-REJECT the chaining marker at compile with a clear
error. Pendle (fungible LP) reaches its compiler with a numeric position_id and
never sees the marker (the runner clears it before dispatch).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.vocabulary import Intent, LPCloseIntent

_PRICES = {"ETH": Decimal("2000"), "WETH": Decimal("2000"), "USDC": Decimal("1")}


def _make_compiler(chain: str) -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        wallet_address="0x" + "1" * 40,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        price_oracle=_PRICES,
    )


@pytest.mark.parametrize(
    ("protocol", "chain"),
    [
        ("uniswap_v3", "arbitrum"),
        ("uniswap_v4", "arbitrum"),
        ("traderjoe_v2", "avalanche"),
    ],
)
def test_nft_identity_connector_rejects_amount_all(protocol: str, chain: str) -> None:
    compiler = _make_compiler(chain)
    intent = LPCloseIntent(
        position_id="12345",
        pool="WETH/USDC",
        protocol=protocol,
        amount="all",
    )
    result = compiler.compile(intent)
    assert result.status == CompilationStatus.FAILED
    assert "amount='all'" in (result.error or "")
    assert protocol in (result.error or "")
    assert "position identity" in (result.error or "")


def test_pendle_compiler_reached_with_numeric_position_id() -> None:
    """Layer-3 contract: Pendle (fungible LP) is reached with a numeric
    position_id and the chaining marker is never present at compile time.

    The runner resolves ``amount="all"`` into ``position_id`` and clears the
    marker via ``Intent.set_resolved_amount`` BEFORE dispatch, so the compiler
    always sees a plain integer-string position_id (which it reads via
    ``int(position_id)``). This asserts the resolution contract that keeps the
    Pendle compiler UNCHANGED.
    """
    intent = LPCloseIntent(position_id="0", protocol="pendle", amount="all")
    resolved = Intent.set_resolved_amount(intent, Decimal(1_200_000_000_000_000_000))
    # Compiler-visible state: numeric position_id, no marker.
    assert resolved.protocol == "pendle"
    assert resolved.amount is None
    assert int(resolved.position_id) == 1_200_000_000_000_000_000
