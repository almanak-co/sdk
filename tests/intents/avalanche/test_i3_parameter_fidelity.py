"""I3 PARAMETER FIDELITY on Avalanche — compile-time, funds-free.

TraderJoe V2 (Liquidity Book) lives here and nowhere else in the intent suite.
See ``docs/internal/qa-invariants/I3-parameter-fidelity.md``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents import LPOpenIntent, SwapIntent
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._parameter_fidelity import (
    assert_known_violation,
    assert_parameter_fidelity,
    check_transactions,
)

CHAIN_NAME = "avalanche"
DECLARED_SLIPPAGE = Decimal("0.05")


def _compiler(anvil_rpc_url: str, funded_wallet: str, price_oracle: dict[str, Decimal]) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )


def _known_violation(result, label: str, *, ticket: str) -> None:
    """``_rule`` for a connector with a ticketed, still-open fidelity defect.

    Does not skip on a failed compile: a skip proves nothing in either
    direction, and this call site exists to keep the defect proven.
    """
    assert result.status is CompilationStatus.SUCCESS, (
        f"{label}: compilation did not succeed ({result.status.value}): {result.error}; "
        f"{ticket} can be neither confirmed nor cleared"
    )
    report = check_transactions(result.transactions, label=label, declared_slippage=DECLARED_SLIPPAGE)
    print("\n" + report.describe())
    assert_known_violation(report, ticket=ticket)


def _rule(result, label: str) -> None:
    if result.status is not CompilationStatus.SUCCESS:
        pytest.skip(f"{label}: compilation did not succeed ({result.status.value}): {result.error}")
    report = check_transactions(result.transactions, label=label, declared_slippage=DECLARED_SLIPPAGE)
    print("\n" + report.describe())
    assert_parameter_fidelity(report)


@pytest.mark.intent(IntentType.SWAP)
def test_traderjoe_v2_swap_carries_its_floor(
    anvil_rpc_url: str, funded_wallet: str, price_oracle: dict[str, Decimal]
) -> None:
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=DECLARED_SLIPPAGE,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )
    )
    _rule(result, "traderjoe_v2.swap (avalanche)")


@pytest.mark.intent(IntentType.LP_OPEN)
def test_traderjoe_v2_lp_open_carries_its_floor(
    anvil_rpc_url: str, funded_wallet: str, price_oracle: dict[str, Decimal]
) -> None:
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        LPOpenIntent(
            pool="WAVAX/USDC/20",
            amount0=Decimal("2.0"),
            amount1=Decimal("50"),
            range_lower=Decimal("5"),
            range_upper=Decimal("500"),
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
        )
    )
    _known_violation(result, "traderjoe_v2.lp_open (avalanche)", ticket="VIB-6760")


@pytest.mark.intent(IntentType.SWAP)
def test_uniswap_v3_swap_carries_its_floor(
    anvil_rpc_url: str, funded_wallet: str, price_oracle: dict[str, Decimal]
) -> None:
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=Decimal("1.0"),
            max_slippage=DECLARED_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )
    )
    _rule(result, "uniswap_v3.swap (avalanche)")
