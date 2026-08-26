"""I3 PARAMETER FIDELITY on the LP_CLOSE lane (Base).

``LPCloseIntent.max_slippage`` is a caller-facing field. I3 asks the same
question of the exit that it asks of the entry: does the tolerance the caller
declared reach the calldata the chain will enforce?

Unlike the LP_OPEN and SWAP lanes, a close cannot be compiled from nothing — a
V3-shaped close needs a position NFT the wallet owns. These tests therefore open
a real position on the fork first, reusing the open helpers the existing LP tests
already maintain, and then compile (never submit) the close. No funds leave the
fork and no close is executed.

See ``docs/internal/qa-invariants/I3-parameter-fidelity.md``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import LPCloseIntent
from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._parameter_fidelity import (
    assert_known_violation,
    assert_parameter_fidelity,
    check_transactions,
)

CHAIN_NAME = "base"
DECLARED_SLIPPAGE = Decimal("0.05")


def _known_violation(result, label: str, *, ticket: str) -> None:
    """``_rule`` for a connector with a ticketed, still-open fidelity defect.

    Deliberately does NOT skip on a failed compile the way ``_rule`` does: a
    skip would report neither "defect present" nor "defect fixed", and this
    call site exists to keep the defect proven until it is not.
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


def _compiler(anvil_rpc_url: str, funded_wallet: str, price_oracle: dict[str, Decimal]) -> IntentCompiler:
    return IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )


@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_uniswap_v3_lp_close_carries_its_floor(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    from tests.intents.base.test_uniswap_v3_lp import POOL, _open_position_via_intent

    position_id = await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
        )
    )
    _known_violation(result, "uniswap_v3.lp_close (base)", ticket="VIB-6220")


@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_aerodrome_slipstream_lp_close_carries_its_floor(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
    anvil_eth_call_adapter,
) -> None:
    from tests.intents.base.test_aerodrome_slipstream_lp import POOL, _open_position_via_intent

    position_id = await _open_position_via_intent(
        funded_wallet, orchestrator, price_oracle, anvil_rpc_url, anvil_eth_call_adapter
    )
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        LPCloseIntent(
            position_id=str(position_id),
            pool=POOL,
            collect_fees=True,
            protocol="aerodrome_slipstream",
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
        )
    )
    _known_violation(result, "aerodrome_slipstream.lp_close (base)", ticket="VIB-6235")


@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_aerodrome_classic_lp_close_carries_its_floor(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    from tests.intents.base.test_aerodrome_lp import POOL_LABEL, _open_lp_position

    _, lp_balance = await _open_lp_position(web3, funded_wallet, orchestrator, price_oracle, anvil_rpc_url)
    assert lp_balance > 0
    result = _compiler(anvil_rpc_url, funded_wallet, price_oracle).compile(
        LPCloseIntent(
            position_id=POOL_LABEL,
            pool=POOL_LABEL,
            protocol="aerodrome",
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
        )
    )
    _rule(result, "aerodrome.lp_close.classic (base)")


@pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_CLOSE)
@pytest.mark.asyncio
async def test_curve_lp_close_carries_its_floor(
    web3: Web3,
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    """Curve LP is fungible: ``position_id`` is an LP-token amount, not an NFT.

    No open is needed to reach the encode boundary — the close compiles from the
    named amount, which is all I3 requires.
    """
    from tests.intents.base.test_curve_lp_base import (
        POOL,
    )

    compiler = _compiler(anvil_rpc_url, funded_wallet, price_oracle)
    result = compiler.compile(
        LPCloseIntent(
            pool=POOL,
            position_id="0.001",
            protocol="curve",
            chain=CHAIN_NAME,
            max_slippage=DECLARED_SLIPPAGE,
        )
    )
    _rule(result, "curve.lp_close (base)")
