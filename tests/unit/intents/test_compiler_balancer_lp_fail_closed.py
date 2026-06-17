"""Balancer LP intents must fail closed with a capability-scoped error (ALM-2729).

Balancer is integrated for flash loans only — its connector manifest declares
``strategy_intents=("FLASH_LOAN",)`` and there is no Balancer LP compiler route.
Historically an ``Intent.lp_open(..., protocol="balancer")`` fell through to a
bare "not supported" message; this asserts the enriched, capability-scoped error
that names the protocols which DO support the LP verb, so a strategy author who
picked Balancer from the support matrix gets an actionable failure rather than a
confusing one.
"""

from decimal import Decimal

from almanak.framework.intents import LPCloseIntent, LPOpenIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)

TEST_WALLET = "0x1234567890123456789012345678901234567890"


def _compiler(chain: str = "arbitrum") -> IntentCompiler:
    config = IntentCompilerConfig(allow_placeholder_prices=True)
    return IntentCompiler(chain=chain, wallet_address=TEST_WALLET, config=config)


def test_balancer_lp_open_fails_closed_with_capability_error():
    compiler = _compiler()
    intent = LPOpenIntent(
        pool="0xBalancerPool",
        amount0=Decimal("500"),
        amount1=Decimal("500"),
        range_lower=Decimal("1"),
        range_upper=Decimal("2"),
        protocol="balancer",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert "balancer" in result.error
    assert "LP_OPEN" in result.error
    # Names the protocols that DO support LP_OPEN so the failure is actionable.
    assert "Protocols supporting LP_OPEN:" in result.error
    assert "uniswap_v3" in result.error
    # And does not silently mis-route to a uniswap-style adapter.
    assert result.action_bundle is None


def test_balancer_lp_close_fails_closed_with_capability_error():
    compiler = _compiler()
    intent = LPCloseIntent(
        position_id="1",
        pool="0xBalancerPool",
        protocol="balancer",
    )

    result = compiler.compile(intent)

    assert result.status == CompilationStatus.FAILED
    assert "balancer" in result.error
    assert "LP_CLOSE" in result.error
    assert "Protocols supporting LP_CLOSE:" in result.error
    assert result.action_bundle is None
