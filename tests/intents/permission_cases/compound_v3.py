"""On-chain permission-authorisation test cases for the Compound V3 connector.

Each case here is consumed by the parametrized harness in
``tests/intents/_permission_onchain_harness.py`` and gated by
``tests/unit/permissions/test_onchain_case_coverage.py``. See
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` for the design.

Compound V3 is "Comets" — one Comet per base (borrowable) asset, with multiple
collateral assets. On Arbitrum we cover the native USDC Comet
(``0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf``, the Comet whose ``baseToken()``
is native USDC at ``0xaf88...5831``).

``market_id`` semantics — **not** the Comet address: the compound_v3 lending
compiler resolves ``intent.market_id`` via
``COMPOUND_V3_COMET_ADDRESSES[chain][market_id]`` (see
``almanak/framework/intents/compiler_lending.py`` — ``_compile_borrow_compound_v3``
and siblings). The key on Arbitrum is the alias ``"usdc"``. The
``synthetic_market_id`` hint in ``permission_hints.py`` is left unset (default
``None``), which the compiler falls back to ``"usdc"`` anyway; we pass the alias
explicitly here so the case is self-describing and does not depend on that
fallback.

BORROW shape — Compound V3 is single-base: ``borrow()`` only draws the Comet's
base token (USDC). Collateral is a separate ``supplyCollateral(asset, amount)``
call on the same Comet. The synthetic compiler in
``almanak/framework/permissions/synthetic_intents.py::_build_borrow_intents``
expresses both sides as one ``BorrowIntent(collateral_token=WETH,
collateral_amount=..., borrow_token=USDC, borrow_amount=...)`` — the compiler
emits approve + supplyCollateral + borrow in a single bundle. We mirror that
shape. LTV is ~16.7% at ETH ~ $3000 (1 WETH collateral, 500 USDC borrow),
comfortably under the 82.5% borrow collateral factor for WETH on this market.
"""

from __future__ import annotations

from tests.intents._permission_onchain_harness import PermissionTestCase

# Arbitrum native USDC Comet (base token = USDC). Alias value matches the
# ``COMPOUND_V3_COMET_ADDRESSES["arbitrum"]`` key.
_USDC_COMET_MARKET_ID = "usdc"

# WITHDRAW needs a prior SUPPLY on-chain for this Safe. The cold-Safe harness
# cannot seed that state yet (plan doc P1 — "harness-seeding of prior state"),
# so defer WITHDRAW at runtime. SUPPLY/BORROW/REPAY are kept active: BORROW on
# Compound V3 is a single compiled bundle that opens its own collateral
# position inside the same tx (approve + supplyCollateral + borrow), so it
# does not need prior state. Declaration-level coverage gate still runs
# against WITHDRAW, so a connector change that drops selector support still
# fails PR-time.
DEFERRED_INTENT_TYPES: list[str] = ["WITHDRAW"]

CASES: list[PermissionTestCase] = [
    PermissionTestCase(
        chain="arbitrum",
        protocol="compound_v3",
        intent_type="SUPPLY",
        config={
            "token": "USDC",
            "amount": "100",
            "market_id": _USDC_COMET_MARKET_ID,
        },
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="compound_v3",
        intent_type="WITHDRAW",
        config={
            "token": "USDC",
            "amount": "50",
            "market_id": _USDC_COMET_MARKET_ID,
        },
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="compound_v3",
        intent_type="BORROW",
        config={
            "collateral_token": "WETH",
            "collateral_amount": "1",
            "borrow_token": "USDC",
            "borrow_amount": "500",
            "market_id": _USDC_COMET_MARKET_ID,
        },
    ),
    PermissionTestCase(
        chain="arbitrum",
        protocol="compound_v3",
        intent_type="REPAY",
        config={
            "token": "USDC",
            "amount": "50",
            "market_id": _USDC_COMET_MARKET_ID,
        },
    ),
]
