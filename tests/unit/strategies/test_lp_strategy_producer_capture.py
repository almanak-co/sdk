"""Producer-side regression tests for VIB-4274 LP pool_address capture.

These tests assert that each migrated LP strategy's
``_capture_pool_address_from_result`` helper:

  - Accepts a valid 42-char hex address from the typed
    ``result.lp_open_data.pool_address`` attribute that the framework's
    ``ResultEnricher`` populates from the receipt parser.
  - Also accepts the same address via the fallback path
    ``result.extracted_data["lp_open_data"].pool_address``.
  - Rejects non-hex inputs (the original VIB-4274 descriptor bug shape
    such as ``"WETH/USDC/500"``, plus malformed length / non-hex body /
    non-string forms).
  - Returns ``None`` gracefully when neither path carries an
    ``lp_open_data`` payload.

Audit history: PR #2231's first version of this test file used a
synthetic ``SimpleNamespace(metadata={...})`` fixture that mocked an
interface the framework's result objects do not actually expose. The
helper passed those tests while never extracting anything in production.
The blocker was caught by the 3-auditor sweep (Codex + Claude
pr-auditor both flagged); the helper now reads the real interface
documented in ``almanak/framework/execution/extracted_data.py:LPOpenData``
and these tests fixture that shape. ``almanak/framework/accounting/lp_accounting.py:332``
and ``tests/unit/strategies/test_demo_lp_teardown_regressions.py:24,87``
are the canonical reference patterns.

A source-level wiring check assert that each strategy still calls the
helper from its receipt handler AND still emits the captured address
under ``details["pool_address"]``. A future refactor that drops either
half of the migration fails these checks at PR time rather than in
production.

See ``docs/internal/uat-cards/VIB-4274.md`` §5.1.5 for the spec these
tests implement.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

POOL_ADDR_FIXTURE = "0x" + "ab" * 20

MIGRATED_STRATEGIES = [
    pytest.param(
        "strategies.accounting.lp.strategy",
        id="accounting-lp",
    ),
    pytest.param(
        "strategies.accounting.lp_dual.strategy",
        id="accounting-lp-dual",
    ),
    pytest.param(
        "strategies.accounting.lp_triple.strategy",
        id="accounting-lp-triple",
    ),
    pytest.param(
        "strategies.incubating.sushiswap_v3_base.strategy",
        id="incubating-sushiswap-v3-base",
    ),
    pytest.param(
        "strategies.incubating.traderjoe_fee_rotator.strategy",
        id="incubating-traderjoe-fee-rotator",
    ),
]


def _helper(module_path: str):
    mod = importlib.import_module(module_path)
    return mod._capture_pool_address_from_result


def _result_with_lp_open_data(*, pool_address: Any, via: str) -> SimpleNamespace:
    """Construct a synthetic ExecutionResult-like object that surfaces
    ``pool_address`` via either the typed attribute (``lp_open_data``) or
    the ``extracted_data["lp_open_data"]`` fallback path.

    Uses ``SimpleNamespace`` for the inner ``lp_open_data`` so non-string
    pool_address values can be exercised — the real ``LPOpenData``
    dataclass types ``pool_address: str`` strictly, which would prevent
    constructing the malformed-input regression fixtures.
    """
    lp_data = SimpleNamespace(pool_address=pool_address)
    if via == "typed_attr":
        return SimpleNamespace(lp_open_data=lp_data, extracted_data={})
    if via == "extracted_data_dict":
        return SimpleNamespace(lp_open_data=None, extracted_data={"lp_open_data": lp_data})
    if via == "both":
        return SimpleNamespace(lp_open_data=lp_data, extracted_data={"lp_open_data": lp_data})
    raise ValueError(f"unknown 'via': {via}")


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
@pytest.mark.parametrize(
    "via", ["typed_attr", "extracted_data_dict", "both"], ids=["typed_attr", "extracted_data_dict", "both"]
)
def test_helper_accepts_hex_pool_address(module_path, via):
    result = _result_with_lp_open_data(pool_address=POOL_ADDR_FIXTURE, via=via)
    assert _helper(module_path)(result) == POOL_ADDR_FIXTURE


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_rejects_descriptor_string(module_path):
    """The original VIB-4274 bug shape — a descriptor where an address belongs."""
    result = _result_with_lp_open_data(pool_address="WETH/USDC/500", via="typed_attr")
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
@pytest.mark.parametrize(
    "malformed",
    [
        pytest.param("0x" + "Z" * 40, id="non_hex_body"),
        pytest.param("0x1234", id="too_short"),
        pytest.param("0x" + "ab" * 30, id="too_long"),
        pytest.param(12345, id="non_string_int"),
        pytest.param(None, id="non_string_none"),
        pytest.param("", id="empty_string"),
        pytest.param("ab" * 20, id="missing_0x_prefix"),
    ],
)
def test_helper_rejects_malformed(module_path, malformed):
    result = _result_with_lp_open_data(pool_address=malformed, via="typed_attr")
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_returns_none_when_no_lp_open_data(module_path):
    result = SimpleNamespace(lp_open_data=None, extracted_data={})
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_returns_none_when_result_has_no_attributes(module_path):
    result = SimpleNamespace()
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_returns_none_when_extracted_data_is_not_a_dict(module_path):
    result = SimpleNamespace(lp_open_data=None, extracted_data="not-a-dict")
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_accepts_real_LPOpenData_dataclass(module_path):
    """Round-trip against the real ``LPOpenData`` dataclass.

    Catches a regression where the helper's typed-attr lookup gets
    over-tightened to only accept ``SimpleNamespace`` (an easy mistake
    when copying test fixtures).
    """
    from almanak.framework.execution.extracted_data import LPOpenData

    lp_open = LPOpenData(position_id=12345, pool_address=POOL_ADDR_FIXTURE)
    result_via_typed = SimpleNamespace(lp_open_data=lp_open, extracted_data={})
    result_via_dict = SimpleNamespace(lp_open_data=None, extracted_data={"lp_open_data": lp_open})
    assert _helper(module_path)(result_via_typed) == POOL_ADDR_FIXTURE
    assert _helper(module_path)(result_via_dict) == POOL_ADDR_FIXTURE


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_helper_rejects_metadata_only_results(module_path):
    """VIB-4274 audit Blocker 1 regression guard.

    Pre-audit, the helper read ``result.metadata.get(...)`` — an
    interface that no production result type exposes. A result whose
    ONLY signal lives on ``metadata`` (the old, broken fixture shape)
    must return ``None`` so a future refactor cannot reintroduce the
    no-op migration by accident.
    """
    result = SimpleNamespace(metadata={"pool_address": POOL_ADDR_FIXTURE})
    assert _helper(module_path)(result) is None


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_strategy_calls_capture_helper_on_intent_executed(module_path):
    """Wiring check: the receipt-handler code path calls
    ``_capture_pool_address_from_result`` on the result. A regression
    that drops the call would silently break the migration.
    """
    mod = importlib.import_module(module_path)
    src = Path(mod.__file__).read_text()
    assert "_capture_pool_address_from_result(result)" in src, (
        f"{module_path} no longer calls _capture_pool_address_from_result on result — VIB-4274 migration regressed"
    )


@pytest.mark.parametrize("module_path", MIGRATED_STRATEGIES)
def test_strategy_emits_pool_address_in_details(module_path):
    """Wiring check: ``get_open_positions`` emits the captured address
    under ``details["pool_address"]``.

    The RHS is intentionally not pinned — ``lp_dual`` flows the address
    through a tuple-unpacked local (``pool_addr``),
    ``traderjoe_fee_rotator`` emits per-pool details dicts
    (``details_a`` / ``details_b``), and the single-position fixtures
    emit directly from ``self._pool_address``. What matters is the key
    is being written into a details dict.
    """
    mod = importlib.import_module(module_path)
    src = Path(mod.__file__).read_text()
    pattern = re.compile(r'\b(?:lp_)?details(?:_[a-z])?\s*\[\s*"pool_address"\s*\]\s*=')
    assert pattern.search(src), (
        f"{module_path} no longer emits details['pool_address'] — "
        "VIB-4274 migration regressed; consumer guards still safe but slot0 fast path is dead"
    )


def test_strategy_on_lp_open_captures_pool_address_end_to_end():
    """End-to-end round-trip for ``AccountingQuantLPStrategy``: synthetic
    LP_OPEN result with a real ``LPOpenData`` → strategy receives it via
    ``on_intent_executed`` → ``_pool_address`` populated → subsequent
    ``get_open_positions`` emits ``details["pool_address"]``.

    Only the canonical single-position fixture is tested end-to-end here —
    the helper-level tests above already prove the other three migrated
    strategies' helpers read the same interface, and the per-strategy
    wiring checks above prove they call it on ``LP_OPEN``. The end-to-end
    test is the canonical "production shape works" assurance; the per-
    strategy parametrized helper + wiring tests cover the regression
    surface across the rest.
    """
    from decimal import Decimal

    from almanak.framework.execution.extracted_data import LPOpenData
    from strategies.accounting.lp.strategy import (
        PHASE_LP_OPEN,
        PHASE_SWAPPED_IN,
        AccountingQuantLPConfig,
        AccountingQuantLPStrategy,
    )

    cfg = AccountingQuantLPConfig(
        pool="WETH/USDC/500",
        starting_asset="USDC",
        total_value_usd=Decimal("4.0"),
        swap_split_pct=Decimal("0.50"),
        range_width_pct=Decimal("0.20"),
        max_slippage=Decimal("0.005"),
    )
    strat = AccountingQuantLPStrategy.__new__(AccountingQuantLPStrategy)
    strat.config = cfg
    strat._chain = "arbitrum"
    strat.pool = cfg.pool
    strat.fee_tier = 500
    strat.token0_symbol = "WETH"
    strat.token1_symbol = "USDC"
    strat.starting_asset = cfg.starting_asset
    strat.other_asset = "WETH"
    strat.max_slippage = cfg.max_slippage
    # VIB-4316 — protocol / swap_protocol normally populated in __init__
    # from config; bypassing __init__ here, so set them explicitly.
    strat.protocol = "uniswap_v3"
    strat.swap_protocol = "uniswap_v3"
    strat._wallet_address = "0xtest"
    strat._strategy_id = "AccountingQuantLPStrategy:test"
    strat._phase = PHASE_SWAPPED_IN
    strat._position_id = None
    strat._pool_address = None
    strat._initial_balance_usd = None
    strat._initial_balance_token = None

    lp_open = LPOpenData(position_id=12345, pool_address=POOL_ADDR_FIXTURE)
    result = SimpleNamespace(
        position_id=12345,
        lp_open_data=lp_open,
        extracted_data={"lp_open_data": lp_open},
    )
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
    strat.on_intent_executed(intent, success=True, result=result)

    assert strat._phase == PHASE_LP_OPEN
    assert strat._position_id == "12345"
    assert strat._pool_address == POOL_ADDR_FIXTURE

    summary = strat.get_open_positions()
    assert len(summary.positions) == 1
    assert summary.positions[0].details.get("pool_address") == POOL_ADDR_FIXTURE
    assert summary.positions[0].details.get("pool") == cfg.pool  # descriptor preserved for dashboards


def test_strategy_on_lp_open_falls_back_to_descriptor_when_no_pool_in_lp_open_data():
    """End-to-end: LP_OPEN with an ``LPOpenData`` whose ``pool_address``
    is empty (parser couldn't identify the pool) → ``_pool_address``
    stays ``None`` → ``get_open_positions`` falls back to descriptor-only
    emission. The consumer guards then accept the descriptor under
    ``"pool"`` is rejected by the valuer's hex-shape check, triggering
    the price-ratio-tick fallback — that's the graceful-degradation
    contract documented in ``portfolio_valuer.py``.
    """
    from decimal import Decimal

    from almanak.framework.execution.extracted_data import LPOpenData
    from strategies.accounting.lp.strategy import (
        PHASE_LP_OPEN,
        PHASE_SWAPPED_IN,
        AccountingQuantLPConfig,
        AccountingQuantLPStrategy,
    )

    cfg = AccountingQuantLPConfig(
        pool="WETH/USDC/500",
        starting_asset="USDC",
        total_value_usd=Decimal("4.0"),
        swap_split_pct=Decimal("0.50"),
        range_width_pct=Decimal("0.20"),
        max_slippage=Decimal("0.005"),
    )
    strat = AccountingQuantLPStrategy.__new__(AccountingQuantLPStrategy)
    strat.config = cfg
    strat._chain = "arbitrum"
    strat.pool = cfg.pool
    strat.fee_tier = 500
    strat.token0_symbol = "WETH"
    strat.token1_symbol = "USDC"
    strat.starting_asset = cfg.starting_asset
    strat.other_asset = "WETH"
    strat.max_slippage = cfg.max_slippage
    # VIB-4316 — protocol / swap_protocol normally populated in __init__
    # from config; bypassing __init__ here, so set them explicitly.
    strat.protocol = "uniswap_v3"
    strat.swap_protocol = "uniswap_v3"
    strat._wallet_address = "0xtest"
    strat._strategy_id = "AccountingQuantLPStrategy:test"
    strat._phase = PHASE_SWAPPED_IN
    strat._position_id = None
    strat._pool_address = None
    strat._initial_balance_usd = None
    strat._initial_balance_token = None

    lp_open = LPOpenData(position_id=12345, pool_address="")  # parser didn't identify the pool
    result = SimpleNamespace(
        position_id=12345,
        lp_open_data=lp_open,
        extracted_data={"lp_open_data": lp_open},
    )
    intent = SimpleNamespace(intent_type=SimpleNamespace(value="LP_OPEN"))
    strat.on_intent_executed(intent, success=True, result=result)

    assert strat._phase == PHASE_LP_OPEN
    assert strat._pool_address is None  # graceful — no hex available

    summary = strat.get_open_positions()
    assert len(summary.positions) == 1
    assert "pool_address" not in summary.positions[0].details  # nothing to emit
    assert summary.positions[0].details.get("pool") == cfg.pool  # descriptor still present


def test_edge_lp_avax_no_pool_key_with_address():
    """``edge_lp_avax_wavax_usdc_v2/strategy.py`` (commit c47415bd7) renamed
    ``"pool"`` → ``"pool_address"`` because the value was already an address.

    Putting it back under ``"pool"`` would re-introduce the ambiguity for
    this file specifically: the consumer guards would still accept the hex
    (shape-based), but the field name would lie about what it carries —
    silently re-entering the dual-typed convention that VIB-4274 is
    supposed to close.
    """
    repo_root = Path(__file__).resolve().parents[3]
    path = repo_root / "strategies/incubating/edge_lp_avax_wavax_usdc_v2/strategy.py"
    assert path.exists(), (
        f"Migrated strategy file missing: {path}. A silent skip here would hide "
        "a real VIB-4274 regression — the producer migration's key-rename guard "
        "for edge_lp_avax_wavax_usdc_v2 depends on this file existing."
    )
    src = path.read_text()
    assert '"pool": self.pool_address' not in src, (
        f"{path} stored address under wrong key 'pool'; must be 'pool_address' per VIB-4274"
    )
