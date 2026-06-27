"""Unit tests for scripts/ci/check_teardown_state_persistence.py (VIB-5464 / TD-06).

Tests use inline fixture strings parsed with ast — no live files are read. Each
test pins one branch of the teardown-state posture rule so a regression in the
guard surfaces precisely.
"""

from __future__ import annotations

from pathlib import Path

from scripts.ci.check_teardown_state_persistence import evaluate_dir, evaluate_module, main


def _fails(source: str) -> list[str]:
    """Return all FAIL messages for the fixture module source."""
    out: list[str] = []
    for report in evaluate_module(source.strip(), "almanak/demo_strategies/fixture/strategy.py"):
        out.extend(report.fails)
    return out


def _classes(source: str) -> dict[str, list[str]]:
    """Map class name -> fail messages."""
    return {r.name: r.fails for r in evaluate_module(source.strip(), "fixture.py")}


# ---------------------------------------------------------------------------
# FAIL: opens a non-tracked position without a posture
# ---------------------------------------------------------------------------


def test_supply_without_posture_fails() -> None:
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert _fails(src), "a supply-opener with no posture must FAIL"


def test_perp_open_without_posture_fails() -> None:
    src = """
class PerpStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.perp_open(market="ETH/USD", size_usd=100)
"""
    assert _fails(src)


def test_stake_without_posture_fails() -> None:
    src = """
class StakerStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.stake(protocol="lido", amount=1)
"""
    assert _fails(src)


def test_trivial_persistent_state_does_not_satisfy() -> None:
    """An override that returns {} is the silent default in disguise -> FAIL."""
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.borrow(borrow_token="USDC", borrow_amount=1)
    def get_persistent_state(self):
        return {}
"""
    assert _fails(src)


def test_bare_super_persistent_state_does_not_satisfy() -> None:
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
    def get_persistent_state(self):
        return super().get_persistent_state()
"""
    assert _fails(src)


# ---------------------------------------------------------------------------
# PASS: declared postures
# ---------------------------------------------------------------------------


def test_real_dict_literal_persistent_state_passes() -> None:
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
    def get_persistent_state(self):
        return {"supplied": self._supplied}
    def load_persistent_state(self, state):
        self._supplied = state.get("supplied")
"""
    assert not _fails(src)


def test_subscript_assignment_persistent_state_passes() -> None:
    """The state["k"] = v; return state pattern (uniswap_lp/v4/traderjoe) passes."""
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
    def get_persistent_state(self):
        state = super().get_persistent_state()
        state["supplied"] = self._supplied
        return state
    def load_persistent_state(self, state):
        self._supplied = state.get("supplied")
"""
    assert not _fails(src)


def test_return_dict_of_state_passes() -> None:
    """return dict(self.state) is a populated return -> non-trivial -> passes."""
    src = """
class PerpStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.perp_open(market="ETH/USD", size_usd=1)
    def get_persistent_state(self):
        return dict(self.state)
    def load_persistent_state(self, state):
        self.state = dict(state)
"""
    assert not _fails(src)


def test_save_without_restore_fails() -> None:
    """A get_persistent_state override with no load_persistent_state is blind on
    restart -- saved state is discarded (CodeRabbit finding 3)."""
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
    def get_persistent_state(self):
        return {"supplied": self._supplied}
"""
    assert _fails(src)


def test_noop_load_does_not_satisfy() -> None:
    """A load_persistent_state that is just ``pass`` does not count as a restore."""
    src = """
class LenderStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
    def get_persistent_state(self):
        return {"supplied": self._supplied}
    def load_persistent_state(self, state):
        pass
"""
    assert _fails(src)


def test_chain_derived_flag_passes() -> None:
    src = """
class LenderStrategy(IntentStrategy):
    teardown_state_derived_from_chain = True
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert not _fails(src)


def test_chain_derived_flag_false_does_not_satisfy() -> None:
    src = """
class LenderStrategy(IntentStrategy):
    teardown_state_derived_from_chain = False
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert _fails(src)


def test_stateless_subclass_with_opener_fails() -> None:
    """Inheriting StatelessStrategy must NOT exempt a class that emits a tracked
    opener -- a 'stateless' base with empty teardown methods + a supply intent is
    exactly the blind case (CodeRabbit finding 4)."""
    src = """
class MonitorStrategy(StatelessStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert _fails(src)


def test_stateless_subclass_without_opener_passes() -> None:
    """A stateless strategy that opens no tracked position still passes."""
    src = """
class MonitorStrategy(StatelessStrategy):
    def decide(self, market):
        return Intent.hold()
"""
    assert not _fails(src)


# ---------------------------------------------------------------------------
# PASS: no non-tracked opener
# ---------------------------------------------------------------------------


def test_lp_open_only_is_auto_tracked_passes() -> None:
    """lp_open is framework-tracked; no posture required even without override."""
    src = """
class LPStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.lp_open(pool="WETH/USDC", amount0=1, amount1=1)
"""
    assert not _fails(src)


def test_swap_only_is_not_a_tracked_opener_passes() -> None:
    src = """
class SwapStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.swap(from_token="USDC", to_token="WETH", amount_usd=1)
"""
    assert not _fails(src)


def test_close_only_helper_passes() -> None:
    src = """
class CloserStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.hold()
    def generate_teardown_intents(self, mode, market=None):
        return [Intent.repay(token="USDC"), Intent.withdraw(token="WETH")]
"""
    assert not _fails(src)


# ---------------------------------------------------------------------------
# Discovery / edge cases
# ---------------------------------------------------------------------------


def test_non_strategy_class_ignored() -> None:
    src = """
class Helper:
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert not evaluate_module(src.strip(), "fixture.py")


def test_subscripted_base_recognised() -> None:
    src = """
class LenderStrategy(IntentStrategy[MyConfig]):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    assert _fails(src)


def test_local_subclass_inherits_base_posture() -> None:
    """A subclass that opens a position is credited a posture from an in-file base."""
    src = """
class BaseLender(IntentStrategy):
    def get_persistent_state(self):
        return {"x": self._x}
    def load_persistent_state(self, state):
        self._x = state.get("x")
class ConcreteLender(BaseLender):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    reports = _classes(src)
    assert "ConcreteLender" in reports, "local subclass of a strategy must be discovered"
    assert not reports["ConcreteLender"], "inherited save+restore credits a posture"


def test_split_inherited_posture_credited() -> None:
    """save on the base + restore on the subclass together satisfy the posture."""
    src = """
class BaseLender(IntentStrategy):
    def get_persistent_state(self):
        return {"x": self._x}
class ConcreteLender(BaseLender):
    def load_persistent_state(self, state):
        self._x = state.get("x")
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    reports = _classes(src)
    assert not reports["ConcreteLender"]


def test_local_subclass_without_inherited_posture_fails() -> None:
    """A subclass whose in-file base ALSO declares no posture must FAIL."""
    src = """
class BaseLender(IntentStrategy):
    pass
class ConcreteLender(BaseLender):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""
    reports = _classes(src)
    assert reports["ConcreteLender"]


def test_multiple_classes_each_evaluated() -> None:
    src = """
class GoodStrategy(IntentStrategy):
    teardown_state_derived_from_chain = True
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
class BadStrategy(IntentStrategy):
    def decide(self, market):
        return Intent.borrow(borrow_token="USDC", borrow_amount=1)
"""
    reports = _classes(src)
    assert not reports["GoodStrategy"]
    assert reports["BadStrategy"]


# ---------------------------------------------------------------------------
# evaluate_dir() + main() — file I/O, error reporting, exit codes (finding 5)
# ---------------------------------------------------------------------------

_GOOD = """
class GoodLender(IntentStrategy):
    teardown_state_derived_from_chain = True
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""

_BAD = """
class BlindLender(IntentStrategy):
    def decide(self, market):
        return Intent.supply(token="USDC", amount=1)
"""


def test_evaluate_dir_flags_bad_passes_good(tmp_path: Path) -> None:
    (tmp_path / "good.py").write_text(_GOOD.strip(), encoding="utf-8")
    (tmp_path / "bad.py").write_text(_BAD.strip(), encoding="utf-8")
    reports = {r.name: r.fails for r in evaluate_dir(tmp_path)}
    assert not reports["GoodLender"]
    assert reports["BlindLender"]


def test_evaluate_dir_reports_syntax_error(tmp_path: Path) -> None:
    (tmp_path / "broken.py").write_text("class Oops(IntentStrategy)\n    pass\n", encoding="utf-8")
    reports = evaluate_dir(tmp_path)
    assert any("syntax-error" in r.name and r.fails for r in reports)


def test_evaluate_dir_skips_pycache(tmp_path: Path) -> None:
    cache = tmp_path / "__pycache__"
    cache.mkdir()
    (cache / "bad.py").write_text(_BAD.strip(), encoding="utf-8")
    assert evaluate_dir(tmp_path) == []


def test_main_missing_dir_is_noop_pass(tmp_path: Path, capsys) -> None:
    rc = main(["--strategies-dir", str(tmp_path / "does-not-exist")])
    assert rc == 0
    assert "no-op pass" in capsys.readouterr().out


def test_main_returns_zero_when_clean(tmp_path: Path) -> None:
    (tmp_path / "good.py").write_text(_GOOD.strip(), encoding="utf-8")
    assert main(["--strategies-dir", str(tmp_path), "--verbose"]) == 0


def test_main_returns_one_on_violation(tmp_path: Path, capsys) -> None:
    (tmp_path / "bad.py").write_text(_BAD.strip(), encoding="utf-8")
    rc = main(["--strategies-dir", str(tmp_path)])
    assert rc == 1
    assert "teardown-blind" in capsys.readouterr().err
