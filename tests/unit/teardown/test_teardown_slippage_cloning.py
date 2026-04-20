"""Tests for slippage-cloning fallback chain in TeardownManager.execute_at_slippage.

When escalating slippage during teardown, the manager clones the intent with an
updated max_slippage value.  The cloning tries three strategies in order:
  1. Pydantic model_copy (primary — frozen models)
  2. dataclasses.replace (fallback for dataclass intents)
  3. to_dict/from_dict round-trip (fallback for custom intent types)
If all three fail, the original intent is used unchanged and an error is logged.
"""

from dataclasses import dataclass, replace
from decimal import Decimal
from types import SimpleNamespace


# ---------------------------------------------------------------------------
# Helpers — mirror the teardown_manager slippage-cloning logic (lines 589-621)
# so each path can be tested in isolation without an async execution context.
# ---------------------------------------------------------------------------

def _clone_with_slippage(intent_to_exec, slippage: Decimal):
    """Replicate the slippage-cloning fallback chain from teardown_manager.

    Returns (cloned_intent, cloned: bool).
    """
    intent_with_slippage = intent_to_exec
    if not hasattr(intent_to_exec, "max_slippage"):
        return intent_with_slippage, True  # nothing to clone

    cloned = False

    if hasattr(intent_to_exec, "model_copy"):
        try:
            intent_with_slippage = intent_to_exec.model_copy(update={"max_slippage": slippage})
            cloned = True
        except (TypeError, ValueError):
            pass

    if not cloned:
        try:
            intent_with_slippage = replace(intent_to_exec, max_slippage=slippage)
            cloned = True
        except TypeError:
            if hasattr(intent_to_exec, "to_dict") and hasattr(intent_to_exec, "from_dict"):
                try:
                    intent_dict = intent_to_exec.to_dict()
                    intent_dict["max_slippage"] = str(slippage)
                    intent_with_slippage = type(intent_to_exec).from_dict(intent_dict)
                    cloned = True
                except (TypeError, ValueError, KeyError):
                    pass

    return intent_with_slippage, cloned


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _DataclassIntent:
    """Minimal frozen dataclass intent for replace() path testing."""
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount: str = "100"
    max_slippage: Decimal = Decimal("0.005")


class _DictRoundTripIntent:
    """Intent that supports to_dict / from_dict but not model_copy or replace."""

    def __init__(self, from_token: str, to_token: str, max_slippage: Decimal):
        self.from_token = from_token
        self.to_token = to_token
        self.max_slippage = max_slippage

    def to_dict(self) -> dict:
        return {
            "from_token": self.from_token,
            "to_token": self.to_token,
            "max_slippage": str(self.max_slippage),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "_DictRoundTripIntent":
        return cls(
            from_token=d["from_token"],
            to_token=d["to_token"],
            max_slippage=Decimal(d["max_slippage"]),
        )


# ---------------------------------------------------------------------------
# Path 1: model_copy (Pydantic frozen models)
# ---------------------------------------------------------------------------

class TestModelCopyPath:
    """model_copy succeeds — primary cloning path for Pydantic intents."""

    def test_pydantic_intent_cloned_via_model_copy(self):
        """SwapIntent (real Pydantic model) is cloned with updated slippage."""
        from almanak.framework.intents.vocabulary import SwapIntent

        original = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            max_slippage=Decimal("0.005"),
        )
        new_slippage = Decimal("0.02")
        result, cloned = _clone_with_slippage(original, new_slippage)

        assert cloned is True
        assert result.max_slippage == new_slippage
        # Original unchanged (frozen)
        assert original.max_slippage == Decimal("0.005")
        assert result is not original

    def test_model_copy_preserves_other_fields(self):
        """Cloning only updates max_slippage — other fields are preserved."""
        from almanak.framework.intents.vocabulary import SwapIntent

        original = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("50"),
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
        )
        result, cloned = _clone_with_slippage(original, Decimal("0.05"))

        assert cloned is True
        assert result.from_token == "USDC"
        assert result.to_token == "WETH"
        assert result.amount == Decimal("50")
        assert result.protocol == "uniswap_v3"
        assert result.max_slippage == Decimal("0.05")


# ---------------------------------------------------------------------------
# Path 2: dataclasses.replace fallback
# ---------------------------------------------------------------------------

class TestReplaceFallback:
    """model_copy unavailable — falls back to dataclasses.replace."""

    def test_dataclass_intent_cloned_via_replace(self):
        """Frozen dataclass intent is cloned via replace()."""
        original = _DataclassIntent(max_slippage=Decimal("0.005"))
        result, cloned = _clone_with_slippage(original, Decimal("0.03"))

        assert cloned is True
        assert result.max_slippage == Decimal("0.03")
        assert original.max_slippage == Decimal("0.005")
        assert result is not original

    def test_replace_preserves_other_fields(self):
        """dataclasses.replace only updates max_slippage."""
        original = _DataclassIntent(
            from_token="DAI", to_token="ETH", amount="200", max_slippage=Decimal("0.01")
        )
        result, cloned = _clone_with_slippage(original, Decimal("0.04"))

        assert cloned is True
        assert result.from_token == "DAI"
        assert result.to_token == "ETH"
        assert result.amount == "200"


# ---------------------------------------------------------------------------
# Path 3: to_dict / from_dict round-trip fallback
# ---------------------------------------------------------------------------

class TestDictRoundTripFallback:
    """Both model_copy and replace fail — falls back to to_dict/from_dict."""

    def test_custom_intent_cloned_via_dict_round_trip(self):
        """Intent with to_dict/from_dict is cloned when replace raises TypeError."""
        original = _DictRoundTripIntent("USDC", "WETH", Decimal("0.005"))
        result, cloned = _clone_with_slippage(original, Decimal("0.05"))

        assert cloned is True
        assert result.max_slippage == Decimal("0.05")
        assert original.max_slippage == Decimal("0.005")
        assert result is not original

    def test_dict_round_trip_preserves_other_fields(self):
        """to_dict/from_dict round-trip preserves non-slippage fields."""
        original = _DictRoundTripIntent("DAI", "ETH", Decimal("0.01"))
        result, cloned = _clone_with_slippage(original, Decimal("0.10"))

        assert cloned is True
        assert result.from_token == "DAI"
        assert result.to_token == "ETH"


# ---------------------------------------------------------------------------
# Path 4: All cloning methods fail — original intent used
# ---------------------------------------------------------------------------

class TestAllCloningFails:
    """All three cloning strategies fail — original intent is retained."""

    def test_unclonable_intent_returns_original(self):
        """When nothing works, the original intent is returned unmodified."""
        intent = SimpleNamespace(max_slippage=Decimal("0.005"))
        result, cloned = _clone_with_slippage(intent, Decimal("0.10"))

        assert cloned is False
        assert result is intent
        assert result.max_slippage == Decimal("0.005")

    def test_unclonable_intent_signals_failure(self):
        """Helper path: cloning failure is signaled via cloned=False."""
        # Use an object that has max_slippage but no cloning methods
        intent = SimpleNamespace(max_slippage=Decimal("0.005"))
        _, cloned = _clone_with_slippage(intent, Decimal("0.10"))

        assert cloned is False


# ---------------------------------------------------------------------------
# Edge case: no max_slippage attribute
# ---------------------------------------------------------------------------

class TestNoSlippageAttribute:
    """Intent without max_slippage — cloning is skipped entirely."""

    def test_intent_without_max_slippage_passes_through(self):
        """HoldIntent-like objects without max_slippage are returned as-is."""
        intent = SimpleNamespace(reason="waiting")
        result, cloned = _clone_with_slippage(intent, Decimal("0.10"))

        assert cloned is True  # "cloned" means no failure — nothing to do
        assert result is intent


# ---------------------------------------------------------------------------
# Path 1 failure → Path 2 success (model_copy raises)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _DataclassWithBrokenModelCopy:
    """Frozen dataclass that also has a model_copy that always fails."""
    from_token: str = "USDC"
    to_token: str = "WETH"
    max_slippage: Decimal = Decimal("0.005")

    def model_copy(self, *, update: dict | None = None):
        raise TypeError("broken model_copy")


class TestModelCopyFailsFallsToReplace:
    """model_copy exists but raises — falls back to replace successfully."""

    def test_broken_model_copy_falls_back_to_replace(self):
        """If model_copy raises TypeError, dataclasses.replace is tried next."""
        original = _DataclassWithBrokenModelCopy(max_slippage=Decimal("0.005"))

        result, cloned = _clone_with_slippage(original, Decimal("0.08"))

        assert cloned is True
        assert result.max_slippage == Decimal("0.08")
        assert original.max_slippage == Decimal("0.005")
