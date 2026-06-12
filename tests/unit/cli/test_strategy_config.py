"""Unit tests for ``almanak.framework.cli._strategy_config``.

The dict -> ConfigT coercion shared by ``strat run`` and ``strat backtest``
(extracted from ``run_helpers._instantiate_strategy`` in PR #2760). The
``_instantiate_strategy`` suites pin the runner-facing behaviour; this file
pins the coercion-specific edges: Decimal detection through Optional/union
annotations, and the ``DictConfigWrapper`` re-export contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from almanak.framework.cli._strategy_config import (
    DictConfigWrapper,
    coerce_strategy_config,
)


class _GenericBase[ConfigT]:
    """Minimal generic base so ``__orig_bases__`` carries the config type."""


@dataclass
class _UnionConfig:
    plain: Decimal = field(default_factory=lambda: Decimal("1"))
    pep604_optional: Decimal | None = None
    typing_optional: Optional[Decimal] = None  # noqa: UP045 -- typing.Union spelling is the point
    not_decimal: str = "keep"


class _UnionStrategy(_GenericBase[_UnionConfig]):
    pass


class _NoGenericStrategy:
    pass


class TestDecimalCoercionThroughUnions:
    def test_plain_decimal_field_coerced(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"plain": "0.25"})
        assert config.plain == Decimal("0.25")
        assert isinstance(config.plain, Decimal)

    def test_pep604_optional_decimal_coerced(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"pep604_optional": 0.5})
        assert config.pep604_optional == Decimal("0.5")
        assert isinstance(config.pep604_optional, Decimal)

    def test_typing_optional_decimal_coerced(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"typing_optional": "3"})
        assert config.typing_optional == Decimal("3")
        assert isinstance(config.typing_optional, Decimal)

    def test_none_value_left_untouched(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"pep604_optional": None})
        assert config.pep604_optional is None

    def test_non_decimal_field_not_coerced(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"not_decimal": "keep"})
        assert config.not_decimal == "keep"
        assert isinstance(config.not_decimal, str)

    def test_uncoercible_value_kept_as_is(self) -> None:
        config = coerce_strategy_config(_UnionStrategy, {"plain": "not-a-number"})
        assert config.plain == "not-a-number"

    def test_bool_for_decimal_field_passes_through_unconverted(self) -> None:
        """bool subclasses int but Decimal(str(True)) raises; it must skip
        the coercion branch and land unchanged, not via the exception path."""
        config = coerce_strategy_config(_UnionStrategy, {"pep604_optional": True})
        assert config.pep604_optional is True


class TestInjectedKeyFiltering:
    """Runtime/framework keys are dropped before dataclass construction.

    config.json carries CLI-injected keys (deployment_id, chain, ...) that no
    strategy config dataclass declares. They must be filtered silently --
    neither passed to the dataclass constructor (TypeError -> silent
    DictConfigWrapper fallback) nor reported as ignored.
    """

    _INJECTED = {
        "deployment_id": "deployment:abc123",
        "chain": "arbitrum",
        "wallet_address": "0x" + "0" * 40,
        "anvil_funding": {"ETH": 100},
        "strategy_display_name": "Test Strategy",
    }

    def test_injected_keys_filtered_from_dataclass(self, capsys) -> None:
        config = coerce_strategy_config(_UnionStrategy, {**self._INJECTED, "plain": "0.25"})

        # Dataclass constructed (no silent DictConfigWrapper fallback) with
        # the real field preserved and no injected keys leaked as attributes.
        assert isinstance(config, _UnionConfig)
        assert config.plain == Decimal("0.25")
        for key in self._INJECTED:
            assert not hasattr(config, key)

        # Injected keys are expected, not "unknown" -> no ignored warning.
        assert "ignored:" not in capsys.readouterr().out

    def test_unexpected_key_still_reported_as_ignored(self, capsys) -> None:
        config = coerce_strategy_config(_UnionStrategy, {**self._INJECTED, "bogus_key": 1})

        assert isinstance(config, _UnionConfig)
        out = capsys.readouterr().out
        assert "ignored:" in out
        assert "bogus_key" in out


class TestDictFallback:
    def test_no_generic_wraps_in_dict_config_wrapper(self) -> None:
        config = coerce_strategy_config(_NoGenericStrategy, {"foo": "bar"})
        assert isinstance(config, DictConfigWrapper)
        assert config.foo == "bar"

    def test_run_py_reexport_is_the_same_class(self) -> None:
        """Existing importers (`from almanak.framework.cli.run import ...`)
        must keep resolving to the one moved class -- no isinstance splits."""
        from almanak.framework.cli.run import DictConfigWrapper as RunDictConfigWrapper

        assert RunDictConfigWrapper is DictConfigWrapper
