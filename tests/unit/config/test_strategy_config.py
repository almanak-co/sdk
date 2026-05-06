"""Schema-level tests for ``StrategyConfig`` (Phase 3, #2098 / #2101).

The model lives at ``almanak/config/strategy.py``. These tests lock in:

* Type coercion — ``Decimal`` fields accept both string and float inputs (the
  102 demo configs vary on this; both must validate identically).
* Mutual exclusion — ``chain`` and ``chains`` cannot both be set.
* Phase-3 leniency — unknown fields are allowed (``extra="allow"``) so the
  per-strategy dataclass extension mechanism keeps working until a follow-up
  migrates it to Pydantic. Tightening to ``extra="forbid"`` is a separate
  phase.
* Hard typing — fields whose type is unambiguous (e.g. ``chain: str | None``)
  reject obviously wrong inputs (``chain=123``).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.config.strategy import StrategyConfig


class TestStrategyConfigSchema:
    def test_chain_only_valid(self):
        """A config with just ``chain`` set validates."""
        cfg = StrategyConfig.model_validate({"chain": "arbitrum"})
        assert cfg.chain == "arbitrum"
        assert cfg.chains is None

    def test_chains_only_valid(self):
        """A multi-chain config with ``chains`` set validates."""
        cfg = StrategyConfig.model_validate({"chains": ["arbitrum", "base"]})
        assert cfg.chains == ["arbitrum", "base"]
        assert cfg.chain is None

    def test_chain_and_chains_rejects(self):
        """Setting both ``chain`` and ``chains`` raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StrategyConfig.model_validate(
                {"chain": "arbitrum", "chains": ["arbitrum", "base"]}
            )
        msg = str(exc_info.value)
        assert "cannot set both 'chain' and 'chains'" in msg

    def test_decimal_string_coerced(self):
        """Stringly-typed numeric (``"0.005"``) coerces to Decimal."""
        cfg = StrategyConfig.model_validate({"max_slippage": "0.005"})
        assert cfg.max_slippage == Decimal("0.005")
        assert isinstance(cfg.max_slippage, Decimal)

    def test_decimal_float_coerced(self):
        """Float numeric (``0.005``) coerces to Decimal identically."""
        cfg = StrategyConfig.model_validate({"max_slippage": 0.005})
        assert cfg.max_slippage == Decimal("0.005")
        assert isinstance(cfg.max_slippage, Decimal)

    def test_extra_field_allowed(self):
        """Unknown fields are permitted (Phase 3 leniency, ``extra="allow"``)."""
        cfg = StrategyConfig.model_validate(
            {"chain": "arbitrum", "strategy_specific_field": "abc"}
        )
        assert cfg.chain == "arbitrum"
        # Phase 3 leniency: unknown fields survive on the model so per-strategy
        # extensions can read them via ``__pydantic_extra__`` until migrated.
        assert cfg.model_extra == {"strategy_specific_field": "abc"}

    def test_anvil_funding_dict_shape(self):
        """``anvil_funding`` accepts heterogeneous values (str, int, float)."""
        cfg = StrategyConfig.model_validate(
            {"anvil_funding": {"WETH": "1.5", "USDC": 1000, "ETH": 100}}
        )
        # Pydantic coerces the union; values may be Decimal/int/float depending
        # on what Pydantic picks for the union resolution. Just check round-trip.
        assert set(cfg.anvil_funding.keys()) == {"WETH", "USDC", "ETH"}

    def test_unknown_chain_field_type_rejected(self):
        """``chain`` must be a string — integers raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            StrategyConfig.model_validate({"chain": 123})
        msg = str(exc_info.value)
        # Pydantic's error message names the failing field.
        assert "chain" in msg
