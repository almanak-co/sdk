"""Tests for DataMeta, DataEnvelope, and DataClassification models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _SamplePrice:
    """Minimal stub used as the envelope's wrapped value."""

    price: Decimal
    tick: int


def _make_meta(**overrides) -> DataMeta:
    defaults = {
        "source": "test_source",
        "observed_at": datetime.now(UTC),
    }
    defaults.update(overrides)
    return DataMeta(**defaults)


# ---------------------------------------------------------------------------
# DataMeta tests
# ---------------------------------------------------------------------------


class TestDataMeta:
    def test_basic_construction(self):
        now = datetime.now(UTC)
        meta = DataMeta(source="alchemy_rpc", observed_at=now, block_number=19_000_000)
        assert meta.source == "alchemy_rpc"
        assert meta.observed_at == now
        assert meta.block_number == 19_000_000
        assert meta.finality == "off_chain"
        assert meta.confidence == 1.0
        assert meta.cache_hit is False

    def test_full_construction(self):
        now = datetime.now(UTC)
        meta = DataMeta(
            source="geckoterminal",
            observed_at=now,
            block_number=100,
            finality="finalized",
            staleness_ms=500,
            latency_ms=120,
            confidence=0.95,
            cache_hit=True,
        )
        assert meta.finality == "finalized"
        assert meta.staleness_ms == 500
        assert meta.latency_ms == 120
        assert meta.confidence == 0.95
        assert meta.cache_hit is True

    def test_frozen(self):
        meta = _make_meta()
        with pytest.raises(AttributeError):
            meta.source = "changed"  # type: ignore[misc]

    def test_is_on_chain_true(self):
        meta = _make_meta(block_number=1)
        assert meta.is_on_chain is True

    def test_is_on_chain_false(self):
        meta = _make_meta(block_number=None)
        assert meta.is_on_chain is False

    def test_is_finalized_true(self):
        meta = _make_meta(finality="finalized")
        assert meta.is_finalized is True

    def test_is_finalized_false_safe(self):
        meta = _make_meta(finality="safe")
        assert meta.is_finalized is False

    def test_is_finalized_false_latest(self):
        meta = _make_meta(finality="latest")
        assert meta.is_finalized is False

    def test_confidence_lower_bound(self):
        with pytest.raises(ValueError, match="confidence"):
            _make_meta(confidence=-0.1)

    def test_confidence_upper_bound(self):
        with pytest.raises(ValueError, match="confidence"):
            _make_meta(confidence=1.1)

    def test_confidence_boundary_zero(self):
        meta = _make_meta(confidence=0.0)
        assert meta.confidence == 0.0

    def test_confidence_boundary_one(self):
        meta = _make_meta(confidence=1.0)
        assert meta.confidence == 1.0

    def test_invalid_finality(self):
        with pytest.raises(ValueError, match="finality"):
            _make_meta(finality="unknown")

    def test_valid_finalities(self):
        for finality in ("finalized", "safe", "latest", "off_chain"):
            meta = _make_meta(finality=finality)
            assert meta.finality == finality


# ---------------------------------------------------------------------------
# DataClassification tests
# ---------------------------------------------------------------------------


class TestDataClassification:
    def test_execution_grade_value(self):
        assert DataClassification.EXECUTION_GRADE.value == "execution_grade"

    def test_informational_value(self):
        assert DataClassification.INFORMATIONAL.value == "informational"

    def test_enum_members(self):
        assert set(DataClassification) == {
            DataClassification.EXECUTION_GRADE,
            DataClassification.INFORMATIONAL,
        }


# ---------------------------------------------------------------------------
# DataEnvelope tests
# ---------------------------------------------------------------------------


class TestDataEnvelope:
    def test_basic_construction(self):
        meta = _make_meta()
        env = DataEnvelope(value=42, meta=meta)
        assert env.value == 42
        assert env.meta is meta
        assert env.classification == DataClassification.INFORMATIONAL

    def test_explicit_classification(self):
        meta = _make_meta()
        env = DataEnvelope(value="data", meta=meta, classification=DataClassification.EXECUTION_GRADE)
        assert env.is_execution_grade is True

    def test_transparent_delegation_attribute(self):
        meta = _make_meta()
        sample = _SamplePrice(price=Decimal("1800.50"), tick=200)
        env = DataEnvelope(value=sample, meta=meta)

        assert env.price == Decimal("1800.50")
        assert env.tick == 200

    def test_transparent_delegation_does_not_shadow_own_attrs(self):
        meta = _make_meta()
        sample = _SamplePrice(price=Decimal("1"), tick=0)
        env = DataEnvelope(value=sample, meta=meta)

        # 'meta' and 'value' are own attributes -- must NOT delegate
        assert env.meta is meta
        assert env.value is sample

    def test_delegation_raises_attribute_error_for_missing(self):
        meta = _make_meta()
        env = DataEnvelope(value=42, meta=meta)
        with pytest.raises(AttributeError, match="no_such_attr"):
            env.no_such_attr  # noqa: B018

    def test_is_fresh_true(self):
        meta = _make_meta(observed_at=datetime.now(UTC))
        env = DataEnvelope(value=1, meta=meta)
        assert env.is_fresh is True

    def test_is_fresh_false(self):
        old = datetime.now(UTC) - timedelta(minutes=5)
        meta = _make_meta(observed_at=old)
        env = DataEnvelope(value=1, meta=meta)
        assert env.is_fresh is False

    def test_is_execution_grade_false(self):
        meta = _make_meta()
        env = DataEnvelope(value=1, meta=meta)
        assert env.is_execution_grade is False

    def test_generic_typing_with_list(self):
        meta = _make_meta()
        env: DataEnvelope[list[int]] = DataEnvelope(value=[1, 2, 3], meta=meta)
        assert len(env.value) == 3

    def test_delegation_with_dict_value(self):
        meta = _make_meta()
        env = DataEnvelope(value={"key": "val"}, meta=meta)
        # dict has a 'keys' method
        assert callable(env.keys)

    def test_delegation_with_none_value(self):
        meta = _make_meta()
        env = DataEnvelope(value=None, meta=meta)
        with pytest.raises(AttributeError):
            env.anything  # noqa: B018
