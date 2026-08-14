"""Unit tests for synthetic permission-vector dispatch."""

import pytest

from almanak.core.intent_types import IntentType


class TestMissingDefaultPairOverrideMembership:
    """Pair-independent overrides still require declared SWAP/LP membership."""

    @pytest.mark.parametrize(
        ("intent_type", "membership_accessor"),
        [
            (IntentType.SWAP, "_swap_protocols"),
            (IntentType.LP_OPEN, "_lp_protocols"),
            (IntentType.LP_CLOSE, "_lp_protocols"),
        ],
    )
    def test_override_is_not_called_after_membership_removal(
        self,
        monkeypatch: pytest.MonkeyPatch,
        intent_type: IntentType,
        membership_accessor: str,
    ) -> None:
        from almanak.framework.permissions import synthetic_intents

        override_called = False

        def stub_override(_protocol: str):
            def build(*_args: object, **_kwargs: object) -> list[object]:
                nonlocal override_called
                override_called = True
                return [object()]

            return build

        monkeypatch.setattr(synthetic_intents, "_get_token_pair_or_none", lambda _chain: None)
        monkeypatch.setattr(synthetic_intents, membership_accessor, lambda: frozenset())
        monkeypatch.setattr(synthetic_intents, "get_discovery_vectors_override", stub_override)

        assert synthetic_intents.build_synthetic_intents("curve", intent_type, "ethereum") == []
        assert not override_called
