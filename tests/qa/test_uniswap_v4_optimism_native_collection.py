"""Prepared collection limits must not leak into a detailed model's native cadence."""

import pytest

from almanak.configuration.backtest_inputs import read_backtest_inputs
from tests.qa.catalog import catalog
from tests.qa.inputs import resolved_author, resolved_history


@pytest.mark.parametrize("style", ["lp", "trading"])
@pytest.mark.parametrize("mode", ["economic", "historical_path"])
def test_native_cadence_preserves_model_specific_collection_settings(tmp_path, style, mode):
    case = next(
        case
        for case in catalog()
        if case.reference == f"uniswap_v4_optimism_{style}"
        and case.input_policy == "native"
        and case.model_mode == mode
    )
    resolved_author(case, tmp_path, {})
    settings = read_backtest_inputs(resolved_history(case, tmp_path, 2000000000)).inputs
    schedule = settings.schedule()
    assert "decision_interval_override_seconds" not in schedule
    assert schedule["end"] - schedule["start"] == 600
    assert schedule["cleanup_seconds"] == 600
    if mode == "economic":
        assert settings.data["max_samples"] == 21
    else:
        assert not settings.data


@pytest.mark.parametrize(
    ("policy", "mode", "expected"),
    [
        ("native", "economic", 1800),
        ("pinned", "economic", 1800),
        ("native", "historical_path", 1800),
    ],
)
def test_default_native_preparation_budget_stays_scoped(policy, mode, expected):
    from dataclasses import replace

    from tests.qa import inputs

    case = next(case for case in catalog() if case.reference == "optimism_aave_borrow_and_trade_v4")
    assert (
        inputs.preparation_timeout(replace(case, input_policy=policy, model_mode=mode)) == expected
    )


@pytest.mark.parametrize("value", [0, -1, True, "7200"])
def test_preparation_budget_rejects_invalid_policy(tmp_path, monkeypatch, value):
    import json
    from dataclasses import replace

    from tests.qa import inputs

    directory = tmp_path / "tests/qa"
    directory.mkdir(parents=True)
    (directory / "policy.json").write_text(
        json.dumps(
            {"native_preparation_timeout_seconds": {"optimism_aave_borrow_and_trade_v4": value}}
        )
    )
    monkeypatch.setattr(inputs, "ROOT", tmp_path)
    case = next(case for case in catalog() if case.reference == "optimism_aave_borrow_and_trade_v4")
    with pytest.raises(ValueError, match="positive integer"):
        inputs.preparation_timeout(replace(case, input_policy="native", model_mode="economic"))


def test_preparation_budget_is_forwarded_without_extending_offline_reuse(tmp_path, monkeypatch):
    import json

    from tests.qa import worker

    calls = []
    monkeypatch.setattr(worker, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    assert worker.prepare_dataset(tmp_path, [], [], timeout=7200) == [
        "--dataset",
        tmp_path / "dataset",
    ]
    assert [call[0][2] for call in calls] == ["cold-prepare", "cached-prepare"]
    assert calls[0][1] == {"timeout": 7200}
    assert calls[1][1] == {"offline": True}
    assert json.loads((tmp_path / "preparation-policy.json").read_text())["timeout_seconds"] == 7200
