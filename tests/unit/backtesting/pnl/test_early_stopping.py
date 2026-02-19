"""Unit tests for OptunaTuner early stopping and history export functionality.

Tests cover:
- EarlyStoppingCallback behavior
- OptunaTunerConfig patience/min_delta validation
- OptimizationResult with stopped_early flag
- OptimizationHistory export and JSON serialization
- Integration with Optuna study
"""

from __future__ import annotations

import json
import logging
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import optuna
import pytest

from almanak.framework.backtesting.pnl.optuna_tuner import (
    EarlyStoppingCallback,
    OptimizationHistory,
    OptimizationResult,
    OptunaTuner,
    OptunaTunerConfig,
    TrialHistoryEntry,
    discrete,
)

# =============================================================================
# EarlyStoppingCallback Tests
# =============================================================================


class TestEarlyStoppingCallback:
    """Tests for EarlyStoppingCallback."""

    def test_init_default_values(self) -> None:
        """Test default initialization."""
        callback = EarlyStoppingCallback()
        assert callback.patience == 10
        assert callback.min_delta == 0.0
        assert callback.direction == "maximize"
        assert callback.verbose is True
        assert callback.stopped_early is False
        assert callback.trials_without_improvement == 0
        assert callback.best_value is None

    def test_init_custom_values(self) -> None:
        """Test initialization with custom values."""
        callback = EarlyStoppingCallback(
            patience=5,
            min_delta=0.01,
            direction="minimize",
            verbose=False,
        )
        assert callback.patience == 5
        assert callback.min_delta == 0.01
        assert callback.direction == "minimize"
        assert callback.verbose is False

    def test_init_invalid_patience(self) -> None:
        """Test that patience < 1 raises error."""
        with pytest.raises(ValueError, match="patience must be >= 1"):
            EarlyStoppingCallback(patience=0)

        with pytest.raises(ValueError, match="patience must be >= 1"):
            EarlyStoppingCallback(patience=-1)

    def test_init_invalid_min_delta(self) -> None:
        """Test that negative min_delta raises error."""
        with pytest.raises(ValueError, match="min_delta must be >= 0"):
            EarlyStoppingCallback(min_delta=-0.1)

    def test_maximize_improvement_detection(self) -> None:
        """Test improvement detection for maximize direction."""
        callback = EarlyStoppingCallback(patience=3, direction="maximize")

        # Create mock study and trials
        study = MagicMock(spec=optuna.study.Study)

        # First trial - establishes baseline
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)
        assert callback.best_value == 1.0
        assert callback.trials_without_improvement == 0

        # Second trial - improvement
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.COMPLETE
        trial2.value = 1.5
        trial2.number = 1
        callback(study, trial2)
        assert callback.best_value == 1.5
        assert callback.trials_without_improvement == 0

        # Third trial - no improvement
        trial3 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial3.state = optuna.trial.TrialState.COMPLETE
        trial3.value = 1.4
        trial3.number = 2
        callback(study, trial3)
        assert callback.best_value == 1.5
        assert callback.trials_without_improvement == 1

    def test_minimize_improvement_detection(self) -> None:
        """Test improvement detection for minimize direction."""
        callback = EarlyStoppingCallback(patience=3, direction="minimize")

        study = MagicMock(spec=optuna.study.Study)

        # First trial
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)
        assert callback.best_value == 1.0

        # Second trial - lower is better for minimize
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.COMPLETE
        trial2.value = 0.5
        trial2.number = 1
        callback(study, trial2)
        assert callback.best_value == 0.5
        assert callback.trials_without_improvement == 0

        # Third trial - worse (higher)
        trial3 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial3.state = optuna.trial.TrialState.COMPLETE
        trial3.value = 0.6
        trial3.number = 2
        callback(study, trial3)
        assert callback.trials_without_improvement == 1

    def test_min_delta_threshold(self) -> None:
        """Test that min_delta threshold is respected."""
        callback = EarlyStoppingCallback(patience=3, min_delta=0.1, direction="maximize")

        study = MagicMock(spec=optuna.study.Study)

        # First trial
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)
        assert callback.best_value == 1.0

        # Second trial - improvement but below min_delta
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.COMPLETE
        trial2.value = 1.05  # Only 0.05 improvement, below min_delta of 0.1
        trial2.number = 1
        callback(study, trial2)
        assert callback.best_value == 1.0  # Not updated
        assert callback.trials_without_improvement == 1  # Counts as no improvement

        # Third trial - improvement above min_delta
        trial3 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial3.state = optuna.trial.TrialState.COMPLETE
        trial3.value = 1.15  # 0.15 improvement from baseline, above 0.1
        trial3.number = 2
        callback(study, trial3)
        assert callback.best_value == 1.15
        assert callback.trials_without_improvement == 0

    def test_early_stopping_triggers(self) -> None:
        """Test that early stopping triggers after patience exhausted."""
        callback = EarlyStoppingCallback(patience=2, direction="maximize")

        study = MagicMock(spec=optuna.study.Study)

        # First trial - baseline
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)

        # Second trial - no improvement
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.COMPLETE
        trial2.value = 0.9
        trial2.number = 1
        callback(study, trial2)
        assert callback.stopped_early is False
        study.stop.assert_not_called()

        # Third trial - still no improvement, triggers early stopping
        trial3 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial3.state = optuna.trial.TrialState.COMPLETE
        trial3.value = 0.8
        trial3.number = 2
        callback(study, trial3)
        assert callback.stopped_early is True
        study.stop.assert_called_once()

    def test_skips_pruned_trials(self) -> None:
        """Test that pruned trials don't affect patience counter."""
        callback = EarlyStoppingCallback(patience=3, direction="maximize")

        study = MagicMock(spec=optuna.study.Study)

        # First trial - baseline
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)

        # Pruned trial - should be skipped
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.PRUNED
        trial2.value = None
        trial2.number = 1
        callback(study, trial2)
        assert callback.trials_without_improvement == 0

    def test_handles_inf_values(self) -> None:
        """Test that inf values (failed backtests) count as no improvement."""
        callback = EarlyStoppingCallback(patience=2, direction="maximize")

        study = MagicMock(spec=optuna.study.Study)

        # First trial - baseline
        trial1 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial1.state = optuna.trial.TrialState.COMPLETE
        trial1.value = 1.0
        trial1.number = 0
        callback(study, trial1)

        # Trial with -inf (failed)
        trial2 = MagicMock(spec=optuna.trial.FrozenTrial)
        trial2.state = optuna.trial.TrialState.COMPLETE
        trial2.value = float("-inf")
        trial2.number = 1
        callback(study, trial2)
        assert callback.trials_without_improvement == 1

    def test_reset(self) -> None:
        """Test reset() clears all state."""
        callback = EarlyStoppingCallback(patience=3)

        # Set some state
        callback._best_value = 1.5
        callback._trials_without_improvement = 2
        callback._stopped_early = True
        callback._best_trial_number = 5

        callback.reset()

        assert callback._best_value is None
        assert callback._trials_without_improvement == 0
        assert callback._stopped_early is False
        assert callback._best_trial_number is None


# =============================================================================
# OptunaTunerConfig Tests
# =============================================================================


class TestOptunaTunerConfig:
    """Tests for OptunaTunerConfig patience/min_delta parameters."""

    def test_default_patience_disabled(self) -> None:
        """Test that patience is None by default (disabled)."""
        config = OptunaTunerConfig()
        assert config.patience is None
        assert config.min_delta == 0.0

    def test_patience_configuration(self) -> None:
        """Test patience configuration."""
        config = OptunaTunerConfig(patience=10, min_delta=0.01)
        assert config.patience == 10
        assert config.min_delta == 0.01

    def test_invalid_patience_validation(self) -> None:
        """Test that invalid patience raises error."""
        with pytest.raises(ValueError, match="patience must be >= 1"):
            OptunaTunerConfig(patience=0)

    def test_invalid_min_delta_validation(self) -> None:
        """Test that negative min_delta raises error."""
        with pytest.raises(ValueError, match="min_delta must be >= 0"):
            OptunaTunerConfig(min_delta=-0.1)

    def test_serialization(self) -> None:
        """Test to_dict includes patience and min_delta."""
        config = OptunaTunerConfig(patience=15, min_delta=0.05)
        data = config.to_dict()
        assert data["patience"] == 15
        assert data["min_delta"] == 0.05


# =============================================================================
# OptimizationResult Tests
# =============================================================================


class TestOptimizationResult:
    """Tests for OptimizationResult with stopped_early flag."""

    def test_default_stopped_early(self) -> None:
        """Test default stopped_early is False."""
        result = OptimizationResult(
            best_params={"x": 1},
            best_value=1.5,
            best_trial_number=5,
            n_trials=10,
            study_name="test",
            objective_metric="sharpe_ratio",
            direction="maximize",
        )
        assert result.stopped_early is False
        assert result.trials_without_improvement == 0

    def test_stopped_early_true(self) -> None:
        """Test stopped_early can be set to True."""
        result = OptimizationResult(
            best_params={"x": 1},
            best_value=1.5,
            best_trial_number=5,
            n_trials=10,
            study_name="test",
            objective_metric="sharpe_ratio",
            direction="maximize",
            stopped_early=True,
            trials_without_improvement=5,
        )
        assert result.stopped_early is True
        assert result.trials_without_improvement == 5

    def test_serialization(self) -> None:
        """Test to_dict includes stopped_early and trials_without_improvement."""
        result = OptimizationResult(
            best_params={"x": Decimal("1.5")},
            best_value=1.5,
            best_trial_number=5,
            n_trials=10,
            study_name="test",
            objective_metric="sharpe_ratio",
            direction="maximize",
            stopped_early=True,
            trials_without_improvement=3,
        )
        data = result.to_dict()
        assert data["stopped_early"] is True
        assert data["trials_without_improvement"] == 3
        assert data["best_params"]["x"] == "1.5"  # Decimal serialized to string


# =============================================================================
# TrialHistoryEntry Tests
# =============================================================================


class TestTrialHistoryEntry:
    """Tests for TrialHistoryEntry."""

    def test_basic_creation(self) -> None:
        """Test basic entry creation."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"x": 1, "y": 2.0},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        assert entry.trial_number == 0
        assert entry.state == "COMPLETE"
        assert entry.value == 1.5
        assert entry.duration_seconds == 60.0

    def test_serialization_with_decimal(self) -> None:
        """Test that Decimal params are serialized to strings."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"capital": Decimal("10000.50"), "ratio": 0.5},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        data = entry.to_dict()
        assert data["params"]["capital"] == "10000.50"
        assert data["params"]["ratio"] == 0.5


# =============================================================================
# OptimizationHistory Tests
# =============================================================================


class TestOptimizationHistory:
    """Tests for OptimizationHistory."""

    def test_basic_creation(self) -> None:
        """Test basic history creation."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"x": 1},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        history = OptimizationHistory(
            study_name="test_study",
            objective_metric="sharpe_ratio",
            direction="maximize",
            n_trials=5,
            n_complete=4,
            n_pruned=0,
            n_failed=1,
            best_trial_number=2,
            best_value=1.8,
            best_params={"x": 2},
            param_names=["x"],
            trials=[entry],
            stopped_early=False,
            export_timestamp="2026-01-27T11:00:00",
        )
        assert history.n_trials == 5
        assert history.n_complete == 4
        assert history.best_value == 1.8

    def test_serialization(self) -> None:
        """Test to_dict serialization."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"x": 1},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        history = OptimizationHistory(
            study_name="test_study",
            objective_metric="sharpe_ratio",
            direction="maximize",
            n_trials=1,
            n_complete=1,
            n_pruned=0,
            n_failed=0,
            best_trial_number=0,
            best_value=1.5,
            best_params={"x": 1},
            param_names=["x"],
            trials=[entry],
            stopped_early=True,
            export_timestamp="2026-01-27T11:00:00",
        )
        data = history.to_dict()
        assert data["stopped_early"] is True
        assert data["n_trials"] == 1
        assert len(data["trials"]) == 1

    def test_to_json(self) -> None:
        """Test JSON export."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"x": 1},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        history = OptimizationHistory(
            study_name="test_study",
            objective_metric="sharpe_ratio",
            direction="maximize",
            n_trials=1,
            n_complete=1,
            n_pruned=0,
            n_failed=0,
            best_trial_number=0,
            best_value=1.5,
            best_params={"x": 1},
            param_names=["x"],
            trials=[entry],
            stopped_early=False,
            export_timestamp="2026-01-27T11:00:00",
        )
        json_str = history.to_json()
        parsed = json.loads(json_str)
        assert parsed["study_name"] == "test_study"
        assert parsed["best_value"] == 1.5

    def test_save_to_file(self) -> None:
        """Test saving history to file."""
        entry = TrialHistoryEntry(
            trial_number=0,
            state="COMPLETE",
            value=1.5,
            params={"x": 1},
            datetime_start="2026-01-27T10:00:00",
            datetime_complete="2026-01-27T10:01:00",
            duration_seconds=60.0,
        )
        history = OptimizationHistory(
            study_name="test_study",
            objective_metric="sharpe_ratio",
            direction="maximize",
            n_trials=1,
            n_complete=1,
            n_pruned=0,
            n_failed=0,
            best_trial_number=0,
            best_value=1.5,
            best_params={"x": 1},
            param_names=["x"],
            trials=[entry],
            stopped_early=False,
            export_timestamp="2026-01-27T11:00:00",
        )

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            history.save(f.name)
            f.seek(0)

        # Read and verify
        with open(f.name) as f:
            loaded = json.load(f)
        assert loaded["study_name"] == "test_study"

        # Clean up
        Path(f.name).unlink()

    def test_decimal_params_serialization(self) -> None:
        """Test that Decimal params in best_params are serialized."""
        history = OptimizationHistory(
            study_name="test",
            objective_metric="sharpe_ratio",
            direction="maximize",
            n_trials=1,
            n_complete=1,
            n_pruned=0,
            n_failed=0,
            best_trial_number=0,
            best_value=1.5,
            best_params={"capital": Decimal("50000.25")},
            param_names=["capital"],
            trials=[],
            stopped_early=False,
            export_timestamp="2026-01-27T11:00:00",
        )
        data = history.to_dict()
        assert data["best_params"]["capital"] == "50000.25"


# =============================================================================
# OptunaTuner Integration Tests
# =============================================================================


class TestOptunaTunerEarlyStoppingIntegration:
    """Integration tests for OptunaTuner with early stopping."""

    def test_tuner_init_with_patience(self) -> None:
        """Test OptunaTuner initialization with patience."""
        tuner = OptunaTuner(
            objective_metric="sharpe_ratio",
            patience=10,
            min_delta=0.01,
        )
        assert tuner.config.patience == 10
        assert tuner.config.min_delta == 0.01
        assert tuner._early_stopping_callback is None  # Not created until optimize()

    def test_tuner_init_with_config(self) -> None:
        """Test OptunaTuner initialization with config object."""
        config = OptunaTunerConfig(
            objective_metric="sharpe_ratio",
            patience=15,
            min_delta=0.005,
        )
        tuner = OptunaTuner(config=config)
        assert tuner.config.patience == 15
        assert tuner.config.min_delta == 0.005

    def test_export_history_empty_study(self) -> None:
        """Test export_history with empty study."""
        tuner = OptunaTuner(objective_metric="sharpe_ratio")
        history = tuner.export_history()

        assert history.n_trials == 0
        assert history.n_complete == 0
        assert history.best_trial_number is None
        assert history.best_value is None
        assert history.best_params is None
        assert len(history.trials) == 0

    def test_export_history_with_trials(self) -> None:
        """Test export_history after some optimization trials."""
        tuner = OptunaTuner(objective_metric="sharpe_ratio")

        # Manually add some trials to the study
        def dummy_objective(trial: optuna.trial.Trial) -> float:
            x = trial.suggest_int("x", 1, 10)
            return float(x)

        tuner.study.optimize(dummy_objective, n_trials=3, show_progress_bar=False)
        tuner._param_ranges = {"x": discrete(1, 10)}

        history = tuner.export_history()

        assert history.n_trials == 3
        assert history.n_complete == 3
        assert history.best_trial_number is not None
        assert history.best_value is not None
        assert len(history.trials) == 3

    def test_save_history(self) -> None:
        """Test save_history convenience method."""
        tuner = OptunaTuner(objective_metric="sharpe_ratio")

        def dummy_objective(trial: optuna.trial.Trial) -> float:
            x = trial.suggest_int("x", 1, 10)
            return float(x)

        tuner.study.optimize(dummy_objective, n_trials=2, show_progress_bar=False)
        tuner._param_ranges = {"x": discrete(1, 10)}

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tuner.save_history(f.name)

        # Verify file was created
        assert Path(f.name).exists()

        # Load and verify
        with open(f.name) as f:
            loaded = json.load(f)
        assert loaded["n_trials"] == 2

        # Clean up
        Path(f.name).unlink()


class TestEarlyStoppingWithOptunaStudy:
    """Tests for EarlyStoppingCallback with real Optuna study."""

    def test_callback_triggers_stop(self) -> None:
        """Test that callback actually stops the study."""
        callback = EarlyStoppingCallback(patience=2, direction="maximize")

        study = optuna.create_study(direction="maximize")

        # Objective that always returns 1.0 (no improvement)
        def constant_objective(trial: optuna.trial.Trial) -> float:
            trial.suggest_int("x", 1, 10)
            return 1.0

        # Should stop after patience + 1 trials
        study.optimize(
            constant_objective,
            n_trials=100,  # Won't reach this
            callbacks=[callback],
            show_progress_bar=False,
        )

        # Should have stopped at 3 trials (1 baseline + 2 without improvement)
        assert len(study.trials) == 3
        assert callback.stopped_early is True

    def test_callback_with_improving_objective(self) -> None:
        """Test that callback doesn't trigger when objective improves."""
        callback = EarlyStoppingCallback(patience=3, direction="maximize")

        study = optuna.create_study(direction="maximize")

        trial_count = [0]

        def improving_objective(trial: optuna.trial.Trial) -> float:
            trial.suggest_int("x", 1, 10)  # Needed for Optuna to track params
            trial_count[0] += 1
            return float(trial_count[0])  # Always improving

        study.optimize(
            improving_objective,
            n_trials=5,
            callbacks=[callback],
            show_progress_bar=False,
        )

        assert len(study.trials) == 5  # Completed all trials
        assert callback.stopped_early is False

    def test_callback_verbose_logging(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that callback logs when early stopping triggers."""
        callback = EarlyStoppingCallback(patience=1, direction="maximize", verbose=True)

        study = optuna.create_study(direction="maximize")

        def constant_objective(trial: optuna.trial.Trial) -> float:
            trial.suggest_int("x", 1, 10)
            return 1.0

        # Capture logs from the optuna_tuner module
        with caplog.at_level(
            logging.INFO,
            logger="almanak.framework.backtesting.pnl.optuna_tuner",
        ):
            study.optimize(
                constant_objective,
                n_trials=100,
                callbacks=[callback],
                show_progress_bar=False,
            )

        # Check for early stopping log message
        assert any("Early stopping triggered" in record.message for record in caplog.records)
