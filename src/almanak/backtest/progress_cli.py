"""CLI adapter for the shared progress contract; stdout remains the result JSON."""

import hashlib
import json
import os
import platform
import signal
import sys
from contextlib import contextmanager

from almanak.backtest.prepared import require_private_directory
from almanak.backtest.progress import ProgressJob, phase


def inputs(args):
    if args.product in ("backtest-resume", "backtest-reproduce"):
        manifest = json.loads((args.run / "manifest.json").read_text())
        return (
            manifest["settings"],
            manifest["config"],
            hashlib.sha256(manifest["strategy"]["source"].encode()).hexdigest(),
        )
    from almanak.backtest.inputs import selected

    selection = selected(args)
    if selection.strategy is None:
        raise ValueError("backtesting requires a strategy")
    return selection.settings_view(), selection.authored_view(), selection.strategy.sha256


def cache_mode(args):
    if getattr(args, "dataset", None) is not None:
        return "selected_dataset"
    if args.product in ("backtest-resume", "backtest-reproduce"):
        return "retained_run"
    directory = args.output
    if args.product == "backtest":
        from almanak.backtest.inputs import selected

        selection = selected(args)
        if selection.market_model != "economic":
            return "cold"
        # The dataset identity needs resolved reserves, so a public root resolves
        # here, into the run directory, before the progress job starts.
        author, settings = selection.resolve(args.output.resolve(), getattr(args, "env_file", None))
        binding = selection.connector.backtest("economic")
        directory = binding.dataset_directory(args, author, settings)
    if (directory / "dataset.json").exists():
        return "completed_dataset"
    return "partial_capture" if (directory / "observations.sqlite").exists() else "cold"


def timing_key(args, settings, config, strategy_hash, cache):
    from almanak.backtest.cli import sdk_digest

    settings = {**settings, "schedule": dict(settings["schedule"])}
    schedule = settings["schedule"]
    start, end = schedule.pop("start"), schedule.pop("end")
    schedule["window_seconds"] = end - start
    context = {
        "settings": settings,
        "config": config,
        "strategy": strategy_hash,
        "sdk": sdk_digest(),
        "operation": args.product,
        "cache": cache,
        "environment": [
            getattr(args, "timing_scope", None) or platform.node(),
            platform.system(),
            platform.machine(),
            platform.python_version(),
            os.cpu_count(),
        ],
    }
    return hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()


def paths(args):
    output = getattr(args, "output", None) or args.run
    output = output.resolve()
    suffix = (
        ".reproduce.progress.json" if args.product == "backtest-reproduce" else ".progress.json"
    )
    progress = getattr(args, "progress_file", None) or output.with_name(output.name + suffix)
    history = getattr(args, "timing_history", None) or output.parent / ".backtest-timings.json"
    progress, history = progress.resolve(), history.resolve()
    for path in (progress, history):
        require_private_directory(path.parent)
        if path.is_relative_to(output):
            raise ValueError(
                "operational progress/timing files must stay outside the run or dataset directory"
            )
    protected = {
        getattr(args, name, None) for name in ("config", "strategy", "backtest_config", "env_file")
    }
    if progress == history or any(
        p is not None and p.resolve() in (progress, history) for p in protected
    ):
        raise ValueError("progress and timing files must differ from each other and command inputs")
    return progress, history


def initial_plan(job, args, settings, cache):
    job.plan("startup", "Validating inputs", 1.0, "phases")
    job.plan("finalization", "Finalizing result", 1.0, "phases")
    prepare = args.product == "backtest-prepare" or (
        args.product == "backtest"
        and settings["market_model"] == "economic"
        and getattr(args, "dataset", None) is None
    )
    if prepare:
        if cache == "completed_dataset":
            job.discard("preparation")
            job.plan("preparation.reuse", "Verifying cached dataset", 1.0, "phases")
        elif not any(key.startswith("preparation.") for key in job.phases):
            job.plan("preparation", "Preparing historical inputs")
    else:
        job.discard("preparation")
    if args.product != "backtest-prepare":
        if settings["market_model"] == "economic":
            job.plan("simulation_setup", "Opening frozen simulation inputs", 1.0, "phases")
        schedule = settings["schedule"]
        job.plan(
            "simulation",
            "Simulating strategy",
            float(schedule["end"] - schedule["start"]),
            "simulation_seconds",
        )
        if schedule["end_policy"] == "teardown":
            job.plan(
                "teardown",
                "Closing remaining obligations",
                None,
                "simulation_seconds",
                estimated=True,
            )
    job.begin("startup", "Validating inputs", 1.0, "phases")


def progress_stream(args):
    mode = getattr(args, "progress", "auto")
    if mode == "none" or (mode == "auto" and not sys.stderr.isatty()):
        return None
    return sys.stderr


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


@contextmanager
def termination_signal():
    previous = signal.signal(signal.SIGTERM, interrupted)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def execute(args, operation, *, callback=None, result_sink=None):
    if args.product == "backtest" and args.output.exists():
        raise FileExistsError("backtest output exists; use backtest-resume or another output")
    path, history = paths(args)
    settings, config, strategy_hash = inputs(args)
    cache = cache_mode(args)
    key = timing_key(args, settings, config, strategy_hash, cache)
    resume = args.product in ("backtest-resume", "backtest-prepare")
    with (
        termination_signal(),
        ProgressJob(
            path,
            key,
            history=history,
            callback=callback,
            stream=progress_stream(args),
            json_events=getattr(args, "progress", "auto") == "json",
            resume=resume,
        ) as job,
    ):
        initial_plan(job, args, settings, cache)
        result = operation(args)
        with phase("finalization", "Finalizing result", 1.0, "phases"):
            if result_sink is not None:
                with job.lock:
                    job.observe(job.clear_terminal)
                    result_sink(result)
    return result
