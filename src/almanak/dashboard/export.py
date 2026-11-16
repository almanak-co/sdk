"""Retained dashboard input: the projected context and snapshot of one committed checkpoint.

An evaluator outside the run renders the author's dashboard from this file instead of the live
journal, so display never touches execution and stopped deployments stay viewable.

`running.json` keeps the latest input taken while the strategy ran: a fork test ends by winding
everything down, and its dashboard is drawn from the run, not from the closed state after it.
"""

import json
import os
import time
from pathlib import Path

from almanak.dashboard import history, series
from almanak.dashboard.projection import project, record_digest

INPUT_VERSION = 1
RUNNING_INPUT = "running.json"
# An unchanged record is exported again after this many seconds, so its capture time stays
# recent; in between, the file and its revision stay as they are and nothing is re-uploaded.
UNCHANGED_REFRESH_SECONDS = 300
# The record digest and capture time this process last exported to each target. Only this
# process's own exports count, so a restarted coordinator always writes its projection once.
_exported: dict[Path, tuple[str, float]] = {}


def execution_effects(state):
    """The connector's token changes for an outcome; None when it cannot state them."""
    from almanak.connectors.composition import connector_for

    try:
        return getattr(connector_for(state.config), "execution_effects", None)
    except Exception:  # noqa: BLE001 - display only
        return None


def history_row(state, observed):
    """The connector family's reading of a decision's observation; None without a history."""
    from almanak.connectors.composition import connector_for

    reader = getattr(connector_for(state.config).projection, "history", None)
    return None if reader is None or not observed else reader(state, observed)


def export_input(
    directory: Path, state, *, captured_at: int | None = None, observed=None, prices=None
) -> dict | None:
    """Write the projected input; None when this process already exported the same record.

    `observed` is the decision's fresh observation: its reading joins the retained history even when
    the checkpoint did not change (a hold), and a new reading is exported with the record. `prices`
    are the series the decision read; they replace the kept ones."""
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = directory / "input.json"
    appended = history.append(directory, history_row(state, observed))
    if prices:
        series.keep_prices(directory, series.prices_of(prices, state))
        appended = True
    record_hash = record_digest(state)
    last = _exported.get(target)
    now = time.monotonic()
    if (
        captured_at is None
        and not appended
        and last is not None
        and last[0] == record_hash
        and now - last[1] < UNCHANGED_REFRESH_SECONDS
        and target.exists()
    ):
        return None
    previous = read_revision(target)
    context, snapshot = project(state, captured_at=captured_at)
    if context is not None:
        context = context.model_copy(
            update={
                "history": history.load(directory),
                "prices": series.load_prices(directory),
                "trades": series.trades_of(state, execution_effects(state)),
            }
        )
    document = {
        "input_version": INPUT_VERSION,
        "revision": previous + 1,
        "captured_at": int(time.time()) if captured_at is None else captured_at,
        "deployment_id": state.deployment_id,
        "record_hash": snapshot.record_hash,
        "snapshot": snapshot.model_dump(mode="json"),
        "context": None if context is None else context.model_dump(mode="json"),
    }
    write_atomic(target, document)
    if snapshot.mode == "running":
        write_atomic(directory / RUNNING_INPUT, document)
    _exported[target] = (record_hash, now)
    return document


def read_revision(target: Path) -> int:
    try:
        return int(json.loads(target.read_text()).get("revision", 0))
    except (OSError, ValueError, AttributeError):
        return 0


def write_atomic(target: Path, document: dict | list):
    temporary = target.with_suffix(".json.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(json.dumps(document) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
