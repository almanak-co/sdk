"""What the runtime knew when it evaluated a decision, logged for the test verdict.

One `decision_evaluated` line per evaluated decision states facts only: whether the
strategy ran, what it returned, which allocations were observed, each declared price
series' completeness and every chain that was not ready. Why a strategy held is its
own logic, which the runtime does not know and does not guess: `reason` is only what
the strategy stated with its decision, null when it stated none. These facts let a
reader rule causes in or out (an incomplete series, an unavailable chain) from evidence.
"""

import json
import sys
from pathlib import Path

EVENT = "decision_evaluated"
# Fields the test verdict adds to its decision row; its result comes from the checkpoint.
VERDICT_FIELDS = (
    "chain",
    "evaluated",
    "observed",
    "price_series",
    "availability",
    "price_error",
    "observation_errors",
)
# A reported observation error longer than this is cut; the full text stays in the log.
ERROR_CHARACTERS = 500
# The strategy's stated reason is cut to this length; the runtime keeps no other copy.
REASON_CHARACTERS = 300


def log(context, decision=None, price_error=None, chain=None) -> None:
    """Print the decision's facts; neither a fact nor the write ever stops the decision."""
    try:
        line = json.dumps(facts(context, decision, price_error, chain))
        print(line, file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001 - diagnostics only
        return


def facts(context, decision=None, price_error=None, chain=None) -> dict:
    """The decision's facts; `chain` is the chain whose tick evaluated it, when scoped."""
    intent = None if decision is None else decision.intent
    document = {
        "event": EVENT,
        "time": context.trigger.model_dump(mode="json")["started_at"],
        "trigger": context.trigger.kind,
        "chain": None if chain is None else chain.model_dump(mode="json"),
        "evaluated": price_error is None,
        "result": "hold" if intent is None else "intent",
    }
    if intent is not None:
        document["intent"] = getattr(intent, "kind", type(intent).__name__)
    reason = getattr(decision, "reason", None)
    document["reason"] = reason[:REASON_CHARACTERS] if isinstance(reason, str) else None
    document["observed"] = sorted(
        {
            item.allocation
            for item in context.observations
            if isinstance(getattr(item, "allocation", None), str)
        }
    )
    document["observation_errors"] = observation_errors(context.observations)
    document["price_series"] = [series_facts(series) for series in getattr(context, "prices", ())]
    document["availability"] = [
        item.model_dump(mode="json")
        for item in getattr(context, "availability", ())
        if item.status != "ready"
    ]
    if price_error is not None:
        document["price_error"] = {"kind": price_error.kind, "message": str(price_error)}
    return document


def observation_errors(observations) -> list[dict]:
    """Every non-empty `*_error` field an allocation's observation recorded.

    An observation records such a field instead of failing when one measurement is
    unavailable, an entry quote on an empty pool for example; the strategy then sees
    no entry to take. `field` is the dotted path inside the observation.
    """
    rows: list[dict] = []
    for item in observations:
        allocation = getattr(item, "allocation", None)
        dump = getattr(item, "model_dump", None)
        if isinstance(allocation, str) and dump is not None:
            for field, message in error_fields(dump(mode="json")):
                rows.append({"allocation": allocation, "field": field, "message": message})
    return rows


def error_fields(value, path=""):
    if isinstance(value, dict):
        for key, item in value.items():
            name = f"{path}.{key}" if path else str(key)
            if str(key).endswith("_error") and isinstance(item, str) and item:
                yield name, item[:ERROR_CHARACTERS]
            else:
                yield from error_fields(item, name)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from error_fields(item, f"{path}.{index}")


def series_facts(series) -> dict:
    from almanak.price_series import PoolSeriesSource

    binding = series.binding
    latest = series.latest
    row = {
        "asset": binding.model_dump(mode="json")["asset"],
        "complete": series.complete,
        "points": len(series.points),
        "window": binding.window,
    }
    if isinstance(binding.source, PoolSeriesSource):
        row["timeframe"] = binding.source.timeframe
        row["gaps"] = len(series.gaps)
        row["max_age_seconds"] = binding.source.max_age_seconds
    row["latest_at"] = None if latest is None else latest.source_time
    row["age_seconds"] = None if latest is None else series.cutoff - latest.source_time
    return row


def logged(path: Path) -> dict[str, list[dict]]:
    """Each decision time's logged facts as verdict fields, one entry per chain.

    Chains of one deployment are evaluated in turn, so two chains may log the same time;
    each keeps its own facts and `chain` tells them apart. Two lines for one chain and
    time cannot be told apart, so neither is kept.
    """
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return {}
    found: dict[str, dict[str, dict | None]] = {}
    for line in lines:
        if EVENT not in line:
            continue
        try:
            document = json.loads(line)
        except ValueError:
            continue
        if not isinstance(document, dict) or document.get("event") != EVENT:
            continue
        time = document.get("time")
        if isinstance(time, str):
            fields = {key: document[key] for key in VERDICT_FIELDS if key in document}
            if fields.get("chain") is None:
                fields.pop("chain", None)
            chain = json.dumps(fields.get("chain"), sort_keys=True)
            by_chain = found.setdefault(time, {})
            by_chain[chain] = None if chain in by_chain else fields
    return {
        time: [fields for fields in by_chain.values() if fields is not None]
        for time, by_chain in found.items()
    }


def matching(candidates: list[dict], chain) -> dict:
    """The facts of the one evaluation a verdict row is: the only one at its time, else
    the one on `chain`. Rows that cannot be told apart get none rather than another's,
    and facts logged on another chain than the row's intent never attach."""
    if chain is not None:
        candidates = [facts for facts in candidates if facts.get("chain") in (None, chain)]
    return candidates[0] if len(candidates) == 1 else {}


def teardown_evaluations(path: Path) -> list[dict]:
    """Every teardown decision the coordinator logged, in evaluation order.

    Teardown decisions are kept apart from the verdict's decision rows: a hold there is
    the strategy's closure claim, so a teardown that holds with obligations open is seen
    only here. Rows are not keyed by time, which a fork clock can repeat.
    """
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    rows = []
    for line in lines:
        if EVENT not in line:
            continue
        try:
            document = json.loads(line)
        except ValueError:
            continue
        if not isinstance(document, dict) or document.get("event") != EVENT:
            continue
        if document.get("trigger") != "teardown":
            continue
        row = {"time": document.get("time"), "result": document.get("result")}
        if isinstance(document.get("intent"), str):
            row["intents"] = [document["intent"]]
        row.update({key: document[key] for key in VERDICT_FIELDS if key in document})
        if row.get("chain") is None:
            row.pop("chain", None)
        rows.append(row)
    return rows
