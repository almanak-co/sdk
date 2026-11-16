"""Retained captures of reference strategies that predate scope-only authority.

Scope-only authority (blueprint 27, commit 9db3d11f9) edited the lending and composed
references with the author fields it removed: they read `capital` where they read
`supply_limit`, and borrow a constant `BORROW` in the loan asset's units where they
borrowed `borrow_limit`. `tests/fixtures/scope-only-strategy-edits.json` records that
reviewed edit exactly, as the commit's replacement blocks per reference.

A recorded source reads as the edited one only when it is exactly a reference's pre-edit
source (by digest), and only when the capture's own budgets decide what the edited
source decides: each supplying allocation started with its old `supply_limit` and each
borrowing allocation's `borrow_limit` is `BORROW` in its reserve asset's base units. Any
other capture keeps its recorded source, so comparing it with the current reference fails.

Typed strategy parameters (#230) then moved each reference's constants into
`strategy.parameters`. The fixture's `parameters` records that conversion the same way:
the replacement blocks per reference and the digest of the source they convert. An edited
source that is exactly that source reads as the current reference, and its capture's
config gains the reference's `config.yaml` parameters, which equal the replaced constants.
Where references that shared a pre-edit source now differ, the capture's chain singles
out the reference. The decision-reason edit (`legacy_references.with_reviewed_reasons`)
follows the conversion the same way.
"""

import hashlib
import json
import re
from decimal import Decimal
from functools import cache
from pathlib import Path

import yaml

from almanak.strategy import StrategyDefinition
from tests.legacy_references import (
    LEGACY_NAMESPACE,
    before_hold_guard,
    before_reasons,
    capture_era_imports,
    current_imports_if_legacy,
    with_reviewed_reasons,
)
from tests.lending_captures import STRATEGIES, captured_parameters, current_record

EDITS = Path(__file__).with_name("fixtures") / "scope-only-strategy-edits.json"
BORROW = re.compile(r'^BORROW = Decimal\("([^"]+)"\)$', re.M)


@cache
def fixture():
    return json.loads(EDITS.read_text())


def reference_pairs(name):
    value = fixture()
    return [value["pairs"][i] for i in value["references"][name]]


def digest_of(source):
    return hashlib.sha256(source.encode()).hexdigest()


def conversion(name):
    """The reference's recorded parameter conversion: the source it converts, its blocks."""
    layer = fixture()["parameters"]
    value = layer["references"].get(name, {"from": None, "pairs": []})
    return value["from"], [layer["pairs"][i] for i in value["pairs"]]


@cache
def edits():
    """The references edited from each pre-edit source, keyed by its digest."""
    result: dict[str, list] = {}
    for name in fixture()["references"]:
        result.setdefault(fixture()["before"][name], []).append(name)
    return result


def reversed_edit(source, pairs, digest):
    """`source` with `pairs` reversed when the result has `digest`, else None."""
    for old, new in reversed(pairs):
        if source.count(new) != 1:
            return None
        source = source.replace(new, old, 1)
    return source if digest_of(source) == digest else None


def unconverted_source(name, source):
    """A current reference source with its decision-reason edit and parameter conversion
    reversed, or None."""
    source = before_reasons(name, source)
    if source is None:
        return None
    converted, pairs = conversion(name)
    return reversed_edit(source, pairs, converted) if pairs else source


def unedited_source(name, source):
    """A reference source with its reviewed edits reversed, or None when they do not reverse.

    The result counts only when it is exactly the reference's recorded pre-edit source.
    """
    source = unconverted_source(name, source)
    if source is None:
        return None
    return reversed_edit(source, reference_pairs(name), fixture()["before"][name])


def before_scope_only_digests(name, strategy):
    """Capture-era digests of every pre-edit source the current reference descends from."""
    sources = (unedited_source(name, text) for text in before_hold_guard(strategy.source))
    return {
        capture_era_imports(StrategyDefinition(filename=strategy.filename, source=text)).sha256
        for text in sources
        if text is not None
    }


def apply_edit(source, pairs):
    for old, new in pairs:
        if source.count(old) != 1:
            raise ValueError("a reviewed scope-only block does not occur exactly once")
        source = source.replace(old, new, 1)
    return source


def converted_source(name, source):
    """An edited source with the reference's parameter conversion, when it converts exactly it,
    and the decision-reason edit that followed."""
    converted, pairs = conversion(name)
    source = apply_edit(source, pairs) if digest_of(source) == converted else source
    return with_reviewed_reasons(source)


def reviewed_edits(source):
    """(reference, edited source, current source) of each reference edited from `source`."""
    result = []
    for name in edits().get(digest_of(source), ()):
        edited = apply_edit(source, reference_pairs(name))
        result.append((name, edited, converted_source(name, edited)))
    return result


def edited_sources(source):
    """Every reviewed edit of an exact pre-edit reference source; empty for any other."""
    return {current for _, _, current in reviewed_edits(source)}


@cache
def reference_chains(name):
    raw = yaml.safe_load((STRATEGIES / name / "config.yaml").read_text())
    return frozenset(market["chain"] for market in raw["markets"].values())


def captured_chains(config):
    reserves = (a.get("reserve") for a in config.get("allocations", ()) if isinstance(a, dict))
    return {r["asset"].split("/")[0] for r in reserves if isinstance(r, dict) and "asset" in r}


def single_edit(matching, config):
    """The one current source left once the capture's chains pick among different ones."""
    if len({current for _, current in matching}) > 1:
        chains = captured_chains(config)
        matching = [(name, s) for name, s in matching if chains <= reference_chains(name)]
    sources = {current: name for name, current in matching}
    if len(sources) != 1:
        raise ValueError("recorded budgets do not single out one reviewed scope-only edit")
    return next(iter(sources.items()))


def borrowed_units(source, decimals):
    """`BORROW` of an edited source in base units of a reserve asset, or None."""
    amounts = BORROW.findall(source)
    return int(Decimal(amounts[0]).scaleb(decimals)) if len(amounts) == 1 else None


def same_allocation(before, after, source, metadata):
    """One recorded allocation's budgets equal what the edited source reads instead."""
    # An Aave V4 `amount` or a separate `entry_amount` is a budget this check does not
    # relate to the edited source, so such a capture is never mapped.
    if {"amount", "entry_amount"} & set(before):
        return False
    if "supply_limit" not in before:
        return True
    borrowed = int(before["borrow_limit"])
    decimals = metadata.get(after["reserve"]["asset"], {}).get("decimals")
    return after["capital"] == before["supply_limit"] and (
        borrowed == 0 or decimals is not None and borrowed_units(source, decimals) == borrowed
    )


def same_decisions(recorded, current, source):
    """The recorded budgets equal what the edited source decides.

    A pre-edit source ran with lending budgets, so a config without any `supply_limit`
    (one already mapped, for instance) proves nothing and is refused.
    """
    metadata = current.get("asset_metadata") or {}
    pairs = list(zip(recorded["allocations"], current["allocations"], strict=True))
    return any("supply_limit" in before for before, _ in pairs) and all(
        same_allocation(before, after, source, metadata) for before, after in pairs
    )


def with_scope_only_strategy(wire, recorded_config):
    """A mapped record whose recorded pre-edit reference source carries the reviewed edit.

    `recorded_config` is the capture's own config; `wire["config"]` is already mapped.
    References that shared one pre-edit source were edited to borrow different amounts,
    so the capture's own budgets must single out exactly one edit.
    """
    strategy = wire.get("strategy")
    if not isinstance(strategy, dict):
        return wire
    recorded = StrategyDefinition.model_validate(strategy)
    candidates = reviewed_edits(current_imports_if_legacy(recorded).source)
    if not candidates:
        return wire
    matching = [
        (name, current)
        for name, edited, current in candidates
        if same_decisions(recorded_config, wire["config"], edited)
    ]
    source, name = single_edit(matching, wire["config"])
    edited = StrategyDefinition(filename=recorded.filename, source=source)
    if LEGACY_NAMESPACE in recorded.source:
        edited = capture_era_imports(edited)
    config = {**wire["config"], "parameters": captured_parameters(name)}
    return {**wire, "config": config, "strategy": edited.model_dump(mode="json")}


def reviewed_record(wire):
    """`current_record` of a capture, its pre-edit reference source carrying the edit."""
    mapped = current_record(wire)
    recorded = wire.get("config")
    return with_scope_only_strategy(mapped, recorded) if isinstance(recorded, dict) else mapped


def reviewed_json(raw):
    """`reviewed_record` of recorded JSON text or bytes, as JSON text."""
    return json.dumps(reviewed_record(json.loads(raw)))
