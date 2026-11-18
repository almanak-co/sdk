"""Frozen pre-conversion inputs used only by archival component replays.

A converted reference carries only the public author root. Tests of the harness
loader, the composed harness wire and the economic runners read the frozen
pre-conversion copy instead; the copy leaves with the harness loader.
"""

import gzip
import hashlib
import json
import re
from functools import cache
from itertools import chain, combinations
from pathlib import Path

from almanak.strategy import StrategyDefinition

ROOT = Path(__file__).parents[1]
FROZEN = ROOT / "tests/fixtures/author/legacy"

LEGACY_NAMESPACE = "almanak_v3"
LEGACY_PREFIXES = (
    ("from almanak_v3 import ", "from almanak import "),
    ("from almanak_v3.", "from almanak."),
)

# GMX revoked the RoleStore roles of the deployment the frozen GMX references name, on
# 16 and 17 September 2026. The immutable snapshots keep those addresses; a value decoded
# from one reads each retired role contract as its successor in the compiled closure.
RETIRED_GMX_ROLES = {
    "eip155:42161": {
        "0x1c3fa76e6e1088bce750f23a5bfcffa1efef6a41": "0x7de39ff2e232a2203196788d37e234cf8f1b83f1",
        "0x470fbc46bcc0f16532691df360a07d8bf5ee0789": "0xfa26cbb46e2614609406de08ca1dc7f70a684184",
        "0x7f01614ca5198ec979b1aad1daf0de7e0a215bdf": "0x26c02f221e8db5a821e12347c7ea8a6b6e10842f",
        "0x63492b775e30a9e6b4b4761c12605eb9d071d5e9": "0xa5d2d45228ee2e3a18ab122b2ce84997d008f4eb",
        "0x33871b8568edc4adf33338cdd8cf52a0ecc84d42": "0x2c60a1890e24a727ccf504a9da9912ace58b2ead",
        "0x11e9e7464f3bc887a7290ec41fcd22f619b177fd": "0xb25ddf7de751e5c8bfa85c815d1b379732cf4925",
        "0xc4eccce7e5231d453991f8c13459999b14afff24": "0x8f83a77a8f075466904b7b926dc3c80052a59ff8",
        "0xf7b962b085775a96a99e3dd38dfff09d7e270088": "0x05920f5788105ecf4003386904ce9f9dc296dcb7",
        "0xaffd408f3f95f83e3b6326c7bacbad08edfad6fe": "0x63170c576c0575852baa3c90f79468b88ec14209",
        "0x32c206d8ea6903c3ca5bded94877c72d92bde82a": "0xff38607a4e1f5f753a317ceb451f7b068df8257b",
    },
    "eip155:43114": {
        "0x8f550e53dfe96c055d5bdb267c21f268fcaf63b2": "0xc002db96e682fff6675966f959677285a0c45efa",
        "0x62cb8740e6986b29dc671b2eb596676f60590a5b": "0xa34320a507493c71fe35e982e496f7c5d1a7fa02",
        "0xe1d5a068c5b75e0c7ea1a9fe8ea056f9356c6ffd": "0x29220fa3b24279279c211701de4a7b035122b911",
        "0x823b558b4bc0a2c4974a0d8d7885aa1102d15dec": "0xc993ef170859dae0241a3c12b8186e456fa1c1b0",
        "0xcc2645e961514a694bca228686ec664933c70647": "0xe93fa5956199d1e395885ab10bbe45b0bd04f0f2",
        "0x334237f7d75497a22b1443f44ddccf95e72904a0": "0xa334b4e3f92a91e927f061494a95d84b7f0d134d",
        "0x63abc52a2750c7cb65d560b52627ec8f6207d9f9": "0x7e99d14db15fe16b58b05beb101bbc69c2203679",
        "0x40fe9ea67c9d29e77661837e986ef20a78c5e9ce": "0x8d3fec13225f611ad7f5fa77442625749a29b057",
        "0x92a542690f8a347e2ee9e51c2da0ea38e1186cdb": "0x7313c9fac0ad3511cf30a1dd603c3255b455fde8",
        "0x0dc24af5bbbec90c1defd8f5f72e47c7d298c0ae": "0xe1ed31707677d961ea8056c23e6153701e7414b9",
    },
}


def current_gmx_roles(value):
    """Return a frozen effective value with its retired GMX role contracts rotated."""
    if isinstance(value, list):
        return [current_gmx_roles(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {name: current_gmx_roles(item) for name, item in value.items()}
    if value.get("kind") == "gmx_v2" and "contracts" in value:
        rotation = RETIRED_GMX_ROLES[value["chain"]["chain_id"]]
        result["contracts"] = {
            role: rotation.get(address, address) for role, address in value["contracts"].items()
        }
    return result


# Author fields the current interface removed. A frozen value is archived history; its
# effective choices are compared without them. `repayment_ceiling` was removed when
# repayment became scoped to the fund's own debt (blueprint 25); the budget fields went
# when Roles grants became scope-only (blueprint 27).
RETIRED_FIELDS = frozenset(
    {
        "repayment_ceiling",
        "maximum0",
        "maximum1",
        "maximums",
        "maximum_input",
        "maximum_size_delta_usd_e30",
        "maximum_inputs",
        "maximum_market_token_input",
        "maximum_price_per_base_unit",
        "maximum_collateral_withdrawal_base_units",
        "supply_limit",
        "borrow_limit",
    }
)


def current_effective(value):
    """Return a frozen effective value in the current interface.

    Lending budgets are translated to `capital` and `borrow` before the retired fields go,
    so a frozen borrowing allocation still borrows.
    """
    from tests.lending_captures import current_config

    return without_retired_fields(current_gmx_roles(current_config(value)))


def without_retired_fields(value):
    if isinstance(value, list):
        return [without_retired_fields(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        name: without_retired_fields(item)
        for name, item in value.items()
        if name not in RETIRED_FIELDS
    }


def _matching_legacy_prefix(line):
    """Return the allowed from-import rewrite for one line, or None."""
    return next((pair for pair in LEGACY_PREFIXES if line.startswith(pair[0])), None)


def _convert_legacy_line(line):
    """Rewrite one strategy line, reporting whether it was a legacy import."""
    if LEGACY_NAMESPACE not in line:
        return line, False
    replacement = _matching_legacy_prefix(line)
    if replacement is None:
        raise ValueError("historical strategy namespace occurs outside a from-import")
    converted = line.replace(*replacement, 1)
    if LEGACY_NAMESPACE in converted:
        raise ValueError("historical strategy namespace occurs outside a from-import")
    return converted, True


def _rewrite_legacy_source(text):
    """Rewrite every legacy import in a strategy source string."""
    parts = []
    matched = False
    for line in text.splitlines(keepends=True):
        converted, was_import = _convert_legacy_line(line)
        parts.append(converted)
        matched = matched or was_import
    if not matched:
        raise ValueError("historical strategy has no legacy from-import")
    return "".join(parts)


def historical_strategy_current_imports(strategy: StrategyDefinition) -> StrategyDefinition:
    """Load an immutable captured strategy through the renamed SDK imports only."""
    source = _rewrite_legacy_source(strategy.source)
    if LEGACY_NAMESPACE in source:
        raise ValueError("historical strategy namespace conversion is incomplete")
    return StrategyDefinition(filename=strategy.filename, source=source)


def current_imports_if_legacy(strategy: StrategyDefinition) -> StrategyDefinition:
    """Convert a captured strategy only when it still carries the legacy namespace."""
    if LEGACY_NAMESPACE not in strategy.source:
        return strategy
    return historical_strategy_current_imports(strategy)


def capture_era_imports(strategy: StrategyDefinition) -> StrategyDefinition:
    """Restore the capture-era namespace on a current strategy for hash comparison.

    Frozen captures recorded strategy hashes before the namespace rename. The
    reverse rewrite is exact for the accepted from-import pairs and refuses any
    other occurrence of the historical namespace.
    """
    source = strategy.source
    if LEGACY_NAMESPACE in source:
        raise ValueError("current strategy unexpectedly carries the historical namespace")
    lines = []
    for line in source.splitlines(keepends=True):
        if line.startswith("from almanak import "):
            line = "from almanak_v3 import " + line[len("from almanak import ") :]
        elif line.startswith("from almanak."):
            line = "from almanak_v3." + line[len("from almanak.") :]
        lines.append(line)
    return StrategyDefinition(filename=strategy.filename, source="".join(lines))


def harness_directory(name: str) -> Path:
    """The directory holding a reference's harness files, frozen once it is converted."""
    frozen = FROZEN / name
    return frozen if frozen.is_dir() else ROOT / "strategies" / name


@cache
def frozen_effective(name: str, filename: str) -> dict:
    """Return one normalized value captured before public author conversion."""
    for path in sorted(FROZEN.glob("*.json.gz")):
        document = json.loads(gzip.decompress(path.read_bytes()))
        entry = document["references"].get(name)
        if entry is not None:
            record = entry["files"][filename]
            if record["status"] != "normalized":
                raise ValueError(f"{name}/{filename} was not a normalized legacy input")
            return current_effective(record["value"])
    raise ValueError(f"no frozen legacy input for {name}/{filename}")


def frozen_backtest_inputs(name: str, strategy):
    """Decode captured effective JSON into the typed historical component wire."""
    from almanak.backtest.inputs import HarnessSelection
    from almanak.configuration.composed import Author
    from almanak.connectors.composition import connector_for

    author = Author.model_validate_json(json.dumps(frozen_effective(name, "config.yaml")))
    wire = frozen_effective(name, "backtest-economic.yaml")
    binding = connector_for(author).backtest(wire["market_model"])
    settings = binding.settings_type.model_validate_json(json.dumps(wire))
    return HarnessSelection(author, settings, strategy)


# `hold_seconds` became optional (PR 175): every reference that reads it now checks
# `allocation.hold_seconds is not None` first. A captured source predates that guard; this
# applies exactly the reviewed edit, so a replay still runs the current reference source.
HOLD = "allocation.hold_seconds"
HOLD_GUARD = f"{HOLD} is not None"
# `collect_seconds` became optional on V4 liquidity (PR 193): the fixed-range V4 LP
# references check `allocation.collect_seconds is not None` before timing collection.
COLLECT = "allocation.collect_seconds"
COLLECT_GUARD = f"{COLLECT} is not None"
GUARDS = ((HOLD, HOLD_GUARD), (COLLECT, COLLECT_GUARD))
ATR_HOLD_LIMIT = "    if any(a.hold_seconds > HOLD_LIMIT for a in config.allocations):\n"
ATR_HOLD_REQUIRED = (
    "    if any(a.hold_seconds is None for a in config.allocations):\n"
    "        raise ValueError(\n"
    '            "this strategy needs hold_seconds: its trailing stop reads the candle window"\n'
    "        )\n"
)
ELAPSED = re.compile(
    rf"^(\s*)elapsed = (\w+) is not None and (.+ >= \2 \+ {re.escape(HOLD)})$", re.M
)
EXPIRED = re.compile(
    rf"^(\s*)expired = (current\.\w+) is not None and \(\n"
    rf"(\s*)(int\(.+\) >= \2 \+ {re.escape(HOLD)})\n"
    rf"\1\)$",
    re.M,
)


def reviewed_hold_guard(source: str) -> str:
    """Apply the reviewed optional-hold guard to a captured reference source."""
    source = ELAPSED.sub(
        lambda m: (
            f"{m[1]}elapsed = (\n{m[1]}    {m[2]} is not None\n"
            f"{m[1]}    and {HOLD_GUARD}\n{m[1]}    and {m[3]}\n{m[1]})"
        ),
        source,
    )
    source = EXPIRED.sub(
        lambda m: (
            f"{m[1]}expired = (\n{m[3]}{m[2]} is not None\n"
            f"{m[3]}and {HOLD_GUARD}\n{m[3]}and {m[4]}\n{m[1]})"
        ),
        source,
    )
    if ATR_HOLD_LIMIT in source and ATR_HOLD_REQUIRED not in source:
        source = source.replace(ATR_HOLD_LIMIT, ATR_HOLD_REQUIRED + ATR_HOLD_LIMIT)
    lines = source.split("\n")
    guarded = []
    for index, line in enumerate(lines):
        for field, guard in GUARDS:
            match = re.match(rf"^(\s*)and .* \+ {re.escape(field)}$", line)
            if match and guard not in lines[index - 1]:
                guarded.append(f"{match[1]}and {guard}")
        guarded.append(line)
    return "\n".join(guarded)


GUARDED_ELAPSED = re.compile(
    rf"^(\s*)elapsed = \(\n\1    (\w+) is not None\n\1    and {re.escape(HOLD_GUARD)}\n"
    rf"\1    and (.+)\n\1\)$",
    re.M,
)
GUARDED_EXPIRED = re.compile(
    rf"^(\s*)expired = \(\n(\s*)(current\.\w+) is not None\n\2and {re.escape(HOLD_GUARD)}\n"
    rf"\2and (.+)\n\1\)$",
    re.M,
)


def before_hold_guard(source: str) -> tuple[str, ...]:
    """Every source the reviewed optional-hold guard turns into this one.

    A guard line may predate the review, so removal is ambiguous; a digest pinned before
    the review must match one candidate, which keeps the comparison exact.
    """
    lines = source.split("\n")
    sites = [
        index
        for index, line in enumerate(lines)
        for field, guard in GUARDS
        if line.strip() == f"and {guard}"
        and index + 1 < len(lines)
        and re.match(rf"^\s*and .* \+ {re.escape(field)}$", lines[index + 1])
    ]
    candidates = {source, source.replace(ATR_HOLD_REQUIRED + ATR_HOLD_LIMIT, ATR_HOLD_LIMIT)}
    for removed in chain.from_iterable(combinations(sites, n) for n in range(1, len(sites) + 1)):
        kept = "\n".join(line for index, line in enumerate(lines) if index not in removed)
        candidates.add(kept)
    for text in tuple(candidates):
        collapsed = GUARDED_ELAPSED.sub(
            lambda m: f"{m[1]}elapsed = {m[2]} is not None and {m[3]}", text
        )
        collapsed = GUARDED_EXPIRED.sub(
            lambda m: f"{m[1]}expired = {m[3]} is not None and (\n{m[2]}{m[4]}\n{m[1]})",
            collapsed,
        )
        candidates.add(collapsed)
    return tuple(sorted(text for text in candidates if reviewed_hold_guard(text) == source))


def with_reviewed_hold_guard(strategy: StrategyDefinition) -> StrategyDefinition:
    """A captured strategy with the reviewed optional-hold guard applied."""
    return strategy.model_copy(update={"source": reviewed_hold_guard(strategy.source)})


def reviewed_current_imports(strategy: StrategyDefinition) -> StrategyDefinition:
    """A captured strategy with current imports, the reviewed optional-hold guard and the
    reviewed decision-reason edit."""
    current = current_imports_if_legacy(strategy)
    return StrategyDefinition(filename=current.filename, source=reviewed_source(current.source))


def before_hold_guard_digests(strategy: StrategyDefinition) -> set[str]:
    """Digests of every pre-review source of a current reference, in capture-era imports."""
    return {
        capture_era_imports(StrategyDefinition(filename=strategy.filename, source=text)).sha256
        for text in before_hold_guard(strategy.source)
    }


# Reference strategies then stated why they hold or act through the decision reason (#269).
# `decision-reason-edits.json` records that reviewed edit exactly, as replacement blocks per
# reference and the digest of the source each edits. Only that exact source reads as edited.
REASON_EDITS = ROOT / "tests/fixtures/decision-reason-edits.json"
# The lending references then moved `dashboard` into dashboard.py; `dashboard-split-edits.json`
# records the removed blocks the same way, applied after the decision-reason edit.
SPLIT_EDITS = ROOT / "tests/fixtures/dashboard-split-edits.json"
LAYERS = (REASON_EDITS, SPLIT_EDITS)


@cache
def layer(path: Path) -> dict:
    return json.loads(path.read_text())


def reason_edits() -> dict:
    return layer(REASON_EDITS)


def layer_pairs(path: Path, name: str) -> list[list[str]]:
    value = layer(path)
    return [value["pairs"][i] for i in value["references"][name]["pairs"]]


def reason_pairs(name: str) -> list[list[str]]:
    return layer_pairs(REASON_EDITS, name)


def _digest(source: str) -> str:
    return hashlib.sha256(source.encode()).hexdigest()


def with_layer(path: Path, source: str) -> str:
    digest = _digest(source)
    references = layer(path)["references"]
    name = next((n for n, r in references.items() if r["from"] == digest), None)
    if name is None:
        return source
    for old, new in layer_pairs(path, name):
        if source.count(old) != 1:
            raise ValueError("a reviewed reference block does not occur exactly once")
        source = source.replace(old, new, 1)
    return source


def before_layer(path: Path, name: str, source: str) -> str | None:
    if name not in layer(path)["references"]:
        return source
    for old, new in reversed(layer_pairs(path, name)):
        if source.count(new) != 1:
            return None
        source = source.replace(new, old, 1)
    return source if _digest(source) == layer(path)["references"][name]["from"] else None


def with_reviewed_reasons(source: str) -> str:
    """A reference source from before the decision-reason edit, with it and the dashboard split
    that followed; others unchanged."""
    for path in LAYERS:
        source = with_layer(path, source)
    return source


def before_reasons(name: str, source: str) -> str | None:
    """A current reference source with its dashboard split and decision-reason edit reversed, or
    None when they do not reverse to the recorded source; a reference left alone is unchanged."""
    for path in reversed(LAYERS):
        reversed_source = before_layer(path, name, source)
        if reversed_source is None:
            return None
        source = reversed_source
    return source


def reviewed_source(source: str) -> str:
    """A captured reference source with every reviewed edit since its capture."""
    return with_reviewed_reasons(reviewed_hold_guard(source))
