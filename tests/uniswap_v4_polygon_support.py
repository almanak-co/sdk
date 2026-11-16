"""Current Polygon references over hash-bound retained prepared evidence."""

import hashlib
import json
import shutil
from pathlib import Path

from almanak.backtest.cli import sdk_digest
from almanak.backtest.engine import Journal
from almanak.connectors.uniswap_v4.economic_binding import Manifest
from almanak.connectors.uniswap_v4.economic_book import PoolBook
from almanak.connectors.uniswap_v4.economic_model import EconomicModel
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.strategy import StrategyDefinition
from tests.uniswap_support import scope_only_json

ROOT = Path(__file__).parents[1]
CAPTURE = (
    ROOT / "docs/internal/reports/evidence/"
    "2026-09-21-polygon-v4-retained-economic-fixtures-bc42371e-v1"
)
PROVENANCE_SHA256 = "5782d5961d2f4ebfa626705a87508af03a52105b3c858ccad483683d2e0c6308"
SOURCE_COMMIT = "bc42371eb12d22b11b5ef74def6e583706e36e6e"
COMPATIBLE_PATHS = (
    "src/almanak/backtest/engine.py",
    "src/almanak/connectors/composition.py",
    "src/almanak/connectors/dex_economic_reporting.py",
    "src/almanak/connectors/uniswap_v4/backtest_binding.py",
    "src/almanak/connectors/uniswap_v4/economic_binding.py",
    "src/almanak/connectors/uniswap_v4/economic_book.py",
    "src/almanak/connectors/uniswap_v4/economic_model.py",
    "src/almanak/connectors/uniswap_v4/economic_state.py",
    "tests/qa/worker.py",
)
# Reviewed (captured digest, current digest) pairs that cannot change a fixed-range
# Polygon replay. Any other difference from the capture still refuses.
REVIEWED_SUCCESSORS = {
    # Banded economic backtests (band contract section 7) add band-only branches: the
    # book builds bracketed growth only for band targets, the model reads it only for
    # band positions, economic parameters add the band model check, the report adds
    # band fields only for band targets, and the log scope for a fixed range is unchanged.
    # Both Polygon references are fixed-range; the replay below re-verifies their results.
    # Residual counter dust (V4 blueprint) then adds a residual-only exit settlement, a
    # report entry only when a residual is measured and closure over that measurement;
    # neither reference holds dust, so their paths and results are unchanged.
    # Strategy-declared price series then add a series requirement and decision input
    # only when a strategy declares one; neither reference does, so their prepared
    # requirements, decision contexts and results are unchanged.
    # Reviewed hook profiles then add the hook's sampled terms to the economic snapshot
    # and the hook address to archival bindings, both only for hooked pools; the
    # Polygon references are hookless, so their reads and results are unchanged.
    # Repeating entries (blueprint 27 section 7) then omit the entry quote from exit
    # observations and outside running mode, and record why when a pool cannot fill an
    # entry instead of failing. Exits already followed first exposure, where the captured
    # one-shot rule offered no entry quote, and both captures completed without one failing.
    # The optional band envelope then changes only the band coverage checks in
    # `band_growth` and `band_bounds`, which the book reaches only for band targets.
    "src/almanak/connectors/uniswap_v4/economic_book.py": (
        "ad0adb229dc22e4db0ea2829669a2b99b910f209be85e22640ea16cbb5fb91a8",
        "b438deaae524ae8d8460abdef71a3dd284d00be874edab4f3b6d5cc778f7174f",
    ),
    "src/almanak/connectors/uniswap_v4/economic_model.py": (
        "8884fbd024e964a67a5ca274af933215d2cd0226a59a373bc2d8118e165b2f55",
        "d6d739793d8f2307a89b25eebaec84793e4bb803fddc6d0fe9ea56c83c15c3e6",
    ),
    "src/almanak/connectors/uniswap_v4/economic_binding.py": (
        "948c41df6254027a19f44999efce7e6a89f2e304edc0c1c28c17462fffc63198",
        "d142bf9549913f9d6d9348e010a84f29578f361d1ceec01939b2007927299995",
    ),
    "src/almanak/connectors/uniswap_v4/backtest_binding.py": (
        "212f7998568a22e14b8ef59f93b1a337161586586f32080c86b283bd1c990ca4",
        "b5ddabcc12d27a515be3c8bbf3e532d9500e66d88b81c924273d314b70f98e28",
    ),
    # Shared exchange reporting adds residual entries only for families that measure
    # exit residuals; the Uniswap V4 report does not use this module.
    "src/almanak/connectors/dex_economic_reporting.py": (
        "5e564861ce675ef8b30f40bb8b54192efc54087aa62ef0bdd77162e2ff142f00",
        "b559fc7ca17b77ae0348c7ebde1b27e26b75a0ded8af5bb3fc904c9e204520c0",
    ),
    # The references gain only the reviewed optional-hold (PR 175) and optional-collect
    # (PR 193) guards, which `tests.legacy_references.before_hold_guard` reverses exactly.
    # Both captures set `hold_seconds`, and the LP capture sets `collect_seconds`, so every
    # guard is true and the replayed decisions are unchanged.
    "strategies/uniswap_v4_polygon_lp/strategy.py": (
        "cfb1263d0e538b0dd995f18baf9e4f4a98fe813b1a1df8cc1a68f92702606a32",
        "a5c19b3c0058e8a2b1c841ae63dafe690569b9fc6cb4712cb472b62f06c4ce91",
    ),
    "strategies/uniswap_v4_polygon_trading/strategy.py": (
        "10e05ec21d52557b5702f5d51141acaca0d4cdef143160b2cba375c8fc011aaf",
        "98b67841386183f279c1a239d7039779a16fda4831af9e53b8fc733272efccd8",
    ),
    # Adds PancakeSwap V3 to the cleanup-scenario routing, then reads retained captures
    # without the retired budget fields (blueprint 27); backtest replay never reaches either.
    "tests/qa/worker.py": (
        "c8aff846dad97e506d95c9b534c224a57d9c1808fe8818539713beafe3cffebb",
        "f3c61ddf769282341ca3dfbfd9b85bc7860b986b839ac4bfce35b505f7b4ca7f",
    ),
}
EXPECTED = {
    "lp": {
        "identity": "93e0f9354b269727b16ae388d4c140b665b047814f2b10eb81636c8c45962694",
        "result": "90dec4a886458c8ccac129c98ae2f913fe7c571b4e73647fcc25386c86aba337",
        "selected": "backtest/pinned/uniswap_v4_polygon_lp/economic",
        "actions": (
            "swap_exact_input",
            "exit_exchange",
            "open_liquidity",
            "collect_liquidity_fees",
            "close_liquidity",
        ),
    },
    "trading": {
        "identity": "cfa3b2dccdc40d9ac66721a25762c9930b8dd91d77020210f0088d062c841e44",
        "result": "667a9bc55a931d485bee3c536d5fa54061980395d07146fe9a111eeeac88d81f",
        "selected": "backtest/pinned/uniswap_v4_polygon_trading/economic",
        "actions": ("swap_exact_input", "exit_exchange"),
    },
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reviewed_files(style):
    provenance_path = CAPTURE / "provenance.json"
    require(
        digest(provenance_path) == PROVENANCE_SHA256,
        "Polygon V4 fixture provenance differs from the reviewed extraction",
    )
    provenance = json.loads(provenance_path.read_text())
    require(
        provenance["source_commit"] == SOURCE_COMMIT and len(provenance["files"]) == 18,
        "Polygon V4 fixture provenance has an unexpected source or file set",
    )
    rows = {
        str(Path(row["path"]).relative_to(style)): row
        for row in provenance["files"]
        if row["style"] == style
    }
    expected_paths = {
        "dataset/data.sqlite",
        "dataset/dataset.json",
        "quoter-comparisons.json",
        "result/results.json",
        "result/results.json.completion.json",
        "run/manifest.json",
        "run/qa-assertions.json",
        "run/report.json",
        "run/state.sqlite",
    }
    require(
        set(rows) == expected_paths,
        "Polygon V4 fixture does not contain the reviewed public whitelist",
    )
    return rows


def verify_files(source, rows):
    for relative, row in rows.items():
        path = source / relative
        require(
            digest(path) == row["sha256"] and path.stat().st_size == row["bytes"],
            f"Polygon V4 fixture differs from the reviewed capture: {relative}",
        )


def compatible(captured, style, current=None):
    current = current or (lambda relative: digest(ROOT / relative))
    paths = (*COMPATIBLE_PATHS, f"strategies/uniswap_v4_polygon_{style}/strategy.py")

    def unchanged(relative):
        recorded, now = captured.get(relative), current(relative)
        return recorded is not None and (
            now == recorded or REVIEWED_SUCCESSORS.get(relative) == (recorded, now)
        )

    return all(unchanged(relative) for relative in paths)


def verify_result(source, style, expected):
    results_path = source / "result/results.json"
    results = json.loads(results_path.read_text())
    completion = json.loads((results_path.with_suffix(".json.completion.json")).read_text())
    require(
        results["identity"]["commit"] == SOURCE_COMMIT,
        "Polygon V4 result has an unexpected source commit",
    )
    require(
        compatible(results["identity"]["files"], style),
        "Polygon V4 fixture is incompatible with a replayed source path",
    )
    require(
        results["selected_cases"] == [expected["selected"]],
        "Polygon V4 result has an unexpected selected case",
    )
    selected = [row for row in results["cases"] if row["status"] != "skipped"]
    require(
        len(selected) == 1 and selected[0]["id"] == expected["selected"],
        "Polygon V4 result does not contain one exact selected row",
    )
    require(selected[0]["status"] == "passed", "Polygon V4 selected result did not pass")
    require(
        all(
            row["category"] == "not_selected"
            for row in results["cases"]
            if row["status"] == "skipped"
        ),
        "Polygon V4 result contains a required-case skip",
    )
    require(
        completion == {"identity": expected["identity"], "output_sha256": expected["result"]},
        "Polygon V4 completion identity differs from the reviewed result",
    )
    require(
        digest(results_path) == expected["result"],
        "Polygon V4 result bytes differ from its completion record",
    )


def verify_assertions(source, expected):
    assertions = json.loads((source / "run/qa-assertions.json").read_text())
    require(
        tuple(assertions["actions"]) == expected["actions"]
        and assertions["chains"] == ["eip155:137"]
        and assertions["completed"] == len(expected["actions"])
        and assertions["closed"],
        "Polygon V4 capture does not prove the advertised closed action set",
    )


def verify_capture(style):
    expected = EXPECTED.get(style)
    require(expected is not None, "Polygon V4 fixture style must be trading or lp")
    source = CAPTURE / style
    verify_files(source, reviewed_files(style))
    verify_result(source, style, expected)
    verify_assertions(source, expected)
    return source


def prepared(directory, style="trading"):
    source = verify_capture(style)
    dataset = directory / "dataset"
    dataset.mkdir()
    shutil.copyfile(source / "dataset/data.sqlite", dataset / "data.sqlite")
    shutil.copyfile(source / "dataset/dataset.json", dataset / "dataset.json")
    expected_path = directory / "expected.sqlite"
    shutil.copyfile(source / "run/state.sqlite", expected_path)

    manifest = Manifest.model_validate_json(
        scope_only_json((source / "run/manifest.json").read_text())
    )
    manifest = manifest.model_copy(
        update={
            "dataset": dataset,
            "sdk_digest": sdk_digest(),
            "strategy": StrategyDefinition.read(
                ROOT / f"strategies/uniswap_v4_polygon_{style}/strategy.py"
            ),
        }
    )
    book = PoolBook(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    model = EconomicModel(manifest.config, manifest.settings, manifest.strategy, book)
    return manifest, Journal(expected_path, State).load(), model
