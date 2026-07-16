"""Static guard: no default-arg tick wrappers, no hand-rolled tick math in demos.

ALM-2891 and ALM-2901 are the same arithmetic defect reached by two different
routes, and BOTH routes were opened by demo/strategy code rather than by the SDK
core:

1. **Omitting the decimals term** -- ``tick = log(price) / log(1.0001)``, which
   is arithmetically identical to ``decimals0 == decimals1 == 18``.
2. **A default-arg wrapper** -- ``def _price_to_tick(price, decimals0=18,
   decimals1=18)``, which silently SATISFIES the SDK's mandatory-decimals guard
   (#3108) while feeding it the wrong answer.

Both produce tick -110798 for the ALM-2901 price where the true tick is -64744:
an error of |d0-d1| * log_1.0001(10) = 46,054 ticks, ~100x in price, which mints
a one-sided position.

Demo code is copied verbatim into user strategies, so a footgun in a demo is a
footgun in production. These are AST checks, not greps: they read the actual
function signatures and call sites.

Deliberately scoped to the LP demo catalog. Connector-internal math and the
SDK's own helpers are out of scope -- the SDK is *supposed* to own this math;
the point is that strategies must not re-implement it.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEMO_CATALOG = _REPO_ROOT / "strategies" / "internal" / "demo_catalog"

# The V3 tick base, and any logarithm call. Matched together on one code line so
# every spelling of the hand-rolled conversion is caught -- log(x)/log(1.0001),
# log(x, 1.0001), log(float(x), 1.0001), log1p(pct)/log(1.0001).
_TICK_BASE = re.compile(r"1\.0001")
_LOG_CALL = re.compile(r"\blog1?p?\s*\(")

# Parameter names that carry the decimals term.
_DECIMALS_PARAMS = {"decimals0", "decimals1", "token0_decimals", "token1_decimals"}
# Function names that convert between prices and ticks.
_TICK_CONVERTERS = {"price_to_tick", "tick_to_price", "_price_to_tick", "_tick_to_price"}


def _demo_python_files() -> list[Path]:
    if not _DEMO_CATALOG.is_dir():  # pragma: no cover - layout guard
        pytest.skip(f"demo catalog not found at {_DEMO_CATALOG}")
    return sorted(p for p in _DEMO_CATALOG.rglob("*.py") if "dashboard" not in p.parts)


def _iter_functions(path: Path):
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            yield node


def test_no_demo_defines_a_tick_converter_with_defaulted_decimals():
    """A defaulted decimals param is the ALM-2891 vector. Ban it outright."""
    offenders: list[str] = []
    for path in _demo_python_files():
        for fn in _iter_functions(path):
            if fn.name not in _TICK_CONVERTERS:
                continue
            args = fn.args
            # Line up positional params with their defaults (defaults bind to the tail).
            positional = args.posonlyargs + args.args
            defaulted = {
                a.arg for a in positional[len(positional) - len(args.defaults) :] if args.defaults
            }
            defaulted |= {
                kw.arg for kw, d in zip(args.kwonlyargs, args.kw_defaults, strict=True) if d is not None
            }
            bad = sorted(defaulted & _DECIMALS_PARAMS)
            if bad:
                offenders.append(f"{path.relative_to(_REPO_ROOT)}:{fn.lineno} {fn.name}() defaults {bad}")
    assert not offenders, (
        "Tick converters in demo strategies must not default their decimals params — a default "
        "silently satisfies the SDK's mandatory-decimals guard while producing the ALM-2901 "
        "46,054-tick error (USDC(6)/cbBTC(8)). Pass decimals from token metadata, or better, "
        "hand the connector a price band and let the SDK convert.\n  " + "\n  ".join(offenders)
    )


def test_no_demo_hand_rolls_the_log_price_over_log_1_0001_formula():
    """The literal ALM-2901 formula. If a demo needs this, the connector is missing a price path."""
    offenders: list[str] = []
    for path in _demo_python_files():
        source = path.read_text()
        for lineno, line in enumerate(source.splitlines(), start=1):
            code = line.split("#", 1)[0]
            # Any log against the 1.0001 tick base, however it is spelled:
            #   log(x) / log(1.0001)   log(x, 1.0001)   log(float(p), 1.0001)
            #   log1p(pct) / log(1.0001)
            if _TICK_BASE.search(code) and _LOG_CALL.search(code):
                offenders.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: {line.strip()}")
    assert not offenders, (
        "Hand-rolled price->tick math found in a demo strategy. This formula omits the decimals "
        "term unless it is threaded through explicitly, which is exactly how ALM-2901 happened. "
        "Use a price band (Intent.lp_open(range_lower=<price>, ...)) — every concentrated-liquidity "
        "connector, including aerodrome_slipstream, converts it correctly.\n  " + "\n  ".join(offenders)
    )


def test_slipstream_demo_states_its_range_as_prices_not_ticks():
    """The proof that VIB-5867 landed: the demo got simpler.

    The Slipstream demo used to carry ~60 lines of tick math purely because the
    connector would not take a price band. If this test fails, either the demo
    regressed to hand-rolled ticks or the connector's price path was removed.
    """
    demo = _DEMO_CATALOG / "aerodrome_slipstream_lp" / "strategy.py"
    if not demo.is_file():  # pragma: no cover - matches _demo_python_files() skip
        pytest.skip(f"slipstream demo not found at {demo}")
    tree = ast.parse(demo.read_text(), filename=str(demo))
    defined = {fn.name for fn in _iter_functions(demo)}

    assert "price_to_tick" not in defined, "slipstream demo re-defined price_to_tick"
    assert "snap_to_tick_spacing" not in defined, "slipstream demo re-defined tick snapping"
    assert "_compute_tick_range" not in defined, "slipstream demo re-introduced tick computation"
    assert "_compute_price_band" in defined, "slipstream demo lost its price-band range"

    # The LP_OPEN must be built from a price band, not from tick variables.
    range_args: dict[str, str] = {}
    found_lp_open = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "lp_open":
            found_lp_open = True
            for kw in node.keywords:
                if kw.arg in ("range_lower", "range_upper", "range_spec"):
                    range_args[kw.arg] = ast.unparse(kw.value)
    assert found_lp_open, "no Intent.lp_open(...) call found in the slipstream demo"
    assert range_args, "slipstream demo's Intent.lp_open(...) declares no range at all"

    # A bare whole-number pair is ambiguous on a tick-based protocol and is
    # rejected at construction, so the demo must state the form explicitly.
    assert "range_spec" in range_args, (
        "slipstream demo must pass an explicit range_spec=PriceBand(...): a bare "
        "range_lower/range_upper pair is ambiguous on a tick-based protocol whenever the "
        "prices happen to be whole numbers."
    )
    assert "PriceBand" in range_args["range_spec"], (
        f"slipstream demo must open with a PriceBand, got {range_args['range_spec']!r}"
    )
    for arg, expr in range_args.items():
        assert "tick" not in expr.lower(), (
            f"slipstream demo passes a tick-derived value to {arg}: {expr!r}. "
            "The demo must state a PRICE band and let the connector convert."
        )
