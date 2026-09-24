"""A transient quote is retried; a bad quote is not, and prices precede funding.

`ax_price` fails closed by design. Resolving it AFTER the funding leg therefore
turned one unavailable quote into live funds stranded in a pool wallet, so the
runner must resolve the whole price set before any value moves.
"""

import ast
import json
import subprocess
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

import qa_lab.chains as C

RUNNER = Path(__file__).resolve().parents[3] / "qa_lab" / "run_mainnet_intent.py"


def _ok(price="123.45"):
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout=json.dumps({"status": "success", "data": {"price": price}}), stderr=""
    )


def _garbage():
    return subprocess.CompletedProcess(args=[], returncode=1, stdout="boom, no json here", stderr="")


def _error_doc():
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout=json.dumps({"status": "error", "error": "upstream 503"}), stderr=""
    )


@pytest.fixture(autouse=True)
def _no_sleep():
    with patch.object(C.time, "sleep"):
        yield


@pytest.mark.parametrize("transient", [_garbage, _error_doc])
def test_a_transient_failure_is_retried_and_can_succeed(transient):
    with patch.object(C.subprocess, "run", side_effect=[transient(), transient(), _ok()]) as run:
        price, _doc = C._measure_price("ETH", "WETH", "arbitrum")

    assert price == Decimal("123.45")
    assert run.call_count == 3


def _not_an_object(payload):
    return subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(payload), stderr="")


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "success", "data": ["price", "1"]},
        {"status": "success", "data": "1.23"},
    ],
)
def test_a_data_field_that_is_not_an_object_is_retried_not_raised_through(payload):
    """A malformed print is transport-shaped, so it belongs to the retry.

    Reading price fields off a non-mapping raises AttributeError, which is not
    in the handler's except tuple -- it would escape both the retry and the
    caller's fail-closed refusal, leaving the lane with neither a price nor the
    message that says why.
    """
    with patch.object(C.subprocess, "run", side_effect=[_not_an_object(payload), _ok("9")]) as run:
        price, _doc = C._measure_price("ETH", "WETH", "arbitrum")

    assert price == Decimal("9")
    assert run.call_count == 2


def test_a_timeout_is_retried():
    timeout = subprocess.TimeoutExpired(cmd="ax price", timeout=240)
    with patch.object(C.subprocess, "run", side_effect=[timeout, _ok("7")]) as run:
        price, _doc = C._measure_price("ETH", "WETH", "arbitrum")

    assert price == Decimal("7")
    assert run.call_count == 2


def test_retries_are_bounded_and_still_fail_closed():
    with patch.object(C.subprocess, "run", side_effect=[_garbage()] * 10) as run:
        with pytest.raises(SystemExit) as excinfo:
            C._measure_price("ETH", "WETH", "arbitrum")

    assert run.call_count == C._PRICE_ATTEMPTS
    assert "CANNOT proceed on an unpriced asset" in str(excinfo.value)


@pytest.mark.parametrize("bad", ["unavailable", "0.0", "0.00", "-1", "NaN", "Infinity", 0, 0.0])
def test_an_unusable_print_is_retried_and_not_returned(bad):
    """A print that is not a positive finite price is not a quote."""
    with patch.object(C.subprocess, "run", side_effect=[_ok(bad), _ok()]) as run:
        price, _doc = C._measure_price("ETH", "WETH", "arbitrum")

    assert price == Decimal("123.45")
    assert run.call_count == 2


def test_a_zero_print_never_becomes_a_price():
    with patch.object(C.subprocess, "run", side_effect=[_ok("0.0")] * 5) as run:
        with pytest.raises(SystemExit, match="unpriced asset"):
            C._measure_price("ETH", "WETH", "arbitrum")

    assert run.call_count == C._PRICE_ATTEMPTS


def _pegged(price: str, source: str):
    return subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps({"status": "success", "data": {"price": price, "source": source}}),
        stderr="",
    )


@pytest.mark.parametrize(
    ("price", "source", "match"),
    [
        ("0.93", "aggregated", "Refusing the cap gate"),
        ("1.0", "stablecoin_peg", "not a market observation"),
    ],
)
def test_a_peg_refusal_is_never_retried(price, source, match, monkeypatch):
    """Retrying a delivered quote would let a peg refusal be rerolled into a pass."""
    monkeypatch.setattr(C, "active_context", lambda: None)
    later_good_quote = _pegged("1.0005", "aggregated")
    with patch.object(C.subprocess, "run", side_effect=[_pegged(price, source), later_good_quote]) as run:
        with pytest.raises(SystemExit, match=match):
            C.ax_price("USDG", "robinhood")

    assert run.call_count == 1


def _assign_line(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return node.lineno
    raise AssertionError(f"no assignment to {name} found")


def test_the_price_set_is_resolved_before_the_funding_leg():
    """The ordering IS the fix: after funding, a failed quote strands live funds."""
    tree = ast.parse(RUNNER.read_text())

    fund_calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_fund"
    ]
    assert fund_calls, "no _fund call site found in the mainnet runner"

    assert _assign_line(tree, "price_oracle") < min(fund_calls), (
        "price_oracle is resolved after _fund: an unavailable quote now strands funds already sent to the pool wallet"
    )


def test_action_symbols_reads_no_chain_or_wallet_state():
    """It must be recipe-only, or it could not be resolved before funding."""
    import qa_lab.run_mainnet_intent as M

    source = ast.parse(RUNNER.read_text())
    func = next(
        node for node in ast.walk(source) if isinstance(node, ast.FunctionDef) and node.name == "_action_symbols"
    )
    args = {a.arg for a in func.args.args} | {a.arg for a in func.args.kwonlyargs}

    assert args == {"recipe"}, f"_action_symbols takes {args}; it must depend on the recipe alone"
    assert callable(M._action_symbols)
