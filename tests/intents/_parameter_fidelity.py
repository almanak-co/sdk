"""I3 — PARAMETER FIDELITY: the ABI-anchored checker.

The invariant, stated once (see ``docs/internal/qa-invariants/I3-parameter-fidelity.md``):

    Every constraint an Intent declares must be present in the calldata the chain
    will actually enforce.

This module decides that question from two sources ONLY, and deliberately from no
others:

1. the **intent contract** — ``almanak/framework/intents/vocabulary.py`` says a
   caller may declare ``max_slippage``, and
   ``almanak/framework/intents/min_out_guard.py`` declares
   ``require_protective_min`` to be "the chokepoint … call this at the ABI-encode
   boundary, on every path"; and
2. the **protocol ABI** — the function signature the emitted calldata selects,
   which is what the chain will actually enforce.

It contains no connector-specific knowledge and no protocol special cases. A
parameter is a *constraint parameter* because its ABI name says so, not because
some connector was known to get it wrong. That is the point: a check written
after reading the implementation can only ever re-assert a defect already found.

Fail-closed by construction
---------------------------
An unrecognised selector is reported as ``UNKNOWN_SELECTOR`` and makes the run
``INCONCLUSIVE``. It never reads as a pass. Silence is a blind spot, and this
module is required to say so out loud.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import function_signature_to_4byte_selector

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONNECTORS = _REPO_ROOT / "almanak" / "connectors"


# ---------------------------------------------------------------------------
# What counts as a constraint parameter — decided from the ABI name alone
# ---------------------------------------------------------------------------

#: Name tokens that mark an ABI parameter as a caller-supplied bound on the
#: outcome of the call. Derived from the shape of the parameter's NAME, never
#: from any connector's use of it.
_MIN_TOKENS = frozenset({"min", "minimum", "mins"})
_MAX_TOKENS = frozenset({"max", "maximum"})
_LIMIT_TOKENS = frozenset({"limit"})
_CONSTRAINT_TOKENS = _MIN_TOKENS | _MAX_TOKENS | _LIMIT_TOKENS

#: Functions that move no value on their own. Excluded from the verdict because
#: a bound is meaningless on them, not because any connector asked for it.
_NON_MONEY_PATH = frozenset(
    {
        "approve",
        "setApprovalForAll",
        "permit",
        "selfPermit",
        "selfPermitAllowed",
        "createAndInitializePoolIfNecessary",
        "refundETH",
    }
)

#: ``multicall`` batches sub-calls into one transaction. The bound the chain
#: enforces lives in a sub-call, so the checker must look through it or it would
#: report a blind ``NO_CONSTRAINT_IN_ABI`` on every V3-shaped position manager.
_MULTICALL_SIGNATURES = (
    "multicall(bytes[])",
    "multicall(uint256,bytes[])",
    "multicall(bytes32,bytes[])",
)

_CAMEL_SPLIT = re.compile(
    r"[^A-Za-z0-9]+"  # snake_case / punctuation boundaries
    r"|(?<=[a-z0-9])(?=[A-Z])"  # camelCase boundary: amountOut -> amount|Out
    r"|(?<=[A-Z])(?=[A-Z][a-z])"  # acronym boundary: amountAMin -> amount|A|Min
    r"|(?<=[A-Za-z])(?=[0-9])"  # amount0 -> amount|0
    r"|(?<=[0-9])(?=[A-Za-z])"  # 0Min -> 0|Min
)


def _name_tokens(name: str) -> frozenset[str]:
    """Split an ABI parameter name into lowercase word tokens."""
    return frozenset(tok.lower() for tok in _CAMEL_SPLIT.split(name) if tok)


class ConstraintKind(Enum):
    """Which direction the bound constrains, read off the parameter name."""

    MIN = "min"
    MAX = "max"
    LIMIT = "limit"


def classify_param(name: str) -> ConstraintKind | None:
    """Return the constraint kind a parameter NAME declares, or ``None``.

    ``MIN`` wins over ``MAX`` when both tokens are present, because a name
    carrying both (``minMaxAmount``) bounds the outcome from below.
    """
    tokens = _name_tokens(name)
    if tokens & _MIN_TOKENS:
        return ConstraintKind.MIN
    if tokens & _MAX_TOKENS:
        return ConstraintKind.MAX
    if tokens & _LIMIT_TOKENS:
        return ConstraintKind.LIMIT
    return None


# ---------------------------------------------------------------------------
# Effectiveness: a bound that cannot bind is not a bound
# ---------------------------------------------------------------------------

_UINT_RE = re.compile(r"^uint(\d+)$")
_INT_RE = re.compile(r"^int(\d+)$")


def _type_max(sol_type: str) -> int | None:
    if match := _UINT_RE.match(sol_type):
        return (1 << int(match.group(1))) - 1
    if match := _INT_RE.match(sol_type):
        return (1 << (int(match.group(1)) - 1)) - 1
    return None


def _is_effective_scalar(kind: ConstraintKind, sol_type: str, value: Any) -> bool:
    """Decide whether one decoded scalar actually binds the chain.

    * a MIN of ``0`` accepts any output — it binds nothing;
    * a MAX equal to the type's maximum is the canonical "no cap" sentinel —
      nominally present, effectively absent;
    * a LIMIT of ``0`` is the canonical "no limit" sentinel (this is what
      ``sqrtPriceLimitX96 == 0`` means to a Uniswap-V3-shaped pool).

    All three rules come from the parameter's TYPE and the universal meaning of
    its sentinel, not from any protocol's convention.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        return False
    if kind is ConstraintKind.MIN:
        return value > 0
    if kind is ConstraintKind.LIMIT:
        return value != 0
    type_max = _type_max(sol_type)
    return value > 0 and (type_max is None or value < type_max)


def _is_effective(kind: ConstraintKind, sol_type: str, value: Any) -> bool:
    """Effectiveness for scalars, arrays and tuples.

    An array or tuple binds if ANY component binds. That is the weakest
    defensible reading of I3 — "a floor derived from the tolerance appears in the
    calldata" — and it is deliberately weak: the intent contract itself supports
    a legitimately single-zero leg on a one-sided LP mint
    (``LPOpenIntent.require_two_sided_minimums`` defaults to false). The
    per-component values are always recorded so a partially-zero vector is
    visible in the report even when the verdict is PROTECTED.
    """
    if sol_type.endswith("]"):
        base = sol_type[: sol_type.rindex("[")]
        return any(_is_effective(kind, base, item) for item in value or ())
    if sol_type.startswith("("):
        return any(_is_effective(kind, "uint256", item) for item in value or ())
    return _is_effective_scalar(kind, sol_type, value)


# ---------------------------------------------------------------------------
# ABI registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AbiFunction:
    name: str
    signature: str
    inputs: tuple[dict[str, Any], ...]
    source: str


def _canonical_type(param: dict[str, Any]) -> str:
    sol_type = param.get("type", "")
    if sol_type.startswith("tuple"):
        inner = ",".join(_canonical_type(c) for c in param.get("components", ()))
        return f"({inner}){sol_type[len('tuple') :]}"
    return sol_type


def _signature(entry: dict[str, Any]) -> str:
    args = ",".join(_canonical_type(p) for p in entry.get("inputs", ()))
    return f"{entry['name']}({args})"


class AbiRegistry:
    """Selector -> ABI function, built from protocol ABI artifacts only."""

    def __init__(self) -> None:
        self._by_selector: dict[bytes, AbiFunction] = {}

    def add_entry(self, entry: dict[str, Any], *, source: str) -> None:
        if entry.get("type") != "function":
            return
        signature = _signature(entry)
        selector = function_signature_to_4byte_selector(signature)
        # First registration wins; identical signatures from several artifacts
        # are the same public function, so a later duplicate is not a conflict.
        self._by_selector.setdefault(
            selector,
            AbiFunction(
                name=entry["name"],
                signature=signature,
                inputs=tuple(entry.get("inputs", ())),
                source=source,
            ),
        )

    def add_abi(self, abi: list[dict[str, Any]], *, source: str) -> None:
        for entry in abi:
            self.add_entry(entry, source=source)

    def lookup(self, selector: bytes) -> AbiFunction | None:
        return self._by_selector.get(selector)

    def __len__(self) -> int:
        return len(self._by_selector)


def _load_repo_abi_artifacts(registry: AbiRegistry) -> None:
    """Load every ABI JSON artifact vendored under ``almanak/connectors``.

    These files are protocol artifacts, not connector logic: they describe what
    the chain enforces.
    """
    for path in sorted(_CONNECTORS.glob("*/abis/*.json")):
        try:
            abi = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):  # pragma: no cover - artifact hygiene
            continue
        if isinstance(abi, dict):
            abi = abi.get("abi", [])
        if isinstance(abi, list):
            registry.add_abi(abi, source=str(path.relative_to(_REPO_ROOT)))


def _tuple(name: str, *components: tuple[str, str]) -> dict[str, Any]:
    return {
        "name": name,
        "type": "tuple",
        "components": [{"name": n, "type": t} for n, t in components],
    }


def _fn(name: str, *inputs: Any) -> dict[str, Any]:
    resolved = [i if isinstance(i, dict) else {"name": i[0], "type": i[1]} for i in inputs]
    return {"type": "function", "name": name, "inputs": resolved}


#: Public router / position-manager entrypoints, transcribed from the protocols'
#: published interfaces. They are here because the connectors that emit them keep
#: their ABIs inline in Python rather than as JSON artifacts; transcribing the
#: public interface keeps this checker independent of connector source.
_AUTHORED_ABI: list[dict[str, Any]] = [
    # --- Uniswap V3 SwapRouter / SwapRouter02 -----------------------------
    _fn(
        "exactInputSingle",
        _tuple(
            "params",
            ("tokenIn", "address"),
            ("tokenOut", "address"),
            ("fee", "uint24"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
            ("sqrtPriceLimitX96", "uint160"),
        ),
    ),
    _fn(
        "exactInputSingle",  # SwapRouter02 — no deadline
        _tuple(
            "params",
            ("tokenIn", "address"),
            ("tokenOut", "address"),
            ("fee", "uint24"),
            ("recipient", "address"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
            ("sqrtPriceLimitX96", "uint160"),
        ),
    ),
    _fn(
        "exactInput",
        _tuple(
            "params",
            ("path", "bytes"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
        ),
    ),
    _fn(
        "exactInput",
        _tuple(
            "params",
            ("path", "bytes"),
            ("recipient", "address"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
        ),
    ),
    _fn(
        "exactOutputSingle",
        _tuple(
            "params",
            ("tokenIn", "address"),
            ("tokenOut", "address"),
            ("fee", "uint24"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountOut", "uint256"),
            ("amountInMaximum", "uint256"),
            ("sqrtPriceLimitX96", "uint160"),
        ),
    ),
    # --- Uniswap V3 NonfungiblePositionManager ----------------------------
    _fn(
        "mint",
        _tuple(
            "params",
            ("token0", "address"),
            ("token1", "address"),
            ("fee", "uint24"),
            ("tickLower", "int24"),
            ("tickUpper", "int24"),
            ("amount0Desired", "uint256"),
            ("amount1Desired", "uint256"),
            ("amount0Min", "uint256"),
            ("amount1Min", "uint256"),
            ("recipient", "address"),
            ("deadline", "uint256"),
        ),
    ),
    _fn(
        "increaseLiquidity",
        _tuple(
            "params",
            ("tokenId", "uint256"),
            ("amount0Desired", "uint256"),
            ("amount1Desired", "uint256"),
            ("amount0Min", "uint256"),
            ("amount1Min", "uint256"),
            ("deadline", "uint256"),
        ),
    ),
    _fn(
        "decreaseLiquidity",
        _tuple(
            "params",
            ("tokenId", "uint256"),
            ("liquidity", "uint128"),
            ("amount0Min", "uint256"),
            ("amount1Min", "uint256"),
            ("deadline", "uint256"),
        ),
    ),
    _fn(
        "collect",
        _tuple(
            "params",
            ("tokenId", "uint256"),
            ("recipient", "address"),
            ("amount0Max", "uint128"),
            ("amount1Max", "uint128"),
        ),
    ),
    _fn("burn", ("tokenId", "uint256")),
    _fn("unwrapWETH9", ("amountMinimum", "uint256"), ("recipient", "address")),
    _fn("sweepToken", ("token", "address"), ("amountMinimum", "uint256"), ("recipient", "address")),
    # --- Aerodrome Slipstream position manager (V3-shaped, tickSpacing) ---
    _fn(
        "mint",
        _tuple(
            "params",
            ("token0", "address"),
            ("token1", "address"),
            ("tickSpacing", "int24"),
            ("tickLower", "int24"),
            ("tickUpper", "int24"),
            ("amount0Desired", "uint256"),
            ("amount1Desired", "uint256"),
            ("amount0Min", "uint256"),
            ("amount1Min", "uint256"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("sqrtPriceX96", "uint160"),
        ),
    ),
    # --- Aerodrome Slipstream SwapRouter (V3-shaped, tickSpacing not fee) --
    _fn(
        "exactInputSingle",
        _tuple(
            "params",
            ("tokenIn", "address"),
            ("tokenOut", "address"),
            ("tickSpacing", "int24"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
            ("sqrtPriceLimitX96", "uint160"),
        ),
    ),
    _fn(
        "exactOutputSingle",
        _tuple(
            "params",
            ("tokenIn", "address"),
            ("tokenOut", "address"),
            ("tickSpacing", "int24"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountOut", "uint256"),
            ("amountInMaximum", "uint256"),
            ("sqrtPriceLimitX96", "uint160"),
        ),
    ),
    _fn(
        "exactInput",
        _tuple(
            "params",
            ("path", "bytes"),
            ("recipient", "address"),
            ("deadline", "uint256"),
            ("amountIn", "uint256"),
            ("amountOutMinimum", "uint256"),
        ),
    ),
    # --- Solidly / Aerodrome classic (v2 constant-product) router ---------
    _fn(
        "addLiquidity",
        ("tokenA", "address"),
        ("tokenB", "address"),
        ("stable", "bool"),
        ("amountADesired", "uint256"),
        ("amountBDesired", "uint256"),
        ("amountAMin", "uint256"),
        ("amountBMin", "uint256"),
        ("to", "address"),
        ("deadline", "uint256"),
    ),
    _fn(
        "removeLiquidity",
        ("tokenA", "address"),
        ("tokenB", "address"),
        ("stable", "bool"),
        ("liquidity", "uint256"),
        ("amountAMin", "uint256"),
        ("amountBMin", "uint256"),
        ("to", "address"),
        ("deadline", "uint256"),
    ),
    _fn(
        "swapExactTokensForTokens",
        ("amountIn", "uint256"),
        ("amountOutMin", "uint256"),
        {
            "name": "routes",
            "type": "tuple[]",
            "components": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "stable", "type": "bool"},
                {"name": "factory", "type": "address"},
            ],
        },
        ("to", "address"),
        ("deadline", "uint256"),
    ),
    _fn(
        "swapExactETHForTokens",
        ("amountOutMin", "uint256"),
        {
            "name": "routes",
            "type": "tuple[]",
            "components": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "stable", "type": "bool"},
                {"name": "factory", "type": "address"},
            ],
        },
        ("to", "address"),
        ("deadline", "uint256"),
    ),
    # --- Curve StableSwap / CryptoSwap pools ------------------------------
    _fn("exchange", ("i", "int128"), ("j", "int128"), ("dx", "uint256"), ("min_dy", "uint256")),
    _fn("exchange", ("i", "uint256"), ("j", "uint256"), ("dx", "uint256"), ("min_dy", "uint256")),
    _fn(
        "exchange",
        ("i", "uint256"),
        ("j", "uint256"),
        ("dx", "uint256"),
        ("min_dy", "uint256"),
        ("use_eth", "bool"),
    ),
    _fn(
        "exchange",
        ("i", "uint256"),
        ("j", "uint256"),
        ("dx", "uint256"),
        ("min_dy", "uint256"),
        ("use_eth", "bool"),
        ("receiver", "address"),
    ),
    _fn(
        "exchange_underlying",
        ("i", "int128"),
        ("j", "int128"),
        ("dx", "uint256"),
        ("min_dy", "uint256"),
    ),
    _fn("add_liquidity", ("amounts", "uint256[2]"), ("min_mint_amount", "uint256")),
    _fn("add_liquidity", ("amounts", "uint256[3]"), ("min_mint_amount", "uint256")),
    _fn("add_liquidity", ("amounts", "uint256[4]"), ("min_mint_amount", "uint256")),
    _fn(
        "add_liquidity",
        ("amounts", "uint256[2]"),
        ("min_mint_amount", "uint256"),
        ("receiver", "address"),
    ),
    _fn(
        "add_liquidity",
        ("amounts", "uint256[3]"),
        ("min_mint_amount", "uint256"),
        ("receiver", "address"),
    ),
    _fn("remove_liquidity", ("amount", "uint256"), ("min_amounts", "uint256[2]")),
    _fn("remove_liquidity", ("amount", "uint256"), ("min_amounts", "uint256[3]")),
    _fn("remove_liquidity", ("amount", "uint256"), ("min_amounts", "uint256[4]")),
    _fn(
        "remove_liquidity",
        ("amount", "uint256"),
        ("min_amounts", "uint256[2]"),
        ("receiver", "address"),
    ),
    _fn(
        "remove_liquidity_one_coin",
        ("token_amount", "uint256"),
        ("i", "int128"),
        ("min_amount", "uint256"),
    ),
    _fn(
        "remove_liquidity_one_coin",
        ("token_amount", "uint256"),
        ("i", "uint256"),
        ("min_amount", "uint256"),
    ),
    _fn(
        "remove_liquidity_imbalance",
        ("amounts", "uint256[2]"),
        ("max_burn_amount", "uint256"),
    ),
    _fn(
        "remove_liquidity_imbalance",
        ("amounts", "uint256[3]"),
        ("max_burn_amount", "uint256"),
    ),
    # --- multicall wrappers ------------------------------------------------
    _fn("multicall", ("data", "bytes[]")),
    _fn("multicall", ("deadline", "uint256"), ("data", "bytes[]")),
    _fn("multicall", ("previousBlockhash", "bytes32"), ("data", "bytes[]")),
    # --- ERC20 / WETH housekeeping (classified as non-money-path) ---------
    _fn("approve", ("spender", "address"), ("amount", "uint256")),
    _fn("transfer", ("to", "address"), ("amount", "uint256")),
    _fn("deposit"),
    _fn("withdraw", ("wad", "uint256")),
    _fn("setApprovalForAll", ("operator", "address"), ("approved", "bool")),
]


def build_registry() -> AbiRegistry:
    registry = AbiRegistry()
    registry.add_abi(
        _AUTHORED_ABI, source="tests/intents/_parameter_fidelity.py (authored from public protocol interfaces)"
    )
    _load_repo_abi_artifacts(registry)
    return registry


_REGISTRY: AbiRegistry | None = None


def registry() -> AbiRegistry:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = build_registry()
    return _REGISTRY


# ---------------------------------------------------------------------------
# Verdicts
# ---------------------------------------------------------------------------


class TxOutcome(Enum):
    PROTECTED = "PROTECTED"
    UNPROTECTED = "UNPROTECTED"
    NO_CONSTRAINT_IN_ABI = "NO_CONSTRAINT_IN_ABI"
    NOT_MONEY_PATH = "NOT_MONEY_PATH"
    UNKNOWN_SELECTOR = "UNKNOWN_SELECTOR"
    UNDECODABLE = "UNDECODABLE"


class RunOutcome(Enum):
    PASS = "PASS"
    VIOLATION = "VIOLATION"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class Constraint:
    path: str
    sol_type: str
    kind: ConstraintKind
    value: Any
    effective: bool

    def __str__(self) -> str:
        return f"{self.path}: {self.sol_type} = {self.value!r} ({'binds' if self.effective else 'DOES NOT BIND'})"


@dataclass
class TxVerdict:
    to: str
    selector: str
    function: str | None
    outcome: TxOutcome
    constraints: tuple[Constraint, ...] = ()
    sub_calls: tuple[TxVerdict, ...] = ()
    note: str = ""

    def describe(self, indent: int = 0) -> str:
        pad = " " * indent
        head = f"{pad}{self.function or self.selector} -> {self.outcome.value}"
        if self.note:
            head += f"  [{self.note}]"
        lines = [head]
        lines.extend(f"{pad}    {c}" for c in self.constraints)
        lines.extend(sub.describe(indent + 4) for sub in self.sub_calls)
        return "\n".join(lines)


@dataclass
class FidelityReport:
    label: str
    declared_slippage: Any
    verdicts: list[TxVerdict] = field(default_factory=list)

    @property
    def outcome(self) -> RunOutcome:
        outcomes = {v.outcome for v in self.verdicts}
        if TxOutcome.UNPROTECTED in outcomes:
            return RunOutcome.VIOLATION
        if TxOutcome.UNKNOWN_SELECTOR in outcomes or TxOutcome.UNDECODABLE in outcomes:
            return RunOutcome.INCONCLUSIVE
        if TxOutcome.PROTECTED in outcomes:
            return RunOutcome.PASS
        return RunOutcome.INCONCLUSIVE

    def describe(self) -> str:
        lines = [
            f"I3 PARAMETER FIDELITY — {self.label}",
            f"  declared max_slippage: {self.declared_slippage}",
            f"  verdict: {self.outcome.value}",
        ]
        lines.extend(v.describe(indent=2) for v in self.verdicts)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------


def _collect_constraints(
    inputs: tuple[dict[str, Any], ...], values: tuple[Any, ...], prefix: str = ""
) -> list[Constraint]:
    found: list[Constraint] = []
    for param, value in zip(inputs, values, strict=False):
        name = param.get("name") or "<unnamed>"
        path = f"{prefix}{name}"
        sol_type = _canonical_type(param)
        if sol_type.startswith("(") and param.get("components"):
            # Recurse into a struct so a bound nested in a params tuple is seen.
            found.extend(_collect_constraints(tuple(param["components"]), tuple(value), prefix=f"{path}."))
            continue
        kind = classify_param(name)
        if kind is None:
            continue
        found.append(
            Constraint(
                path=path,
                sol_type=sol_type,
                kind=kind,
                value=value,
                effective=_is_effective(kind, sol_type, value),
            )
        )
    return found


def check_calldata(to: str, data: str | bytes, *, reg: AbiRegistry | None = None) -> TxVerdict:
    """Decode one transaction's calldata and rule on its parameter fidelity."""
    reg = reg or registry()
    raw = (
        bytes.fromhex(data[2:])
        if isinstance(data, str) and data.startswith("0x")
        else (bytes.fromhex(data) if isinstance(data, str) else bytes(data))
    )
    if len(raw) < 4:
        return TxVerdict(
            to=to, selector="0x", function=None, outcome=TxOutcome.UNDECODABLE, note="calldata shorter than a selector"
        )
    selector, body = raw[:4], raw[4:]
    fn = reg.lookup(selector)
    if fn is None:
        return TxVerdict(
            to=to,
            selector="0x" + selector.hex(),
            function=None,
            outcome=TxOutcome.UNKNOWN_SELECTOR,
            note="selector not in the ABI registry — a blind spot, never a pass",
        )

    types = [_canonical_type(p) for p in fn.inputs]
    try:
        values = abi_decode(types, body) if types else ()
    except Exception as exc:  # noqa: BLE001 - any decode failure is a blind spot
        return TxVerdict(
            to=to,
            selector="0x" + selector.hex(),
            function=fn.signature,
            outcome=TxOutcome.UNDECODABLE,
            note=str(exc)[:120],
        )

    if fn.signature in _MULTICALL_SIGNATURES:
        payloads = next((v for t, v in zip(types, values, strict=False) if t == "bytes[]"), ())
        subs = tuple(check_calldata(to, bytes(p), reg=reg) for p in payloads)
        sub_outcomes = {s.outcome for s in subs}
        if TxOutcome.PROTECTED in sub_outcomes:
            outcome = TxOutcome.PROTECTED
        elif TxOutcome.UNPROTECTED in sub_outcomes:
            outcome = TxOutcome.UNPROTECTED
        elif TxOutcome.UNKNOWN_SELECTOR in sub_outcomes or TxOutcome.UNDECODABLE in sub_outcomes:
            outcome = TxOutcome.UNKNOWN_SELECTOR
        elif sub_outcomes <= {TxOutcome.NOT_MONEY_PATH}:
            outcome = TxOutcome.NOT_MONEY_PATH
        else:
            outcome = TxOutcome.NO_CONSTRAINT_IN_ABI
        return TxVerdict(
            to=to,
            selector="0x" + selector.hex(),
            function=fn.signature,
            outcome=outcome,
            sub_calls=subs,
            note="aggregated over multicall sub-calls",
        )

    if fn.name in _NON_MONEY_PATH:
        return TxVerdict(to=to, selector="0x" + selector.hex(), function=fn.signature, outcome=TxOutcome.NOT_MONEY_PATH)

    constraints = tuple(_collect_constraints(fn.inputs, tuple(values)))
    if not constraints:
        return TxVerdict(
            to=to,
            selector="0x" + selector.hex(),
            function=fn.signature,
            outcome=TxOutcome.NO_CONSTRAINT_IN_ABI,
            note="the ABI of the selected entrypoint declares no caller-supplied bound",
        )
    outcome = TxOutcome.PROTECTED if any(c.effective for c in constraints) else TxOutcome.UNPROTECTED
    return TxVerdict(
        to=to,
        selector="0x" + selector.hex(),
        function=fn.signature,
        outcome=outcome,
        constraints=constraints,
    )


def check_transactions(
    transactions: Any,
    *,
    label: str,
    declared_slippage: Any,
    reg: AbiRegistry | None = None,
) -> FidelityReport:
    """Rule on every transaction a compiled intent emitted."""
    report = FidelityReport(label=label, declared_slippage=declared_slippage)
    for tx in transactions:
        to = getattr(tx, "to", None) or (tx.get("to") if isinstance(tx, dict) else "")
        data = getattr(tx, "data", None) or (tx.get("data") if isinstance(tx, dict) else "")
        report.verdicts.append(check_calldata(to, data, reg=reg))
    return report


def assert_parameter_fidelity(report: FidelityReport) -> None:
    """Fail the test unless the compiled calldata carries a binding constraint."""
    if report.outcome is not RunOutcome.PASS:
        raise AssertionError(
            "I3 PARAMETER FIDELITY violated or unproven.\n"
            f"{report.describe()}\n"
            "The caller declared a slippage tolerance; the chain will not enforce one."
        )


def assert_known_violation(report: FidelityReport, *, ticket: str) -> None:
    """Assert a connector STILL carries a known, ticketed fidelity defect.

    This exists instead of ``@pytest.mark.xfail(strict=True)`` on the test
    function. An xfail mark covers the whole body, so any exception satisfies it
    -- a failed on-chain setup, a fixture error, an unrelated regression. The
    test then reports ``xfailed`` and reads as "the defect was demonstrated"
    when nothing of the sort was demonstrated. A ``pytest.skip`` inside the body
    is worse still: it reports ``skipped`` and proves nothing in either
    direction.

    Asserting the violation positively fixes all three:

    * the defect is still present  -> PASS, and the test names what it verified;
    * a floor lands (outcome PASS) -> FAIL, which forces this call site out in
      the same change that fixes the connector -- the property the strict xfail
      was there to provide;
    * INCONCLUSIVE                 -> FAIL, because "could not measure" is not
      evidence the defect persists (``Empty != Zero``);
    * a broken setup               -> ERROR, loudly, instead of being swallowed.
    """
    if report.outcome is RunOutcome.PASS:
        raise AssertionError(
            f"{ticket} appears to be FIXED: the compiled calldata now carries a binding constraint.\n"
            f"{report.describe()}\n"
            "Remove this assert_known_violation call and restore assert_parameter_fidelity."
        )
    if report.outcome is not RunOutcome.VIOLATION:
        raise AssertionError(
            f"{ticket} is UNPROVEN, not confirmed: fidelity was {report.outcome.value}, not VIOLATION.\n"
            f"{report.describe()}\n"
            "An unmeasured run is not evidence that the defect persists."
        )


# ---------------------------------------------------------------------------
# Mutation control support
# ---------------------------------------------------------------------------


def _zero_value(sol_type: str, value: Any) -> Any:
    """Return ``value`` with every scalar component replaced by its zero."""
    if sol_type.endswith("]"):
        base = sol_type[: sol_type.rindex("[")]
        return type(value)(_zero_value(base, item) for item in value) if value else value
    if isinstance(value, tuple):
        return tuple(0 for _ in value)
    return 0


def zero_constraints(data: str | bytes, *, reg: AbiRegistry | None = None) -> str:
    """Re-encode ``data`` with every constraint parameter forced to zero.

    This is the mutation operator behind the live-calldata mutation control: it
    takes calldata a connector really produced and removes exactly the property
    I3 asserts, changing nothing else. A checker that still reports ``PASS`` on
    the result has never been able to detect anything.

    Unrecognised or undecodable calldata is returned unchanged, so a mutation
    control built on it fails to flip and is visible as such rather than
    silently passing.
    """
    reg = reg or registry()
    raw = (
        bytes.fromhex(data[2:])
        if isinstance(data, str) and data.startswith("0x")
        else (bytes.fromhex(data) if isinstance(data, str) else bytes(data))
    )
    if len(raw) < 4:
        return "0x" + raw.hex()
    selector, body = raw[:4], raw[4:]
    fn = reg.lookup(selector)
    if fn is None:
        return "0x" + raw.hex()
    types = [_canonical_type(p) for p in fn.inputs]
    try:
        values = list(abi_decode(types, body)) if types else []
    except Exception:  # noqa: BLE001
        return "0x" + raw.hex()

    if fn.signature in _MULTICALL_SIGNATURES:
        mutated = [
            [bytes.fromhex(zero_constraints(bytes(p), reg=reg)[2:]) for p in value] if sol_type == "bytes[]" else value
            for sol_type, value in zip(types, values, strict=False)
        ]
        return "0x" + (selector + abi_encode(types, mutated)).hex()

    if fn.name in _NON_MONEY_PATH:
        return "0x" + raw.hex()

    def _mutate(params: tuple[dict[str, Any], ...], vals: Any) -> Any:
        out = []
        for param, value in zip(params, vals, strict=False):
            sol_type = _canonical_type(param)
            if sol_type.startswith("(") and param.get("components"):
                out.append(_mutate(tuple(param["components"]), value))
            elif classify_param(param.get("name") or "") is not None:
                out.append(_zero_value(sol_type, value))
            else:
                out.append(value)
        return tuple(out)

    mutated_values = list(_mutate(fn.inputs, values))
    return "0x" + (selector + abi_encode(types, mutated_values)).hex()
