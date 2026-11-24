"""Explicit proof mappings checked against authoritative connector declarations."""

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from almanak.caip import ChainId
from almanak.configuration.unresolved import public_root, read_inputs
from almanak.configuration.yaml import load
from almanak.connectors.composition import installed_connectors
from almanak.connectors.curve.economic_data import CRYPTO_MODEL
from almanak.connectors.curve.economic_data import MODEL as CURVE_STABLE_MODEL
from almanak.connectors.gmx_v2.economic_data import LIQUIDITY_MODEL as GMX_LIQUIDITY_MODEL
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import EconomicAction, RunMode
from tests.strategy_fork.harness import discover

ROOT = Path(__file__).resolve().parents[2]
Profile = Literal["safe_roles", "eoa"]
AAVE = ("supply", "withdraw", "borrow", "repay")
AAVE_V3 = (*AAVE, "withdraw_amount", "repay_amount", "collateral")
LENDING = (*AAVE, "withdraw_amount", "repay_amount")
SWAP = ("swap_exact_input", "revoke_exchange", "exit_exchange")
LP = (*SWAP, "open_liquidity", "collect_liquidity_fees", "close_liquidity")
V3_SWAP = ("swap_exact_input", "exit_exchange")
V3_LP = (*LP, "increase_liquidity", "decrease_liquidity")
BAND_LP = ("swap_exact_input", "exit_exchange", "open_liquidity", "close_liquidity")
BRIDGE = ("bridge", "revoke_bridge")
GMX_POSITION = ("increase_position", "close_position")
HYPERCORE = ("deposit", "set_leverage", "increase_position", "close_position", "withdraw")
GMX_PATH = (*GMX_POSITION, "decrease_position")
GMX_LIQUIDITY = (
    "open_liquidity",
    "increase_liquidity",
    "decrease_liquidity",
    "close_liquidity",
    "revoke_exchange",
)
CURVE_LP = (
    "swap_exact_input",
    "exit_exchange",
    "open_liquidity",
    "increase_liquidity",
    "decrease_liquidity",
    "close_liquidity",
)
CURVE_VARIANTS = {
    "curve_robinhood_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_robinhood_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_robinhood_tricrypto_lp": ProtocolVariant("tricrypto_ng_2024"),
    "curve_optimism_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_optimism_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_optimism_tricrypto_lp": ProtocolVariant("tricrypto_ng_2024"),
    "curve_bnb_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_bnb_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_polygon_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_polygon_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_polygon_tricrypto_lp": ProtocolVariant("tricrypto_ng_2024"),
    "curve_avalanche_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_ethereum_stable_trading": ProtocolVariant("stableswap_ng"),
    "curve_ethereum_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_ethereum_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_ethereum_tricrypto_lp": ProtocolVariant("tricrypto_ng"),
    "curve_arbitrum_tricrypto_lp": ProtocolVariant("tricrypto_ng"),
    "curve_arbitrum_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_arbitrum_stable_lp": ProtocolVariant("stableswap_ng"),
    "curve_base_tricrypto_lp": ProtocolVariant("tricrypto_ng"),
    "curve_base_twocrypto_lp": ProtocolVariant("twocrypto_ng"),
    "curve_base_stable_lp": ProtocolVariant("stableswap_ng"),
}
AERODROME_VARIANTS = {
    f"aerodrome_base_{invariant}_{style}": ProtocolVariant(invariant)
    for invariant in ("stable", "volatile")
    for style in ("trading", "lp")
}
MODELS = {
    (ConnectorId("across"), "economic"): "across-full-fee-ceiling-fixed-latency-v1",
    (ConnectorId("aerodrome"), "economic"): "aerodrome-economic-integer-fixed-indices-v1",
    (
        ConnectorId("aerodrome_slipstream"),
        "economic",
    ): "slipstream-economic-integer-fixed-growth-v1",
    (ConnectorId("morpho_blue"), "economic"): "morpho-blue-archive-anchored-market-v1",
    (ConnectorId("compound_v3"), "economic"): "compound-v3-exogenous-integer-indices-v1",
    (ConnectorId("pancakeswap_v3"), "economic"): "pancakeswap-v3-economic-integer-fixed-growth-v1",
    (ConnectorId("sparklend"), "economic"): "sparklend-revision4-economic-indices-v1",
    (ConnectorId("uniswap_v3"), "economic"): "uniswap-v3-economic-integer-fixed-growth-v1",
    (ConnectorId("aave_v3"), "economic"): "aave-v3-revision11-economic-indices-v1",
    (ConnectorId("aave_v4"), "economic"): "aave-v4-economic-rates-v1",
    (ConnectorId("aave_v4"), "historical_path"): "aave-v4-supply-historical-shares-v1",
    (ConnectorId("uniswap_v4"), "economic"): "uniswap-v4-economic-integer-fixed-growth-v1",
    (ConnectorId("uniswap_v4"), "historical_path"): "uniswap-v4-exact-quoter-fixed-path-lp-v1",
}
GMX_REFERENCE_MODELS = {"gmx_v2_arbitrum_liquidity": GMX_LIQUIDITY_MODEL}
# These are exercised actions, not a copy of connector support. New declarations
# must find a proof here instead of automatically counting themselves as covered.
REFERENCES = {
    **{
        f"gmx_v2_{chain}_{path}": (
            actions,
            ("economic",) if (chain, path) == ("arbitrum", "liquidity") else (),
        )
        for chain in ("arbitrum", "avalanche")
        for path, actions in (
            ("position", GMX_POSITION),
            ("position_path", GMX_PATH),
            ("collateral_path", GMX_PATH),
            ("swap", ("swap_exact_input", "revoke_exchange")),
            ("liquidity", GMX_LIQUIDITY),
        )
    },
    **{
        name: (V3_LP if name.endswith("_lp") else V3_SWAP, ("economic",))
        for name in AERODROME_VARIANTS
    },
    "gmx_v2_arbitrum_price_stop": (GMX_POSITION, ()),
    "aerodrome_slipstream_base_lp": (V3_LP, ("economic",)),
    "aerodrome_slipstream_base_lp_band": (BAND_LP, ()),
    "aerodrome_slipstream_base_trading": (V3_SWAP, ("economic",)),
    "compound_v3_optimism_lending": (LENDING, ("economic",)),
    "morpho_blue_optimism_lending": (LENDING, ("economic",)),
    "morpho_blue_polygon_lending": (LENDING, ("economic",)),
    "morpho_blue_robinhood_lending": (LENDING, ("economic",)),
    "morpho_blue_hyperevm_lending": (LENDING, ("economic",)),
    # Its backtest is proven offline over a real preparation (`hypercore_supplemental_cases`).
    "hyperliquid_hyperevm_perp": (HYPERCORE, ()),
    "morpho_blue_bnb_lending": (LENDING, ("economic",)),
    "morpho_blue_base_lending": (LENDING, ("economic",)),
    "morpho_blue_arbitrum_lending": (LENDING, ("economic",)),
    "morpho_blue_ethereum_lending": (LENDING, ("economic",)),
    "morpho_blue_avalanche_lending": (LENDING, ("economic",)),
    "pancakeswap_v3_arbitrum_lp": (V3_LP, ("economic",)),
    "pancakeswap_v3_arbitrum_trading": (V3_SWAP, ("economic",)),
    "pancakeswap_v3_base_lp": (V3_LP, ("economic",)),
    "pancakeswap_v3_base_trading": (V3_SWAP, ("economic",)),
    "pancakeswap_v3_ethereum_lp": (V3_LP, ("economic",)),
    "pancakeswap_v3_ethereum_trading": (V3_SWAP, ("economic",)),
    "compound_v3_ethereum_lending": (LENDING, ("economic",)),
    "compound_v3_base_lending": (LENDING, ("economic",)),
    "compound_v3_arbitrum_lending": (LENDING, ("economic",)),
    "pancakeswap_v3_bnb_trading": (V3_SWAP, ("economic",)),
    "pancakeswap_v3_bnb_lp": (V3_LP, ("economic",)),
    "pancakeswap_v3_bnb_lp_band": (BAND_LP, ()),
    "sparklend_ethereum_lending": (AAVE_V3, ("economic",)),
    "uniswap_v3_ethereum_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_ethereum_lp": (V3_LP, ("economic",)),
    # Not in the frozen QA economic matrix; their price-series backtests are verified directly.
    "uniswap_v3_base_rsi": (V3_SWAP, ()),
    "uniswap_v3_base_atr_breakout": (V3_SWAP, ()),
    "uniswap_v3_base_rsi_lp": (BAND_LP, ()),
    "uniswap_v3_base_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_base_lp": (V3_LP, ("economic",)),
    "uniswap_v3_arbitrum_trading": (("swap_exact_input", "exit_exchange"), ("economic",)),
    "uniswap_v3_arbitrum_lp": (V3_LP, ("economic",)),
    "uniswap_v3_optimism_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_optimism_lp": (V3_LP, ("economic",)),
    "uniswap_v3_avalanche_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_avalanche_lp": (V3_LP, ("economic",)),
    "uniswap_v3_bnb_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_bnb_lp": (V3_LP, ("economic",)),
    "uniswap_v3_robinhood_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_robinhood_lp": (V3_LP, ("economic",)),
    "uniswap_v3_robinhood_lp_band": (BAND_LP, ()),
    "uniswap_v3_polygon_trading": (V3_SWAP, ("economic",)),
    "uniswap_v3_polygon_lp": (V3_LP, ("economic",)),
    "aave_v3_ethereum_lending": (AAVE_V3, ("economic",)),
    "aave_v3_base_lending": (AAVE_V3, ("economic",)),
    # Not in the frozen QA economic matrix; its price-series backtest is verified directly.
    "aave_v3_base_sma_borrow": (AAVE, ()),
    "aave_v3_arbitrum_lending": (AAVE_V3, ("economic",)),
    "aave_v3_arbitrum_managed_borrow": (LENDING, ()),
    "aave_v3_optimism_lending": (AAVE_V3, ("economic",)),
    "aave_v3_bnb_lending": (AAVE_V3, ("economic",)),
    "aave_v3_polygon_lending": (AAVE_V3, ("economic",)),
    "aave_v3_avalanche_lending": (AAVE_V3, ("economic",)),
    "curve_ethereum_stable_trading": (("swap_exact_input", "exit_exchange"), ("economic",)),
    **{name: (CURVE_LP, ("economic",)) for name in CURVE_VARIANTS if name.endswith("_lp")},
    "aave_v4_weth_supply": (AAVE, ("economic", "historical_path")),
    "aave_v4_avalanche_borrowing": (AAVE, ("economic",)),
    "aave_v4_multichain_borrowing": (AAVE, ("economic",)),
    "uniswap_v4_trading": (SWAP, ("economic", "historical_path")),
    "uniswap_v4_lp": (LP, ("economic", "historical_path")),
    "uniswap_v4_arbitrum_lp": (LP, ("economic", "historical_path")),
    "uniswap_v4_arbitrum_trading": (SWAP, ("economic", "historical_path")),
    "uniswap_v4_optimism_lp": (LP, ("economic", "historical_path")),
    "uniswap_v4_optimism_trading": (SWAP, ("economic", "historical_path")),
    "uniswap_v4_base_lp": (LP, ("economic", "historical_path")),
    "uniswap_v4_base_trading": (SWAP, ("economic", "historical_path")),
    "uniswap_v4_avalanche_trading": (SWAP, ("economic", "historical_path")),
    "uniswap_v4_avalanche_lp": (LP, ("economic", "historical_path")),
    "uniswap_v4_bnb_trading": (SWAP, ("economic",)),
    "uniswap_v4_bnb_lp": (LP, ("economic",)),
    "uniswap_v4_polygon_trading": (SWAP, ("economic",)),
    "uniswap_v4_polygon_lp": (LP, ("economic",)),
    "uniswap_v4_robinhood_trading": (SWAP, ("economic",)),
    "uniswap_v4_ethereum_native_trading": (SWAP, ("economic",)),
    "uniswap_v4_robinhood_lp": (LP, ("economic",)),
    "uniswap_v4_robinhood_lp_band": (BAND_LP, ()),
    "across_usdc_arbitrum_base_bridge": (BRIDGE, ("economic",)),
}
# Candidate references remain in the bench denominator. They cannot satisfy a
# declared obligation or turn a full run green before promotion and proof mapping.
CANDIDATES: dict[str, tuple[str | None, tuple[str, ...]]] = {
    "compound_v3_polygon_lending": ("compound_v3", ("eip155:137",)),
    "arbitrum_aave_borrow_and_trade_gmx": (None, ("eip155:42161",)),
    "avalanche_aave_borrow_and_trade_gmx": (None, ("eip155:43114",)),
}


@dataclass(frozen=True, order=True)
class Obligation:
    connector: ConnectorId
    chain: ChainId
    mode: str
    profile: str
    model: str
    action: EconomicAction
    variant: ProtocolVariant | None = None


@dataclass(frozen=True)
class ProofScope:
    connector: ConnectorId
    chains: tuple[ChainId, ...]
    actions: tuple[EconomicAction, ...]
    model: str = ""
    variants: tuple[ProtocolVariant, ...] = ()


@dataclass(frozen=True)
class Case:
    id: str
    lane: str
    reference: str = ""
    connector: ConnectorId | None = None
    chains: tuple[ChainId, ...] = ()
    actions: tuple[EconomicAction, ...] = ()
    profile: str = ""
    model: str = ""
    model_mode: Literal["", "economic", "historical_path"] = ""
    backtest_file: str = ""
    mode: str = ""
    input_policy: str = "pinned"
    operator_mode: str = "configured"
    nodes: tuple[str, ...] = ()
    blocked_reason: str = ""
    variants: tuple[ProtocolVariant, ...] = ()
    scenario: Literal["", "curve_mixed", "curve_cleanup", "gmx_cleanup", "uniswap_cleanup"] = ""
    scopes: tuple[ProofScope, ...] = ()

    def checks(self):
        if not self.scopes:
            return (self,)
        return tuple(
            replace(
                self,
                connector=scope.connector,
                chains=scope.chains,
                actions=scope.actions,
                model=scope.model,
                variants=scope.variants,
                scopes=(),
            )
            for scope in self.scopes
        )

    def obligations(self):
        return {
            Obligation(
                check.connector, chain, check.mode, check.profile, check.model, action, variant
            )
            for check in self.checks()
            for chain in check.chains
            for action in check.actions
            for variant in check.variants or (None,)
            if check.connector is not None
        }


def required(bindings=None):
    result = set()
    for name, connector in installed_connectors() if bindings is None else bindings:
        for support in connector.chain_support:
            result.update(execution_obligations(name, support))
            for model in support.historical:
                result.update(
                    Obligation(name, support.chain, "backtest", "", model.model, action, variant)
                    for action in model.actions
                    for variant in model.variants or (None,)
                )
    return result


def execution_obligations(name, support):
    return {
        Obligation(name, support.chain, "fork", profile, "", action, variant)
        for profile in support.account_profiles
        for action in support.actions
        for variant in support.variants or (None,)
        if RunMode.FORK in support.modes or RunMode.PRODUCTION in support.modes
    }


def reference_cases(root=ROOT):
    from tests.qa.composed_catalog import COMPOSED_REFERENCES, composed_cases

    found = discover(root / "strategies", root / "tests/strategy_fork/scenarios")
    mapped = set(REFERENCES) | set(COMPOSED_REFERENCES)
    if mapped & set(CANDIDATES) or set(REFERENCES) & set(COMPOSED_REFERENCES):
        raise ValueError("a reference cannot be both a candidate and a mapped proof")
    if {c.name for c in found} != mapped | set(CANDIDATES):
        raise ValueError("QA references differ from discovered strategies; update proof mappings")
    result = []
    for source in found:
        if source.name in CANDIDATES:
            result.append(candidate_case(source.name))
            continue
        if source.name in COMPOSED_REFERENCES:
            result.extend(composed_cases(source))
            continue
        connector, chains = reference_scope(source)
        actions, modes = REFERENCES[source.name]
        common = dict(reference=source.name, connector=connector, chains=chains)
        variants = CURVE_VARIANTS | AERODROME_VARIANTS
        if source.name in variants:
            common["variants"] = (variants[source.name],)
        result.extend(fork_cases(common, actions))
        for mode in modes:
            filename = "backtest-economic.yaml" if mode == "economic" else "backtest.yaml"
            if not source.config.with_name(filename).is_file():
                raise ValueError(f"{source.name}: missing {filename}")
            result.extend(history_cases(common, actions, mode))
            if source.name == "aave_v4_weth_supply" and mode == "economic":
                vendor = "backtest-economic-vendor.yaml"
                if not source.config.with_name(vendor).is_file():
                    raise ValueError(f"{source.name}: missing {vendor}")
                result.extend(history_cases(common, actions, mode, vendor))
    return result


def converted(case) -> bool:
    """Whether this QA case reads the current public reference or scenario root."""
    if not case.reference:
        return False
    directory = (
        ROOT / "tests/qa/scenarios/curve_mixed"
        if case.scenario == "curve_mixed"
        else ROOT / "strategies" / case.reference
    )
    config = directory / "config.yaml"
    return config.is_file() and public_root(load(config).value)


def reference_scope(source):
    """The connector and account chains one reference proves, from its public author file."""
    inputs = read_inputs(source.config).inputs
    connectors = {ConnectorId(str(m.connector)) for m in inputs.markets.values()}
    if len(connectors) != 1:
        raise ValueError(f"{source.name}: a mapped reference needs one market connector")
    # An account that only receives a bridge is on a chain no position acts on: the
    # reference proves nothing there.
    acting = {market.chain for market in inputs.markets.values()}
    return connectors.pop(), tuple(c for c in account_chains(inputs) if c in acting)


def account_chains(inputs):
    return tuple(dict.fromkeys(account.chain for account in inputs.accounts.values()))


def reference_chains(root, reference):
    """The account chains bound by one shipped public reference."""
    config = root / "strategies" / reference / "config.yaml"
    return account_chains(read_inputs(config).inputs)


def candidate_case(name):
    connector, chains = CANDIDATES[name]
    selected = tuple(ChainId(chain) for chain in chains)
    identity = ConnectorId(connector) if connector is not None else None
    binding = dict(installed_connectors())[identity] if identity is not None else None
    if binding is not None and any(s.chain in selected for s in binding.chain_support):
        raise ValueError(f"{name}: promoted support requires an explicit QA proof mapping")
    return Case(
        id=f"blocked/{name}",
        lane="blocked",
        reference=name,
        connector=identity,
        chains=selected,
        blocked_reason="Incomplete proof mapping: rollout proof gates remain open.",
    )


def fork_cases(common, actions):
    # A clean GMX exit escrows its request-sized approvals in full and revokes nothing;
    # the gmx_cleanup cases prove revoke_exchange after a crash instead.
    if common["connector"] == ConnectorId("gmx_v2"):
        actions = tuple(a for a in actions if a != "revoke_exchange")
    # A clean bridge clears its own approval inside the bridge and requests no revoke;
    # across_supplemental_cases proves revoke_bridge from an interrupted run instead.
    if common["connector"] == ConnectorId("across"):
        actions = tuple(a for a in actions if a != "revoke_bridge")
    declared = declared_profiles(common["connector"], common["chains"])
    for policy in ("pinned", "fresh"):
        for profile, operator in (
            ("safe_roles", "configured"),
            ("safe_roles", "distinct"),
            ("eoa", "configured"),
        ):
            # A cell proves only the account profiles its connector declares; HyperCore's
            # Safe owns the account, so it declares no EOA profile (blueprint 30).
            if profile not in declared:
                continue
            yield Case(
                id=f"fork/{policy}/{common['reference']}/{profile}/{operator}",
                lane="fork",
                mode="fork",
                input_policy=policy,
                profile=profile,
                operator_mode=operator,
                actions=tuple(map(EconomicAction, actions)),
                **common,
            )


def declared_profiles(connector, chains):
    """The account profiles a reference's connector declares on its chains; a composed
    reference's scopes name their connectors themselves, so it keeps every profile."""
    if connector is None:
        return {"safe_roles", "eoa"}
    binding = dict(installed_connectors())[connector]
    return {p for s in binding.chain_support if s.chain in chains for p in s.account_profiles}


def history_cases(common, actions, mode, backtest_file=""):
    selected = actions
    if common["connector"] == ConnectorId("aave_v4") and mode == "historical_path":
        selected = ("supply", "withdraw")
    if (
        common["connector"]
        in (
            ConnectorId("uniswap_v4"),
            ConnectorId("uniswap_v3"),
            ConnectorId("pancakeswap_v3"),
            ConnectorId("aerodrome_slipstream"),
            ConnectorId("aerodrome"),
        )
        and mode == "economic"
    ):
        selected = tuple(a for a in actions if a != "revoke_exchange")
    # The bridge model keeps no approval, so a backtest never serves a revoke.
    if common["connector"] == ConnectorId("across"):
        selected = tuple(a for a in actions if a != "revoke_bridge")
    for policy in ("pinned", "fresh", "native"):
        suffix = "/vendor" if backtest_file else ""
        yield Case(
            id=f"backtest/{policy}/{common['reference']}/{mode}{suffix}",
            lane="backtest",
            mode="backtest",
            input_policy=policy,
            model=reference_model(common, mode),
            model_mode=mode,
            backtest_file=backtest_file,
            actions=tuple(map(EconomicAction, selected)),
            **common,
        )


def reference_model(common, mode):
    if common.get("reference") in GMX_REFERENCE_MODELS:
        return GMX_REFERENCE_MODELS[common["reference"]]
    if common["connector"] == ConnectorId("curve"):
        return (
            CURVE_STABLE_MODEL
            if common["variants"] == (ProtocolVariant("stableswap_ng"),)
            else CRYPTO_MODEL
        )
    return MODELS[common["connector"], mode]


def catalog(root=ROOT):
    return [
        Case(id="offline/quality", lane="offline", nodes=("tests",)),
        Case(
            id="fork/faults",
            lane="faults",
            nodes=(
                "tests/fork",
                "tests/strategy_fork/test_composed_outages.py",
                "tests/strategy_fork/test_multichain_outages.py",
                "tests/strategy_fork/test_production_rehearsals.py",
                *(f"{ACROSS_PROOFS}::{name}" for name in ACROSS_FAULT_PROOFS),
                # The composed bridge reference stage by stage, and its refund.
                COMPOSED_BRIDGE_PROOFS,
            ),
        ),
        Case(id="offline/support-refusals", lane="pytest", nodes=("tests/qa/test_support.py",)),
        *reference_cases(root),
        *supplemental_cases(),
        *curve_supplemental_cases(root),
        *uniswap_supplemental_cases(root),
        *gmx_supplemental_cases(root),
        *across_supplemental_cases(root),
        *hypercore_supplemental_cases(),
        *curve_base_model_cases(),
        *curve_arbitrum_model_cases(),
        *curve_expansion_model_cases(),
        *curve_robinhood_model_cases(),
    ]


def supplemental_cases():
    yield Case(
        id="model-actions/uniswap_v4/eip155:56",
        lane="pytest",
        connector=ConnectorId("uniswap_v4"),
        chains=(ChainId("eip155:56"),),
        mode="backtest",
        model=MODELS[ConnectorId("uniswap_v4"), "economic"],
        model_mode="economic",
        actions=(EconomicAction.REVOKE,),
        nodes=(
            "tests/qa/test_uniswap_v4_bnb.py::test_prepared_revoke",
            "tests/qa/test_uniswap_v4_bnb.py::test_retained_quoter_comparisons_match_current_pool_math",
        ),
    )
    yield Case(
        id="model-actions/uniswap_v4/eip155:8453",
        lane="pytest",
        connector=ConnectorId("uniswap_v4"),
        chains=(ChainId("eip155:8453"),),
        mode="backtest",
        model=MODELS[ConnectorId("uniswap_v4"), "economic"],
        model_mode="economic",
        actions=(EconomicAction.REVOKE,),
        nodes=("tests/qa/test_uniswap_v4_base.py::test_prepared_revoke",),
    )
    yield Case(
        id="model-actions/uniswap_v4/eip155:42161",
        lane="pytest",
        connector=ConnectorId("uniswap_v4"),
        chains=(ChainId("eip155:42161"),),
        mode="backtest",
        model=MODELS[ConnectorId("uniswap_v4"), "economic"],
        model_mode="economic",
        actions=(EconomicAction.REVOKE,),
        nodes=("tests/qa/test_uniswap_v4_arbitrum.py::test_prepared_revoke",),
    )
    yield Case(
        id="model-actions/uniswap_v4/eip155:10",
        lane="pytest",
        connector=ConnectorId("uniswap_v4"),
        chains=(ChainId("eip155:10"),),
        mode="backtest",
        model=MODELS[ConnectorId("uniswap_v4"), "economic"],
        model_mode="economic",
        actions=(EconomicAction.REVOKE,),
        nodes=("tests/qa/test_uniswap_v4_optimism.py::test_prepared_revoke",),
    )
    for chain in (ChainId("eip155:1"), ChainId("eip155:43114")):
        for name, actions in (
            (
                ConnectorId("aave_v4"),
                ("withdraw_amount", "repay_amount", "collateral", "aave_refresh"),
            ),
            (ConnectorId("uniswap_v4"), ("revoke_exchange",)),
        ):
            yield Case(
                id=f"model-actions/{name}/{chain}",
                lane="pytest",
                connector=name,
                chains=(chain,),
                mode="backtest",
                model=MODELS[name, "economic"],
                model_mode="economic",
                actions=tuple(map(EconomicAction, actions)),
                nodes=(f"tests/qa/test_model_actions.py::test_prepared_actions[{name}-{chain}]",),
            )


def hypercore_supplemental_cases():
    """HyperCore's backtest actions, proven by the offline lifecycle over a real
    preparation's `/info` answers; its fork cell is the reference's own fork cases."""
    from almanak.connectors.hyperliquid.economic_data import MODEL

    module = "tests/test_hypercore_backtest_model.py"
    yield Case(
        id="model-actions/hyperliquid/eip155:999",
        lane="pytest",
        reference="hyperliquid_hyperevm_perp",
        connector=ConnectorId("hyperliquid"),
        chains=(ChainId("eip155:999"),),
        mode="backtest",
        model=MODEL,
        model_mode="economic",
        actions=tuple(map(EconomicAction, HYPERCORE)),
        nodes=(
            f"{module}::test_the_reference_lifecycle_runs_and_reproduces_offline",
            f"{module}::test_a_position_the_venue_liquidates_ends_the_hold",
            f"{module}::test_the_reference_hooks_drive_the_model_in_process",
        ),
    )


# A case that runs pytest nodes takes no input policy: its proof stands for both.
NODE_LANES = ("pytest", "faults")


def validate(cases, bindings=None):
    ids = [case.id for case in cases]
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate QA case identity")
    candidates = {case for case in cases if case.reference in CANDIDATES}
    if candidates != {candidate_case(name) for name in CANDIDATES}:
        raise ValueError("QA coverage gaps: candidate blockers changed or omitted")
    expected = required(bindings)
    covered = set().union(*(c.obligations() for c in cases))
    missing = expected - covered
    stale = covered - expected
    if missing or stale:
        raise ValueError(
            f"QA coverage gaps: {sorted(missing, key=repr)}; "
            f"unsupported proof claims: {sorted(stale, key=repr)}"
        )
    for policy in ("pinned", "fresh"):
        present = set().union(
            *(c.obligations() for c in cases if c.input_policy == policy or c.lane in NODE_LANES)
        )
        if gaps := expected - present:
            raise ValueError(f"QA {policy} coverage gaps: {sorted(gaps, key=repr)}")
    validate_model_modes(cases, bindings)
    return expected


def validate_model_modes(cases, bindings=None):
    modes = {
        (name, support.chain, model.model): model.mode
        for name, connector in (installed_connectors() if bindings is None else bindings)
        for support in connector.chain_support
        for model in support.historical
    }
    for case in cases:
        for check in case.checks():
            for chain in check.chains:
                if check.model and check.model_mode != modes[check.connector, chain, check.model]:
                    raise ValueError(f"{case.id}: mode differs from the declared historical model")


def curve_supplemental_cases(root=ROOT):
    for reference, variant in CURVE_VARIANTS.items():
        if not reference.endswith("_lp"):
            continue
        chains = reference_chains(root, reference)
        for policy in ("pinned", "fresh"):
            for profile in ("eoa", "safe_roles"):
                yield Case(
                    id=f"cleanup/{policy}/{reference}/{profile}",
                    lane="fork",
                    reference=reference,
                    variants=(variant,),
                    mode="fork",
                    profile=profile,
                    input_policy=policy,
                    scenario="curve_cleanup",
                    actions=(EconomicAction.REVOKE,),
                    connector=ConnectorId("curve"),
                    chains=chains,
                )
    # The mixed proof executes these Ethereum targets, not every revision on other chains.
    variants = tuple(
        CURVE_VARIANTS[name]
        for name in (
            "curve_ethereum_stable_lp",
            "curve_ethereum_twocrypto_lp",
            "curve_ethereum_tricrypto_lp",
        )
    )
    for policy in ("pinned", "fresh", "native"):
        yield Case(
            id=f"backtest/{policy}/curve_mixed/economic",
            lane="backtest",
            reference="curve_ethereum_tricrypto_lp",
            variants=variants,
            mode="backtest",
            model=CRYPTO_MODEL,
            model_mode="economic",
            input_policy=policy,
            scenario="curve_mixed",
            actions=tuple(map(EconomicAction, CURVE_LP)),
            connector=ConnectorId("curve"),
            chains=(ChainId("eip155:1"),),
        )
    for model, selected, filename, function in (
        (
            CURVE_STABLE_MODEL,
            (ProtocolVariant("stableswap_ng"),),
            "test_curve_prepared_runner",
            "test_revoke_preserves_captured_inventory",
        ),
        (
            CRYPTO_MODEL,
            variants,
            "test_curve_mixed_prepared_runner",
            "test_revoke_preserves_inventory_for_every_captured_family",
        ),
    ):
        yield Case(
            id=f"model-actions/curve/{model}/revoke",
            lane="pytest",
            mode="backtest",
            model=model,
            model_mode="economic",
            variants=selected,
            actions=(EconomicAction.REVOKE,),
            nodes=(f"tests/{filename}.py::{function}",),
            connector=ConnectorId("curve"),
            chains=(ChainId("eip155:1"),),
        )


def uniswap_lp_cleanup_references():
    # Every promoted fixed-range Uniswap V3/V4 LP reference plus the independently
    # promoted Pancake BNB LP reference. Other Pancake rows are not production support.
    # Banded references, the RSI-gated LP among them, reopen a range each cycle and are
    # outside this fixed-range lane, whatever their name.
    return tuple(
        sorted(
            name
            for name in REFERENCES
            if REFERENCES[name][0] != BAND_LP
            and (
                (name.startswith(("uniswap_v3_", "uniswap_v4")) and name.endswith("_lp"))
                or name == "pancakeswap_v3_bnb_lp"
            )
        )
    )


def uniswap_lp_family(reference):
    name = str(reference)
    if name == "pancakeswap_v3" or name.startswith("pancakeswap_v3_"):
        return "pancakeswap_v3"
    return "uniswap_v4" if name.startswith("uniswap_v4") else "uniswap_v3"


def uniswap_supplemental_cases(root=ROOT):
    # Interrupted approval cleanup for promoted V3/V4 LP references, mirroring the
    # Curve lane: the approval commits, the process dies, and closure must revoke
    # without trading. Candidates join only after promotion.
    for reference in uniswap_lp_cleanup_references():
        connector = uniswap_lp_family(reference)
        chains = reference_chains(root, reference)
        for policy in ("pinned", "fresh"):
            for profile in ("eoa", "safe_roles"):
                yield Case(
                    id=f"cleanup/{policy}/{reference}/{profile}",
                    lane="fork",
                    reference=reference,
                    mode="fork",
                    profile=profile,
                    input_policy=policy,
                    scenario="uniswap_cleanup",
                    actions=(EconomicAction.REVOKE,),
                    connector=ConnectorId(connector),
                    chains=chains,
                )


def curve_base_model_cases():
    for family, model, filename in (
        ("stableswap_ng", CURVE_STABLE_MODEL, "test_curve_prepared_runner"),
        ("twocrypto_ng", CRYPTO_MODEL, "test_curve_crypto_prepared_runner"),
        ("tricrypto_ng", CRYPTO_MODEL, "test_curve_tricrypto_prepared_runner"),
    ):
        yield Case(
            id=f"model-actions/curve/eip155:8453/{family}/revoke",
            lane="pytest",
            mode="backtest",
            model=model,
            model_mode="economic",
            variants=(ProtocolVariant(family),),
            actions=(EconomicAction.REVOKE,),
            nodes=(f"tests/{filename}.py::test_revoke_preserves_captured_inventory[base_lp]",),
            connector=ConnectorId("curve"),
            chains=(ChainId("eip155:8453"),),
        )


def curve_arbitrum_model_cases():
    for index, (family, model) in enumerate(
        (
            ("stableswap_ng", CURVE_STABLE_MODEL),
            ("twocrypto_ng", CRYPTO_MODEL),
            ("tricrypto_ng", CRYPTO_MODEL),
        )
    ):
        yield Case(
            id=f"model-actions/curve/eip155:42161/{family}/revoke",
            lane="pytest",
            mode="backtest",
            model=model,
            model_mode="economic",
            variants=(ProtocolVariant(family),),
            actions=(EconomicAction.REVOKE,),
            nodes=(
                f"tests/test_curve_arbitrum_prepared.py::test_cleanup_preserves_the_captured_fund_inventory[captured{index}]",
            ),
            connector=ConnectorId("curve"),
            chains=(ChainId("eip155:42161"),),
        )


def curve_expansion_model_cases():
    references = (
        ("optimism", "eip155:10", ("stable", "twocrypto", "tricrypto")),
        ("bnb", "eip155:56", ("stable", "twocrypto")),
        ("polygon", "eip155:137", ("stable", "twocrypto", "tricrypto")),
        ("avalanche", "eip155:43114", ("stable",)),
    )
    for chain_name, chain, families in references:
        for index, family in enumerate(families):
            variant = CURVE_VARIANTS[f"curve_{chain_name}_{family}_lp"]
            yield Case(
                id=f"model-actions/curve/{chain}/{variant}/revoke",
                lane="pytest",
                mode="backtest",
                model=CURVE_STABLE_MODEL if family == "stable" else CRYPTO_MODEL,
                model_mode="economic",
                variants=(variant,),
                actions=(EconomicAction.REVOKE,),
                nodes=(
                    f"tests/test_curve_{chain_name}_prepared.py::"
                    f"test_cleanup_preserves_the_captured_fund_inventory[captured{index}]",
                ),
                connector=ConnectorId("curve"),
                chains=(ChainId(chain),),
            )


def curve_robinhood_model_cases():
    for variant, model, filename, index in (
        ("stableswap_ng", CURVE_STABLE_MODEL, "stable", 0),
        ("twocrypto_ng", CRYPTO_MODEL, "crypto", 0),
        ("tricrypto_ng_2024", CRYPTO_MODEL, "crypto", 1),
    ):
        yield Case(
            id=f"model-actions/curve/eip155:4663/{variant}/revoke",
            lane="pytest",
            mode="backtest",
            model=model,
            model_mode="economic",
            variants=(ProtocolVariant(variant),),
            actions=(EconomicAction.REVOKE,),
            nodes=(
                f"tests/test_curve_robinhood_{filename}_prepared.py::"
                f"test_cleanup_preserves_the_captured_fund_inventory[captured{index}]",
            ),
            connector=ConnectorId("curve"),
            chains=(ChainId("eip155:4663"),),
        )


ACROSS_PROOFS = "tests/strategy_fork/test_across_bridge.py"
COMPOSED_BRIDGE_PROOFS = "tests/strategy_fork/test_composed_bridge.py"
# The supervised proofs that claim no action of their own: a refund, a restart and an outage.
ACROSS_FAULT_PROOFS = (
    "test_an_unfilled_transfer_is_refunded_only_after_the_destination_proves_it",
    "test_a_coordinator_killed_after_the_deposit_sends_no_second_one",
    "test_the_source_bridges_while_the_destination_is_unavailable",
)
ACROSS_REVOKE_PROOF = "test_teardown_revokes_an_approval_whose_deposit_was_never_signed"


def across_supplemental_cases(root=ROOT):
    """A bridge leaves an approval to revoke only when it is interrupted between its
    approval and its deposit; this supervised proof is the one that revokes."""
    reference = "across_usdc_arbitrum_base_bridge"
    yield Case(
        id=f"cleanup/{reference}/eoa",
        lane="faults",
        reference=reference,
        mode="fork",
        profile="eoa",
        actions=(EconomicAction.REVOKE_BRIDGE,),
        connector=ConnectorId("across"),
        # The source chain alone: no position acts where the bridge is received.
        chains=(ChainId("eip155:42161"),),
        nodes=(f"{ACROSS_PROOFS}::{ACROSS_REVOKE_PROOF}",),
    )


def gmx_supplemental_cases(root=ROOT):
    """Request-sized approvals leave a revocation only after a crash; these cases prove it."""
    for chain, path, revocation in (
        (chain, path, revocation)
        for chain in ("arbitrum", "avalanche")
        for path, revocation in (("position", "revoke_position"), ("swap", "revoke_exchange"))
    ):
        reference = f"gmx_v2_{chain}_{path}"
        chains = reference_chains(root, reference)
        for policy in ("pinned", "fresh"):
            for profile, operator in (
                ("eoa", "configured"),
                ("safe_roles", "configured"),
                ("safe_roles", "distinct"),
            ):
                yield Case(
                    id=f"cleanup/{policy}/{reference}/{profile}/{operator}",
                    lane="fork",
                    reference=reference,
                    mode="fork",
                    profile=profile,
                    operator_mode=operator,
                    input_policy=policy,
                    scenario="gmx_cleanup",
                    actions=(EconomicAction(revocation),),
                    connector=ConnectorId("gmx_v2"),
                    chains=chains,
                )
