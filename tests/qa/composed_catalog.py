"""A composed reference runs once and proves explicitly scoped connector actions."""

from dataclasses import replace

from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composed.economic_book import MODEL
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import EconomicAction, RunMode
from tests.qa.catalog import (
    Case,
    ProofScope,
    declared_profiles,
    fork_cases,
    installed_connectors,
    reference_model,
)

COMPOSED_REFERENCES = {
    "arbitrum_aave_lending_short": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_aave_borrow_lp": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (
            ConnectorId("uniswap_v3"),
            ("swap_exact_input", "open_liquidity", "close_liquidity", "exit_exchange"),
        ),
    ),
    "arbitrum_aave_leverage_loop": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_morpho_leverage_loop": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "bnb_aave_borrow_and_trade_v4": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "polygon_aave_borrow_and_trade_v4": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "polygon_aave_borrow_and_trade_v3": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "avalanche_aave_borrow_and_trade_v3": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "bnb_aave_borrow_and_trade_v3": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_base_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "bnb_aave_pancake_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("pancakeswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_aave_pancake_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("pancakeswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_aave_pancake_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("pancakeswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_aave_aerodrome_stable_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("aerodrome"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_aave_aerodrome_volatile_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("aerodrome"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_aave_slipstream_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("aerodrome_slipstream"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_aave_curve_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_aave_curve_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_aave_pancake_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("pancakeswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_aave_v4_borrow_and_trade": (
        (ConnectorId("aave_v4"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "avalanche_aave_v4_borrow_and_trade": (
        (ConnectorId("aave_v4"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "avalanche_morpho_borrow_and_trade_v4": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "robinhood_morpho_borrow_and_trade_v4": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "robinhood_morpho_borrow_and_trade_v3": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "optimism_aave_borrow_and_trade_v4": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_spark_curve_borrow_and_trade": (
        (ConnectorId("sparklend"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_compound_curve_borrow_and_trade": (
        (ConnectorId("compound_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_aave_borrow_and_trade_v4": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "bnb_morpho_curve_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "robinhood_morpho_curve_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_aave_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "optimism_aave_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "bnb_aave_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "polygon_aave_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "avalanche_aave_borrow_and_trade": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "optimism_compound_curve_borrow_and_trade": (
        (ConnectorId("compound_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "optimism_morpho_curve_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "polygon_morpho_curve_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_borrow_and_trade_v4": (
        (ConnectorId("aave_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v4"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_morpho_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "ethereum_morpho_curve_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("curve"), ("swap_exact_input", "exit_exchange")),
    ),
    "base_compound_borrow_and_trade": (
        (ConnectorId("compound_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_compound_borrow_and_trade": (
        (ConnectorId("compound_v3"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    "arbitrum_morpho_borrow_and_trade": (
        (ConnectorId("morpho_blue"), ("supply", "borrow", "repay", "withdraw")),
        (ConnectorId("uniswap_v3"), ("swap_exact_input", "exit_exchange")),
    ),
    # The bridge clears its own approval inside a clean bridge; the standalone Across
    # cleanup case proves revoke_bridge.
    "arbitrum_base_bridge_and_lend": (
        (ConnectorId("across"), ("bridge",)),
        (ConnectorId("compound_v3"), ("supply", "withdraw")),
    ),
}

COMPOSED_VARIANTS = {
    "base_aave_aerodrome_stable_borrow_and_trade": {
        ConnectorId("aerodrome"): (ProtocolVariant("stable"),)
    },
    "base_aave_aerodrome_volatile_borrow_and_trade": {
        ConnectorId("aerodrome"): (ProtocolVariant("volatile"),)
    },
    **{
        name: {ConnectorId("curve"): (ProtocolVariant("stableswap_ng"),)}
        for name, components in COMPOSED_REFERENCES.items()
        if any(connector == ConnectorId("curve") for connector, _ in components)
    },
}


def composed_cases(source):
    author = read_inputs(source.config).inputs
    scopes = tuple(
        proof_scope(source.name, author, connector, actions)
        for connector, actions in COMPOSED_REFERENCES[source.name]
    )
    configured = {ConnectorId(str(selection.connector)) for selection in author.markets.values()}
    if {scope.connector for scope in scopes} != configured or any(
        not scope.chains for scope in scopes
    ):
        raise ValueError("composed proof scopes must match every configured connector")
    common = dict(
        reference=source.name,
        connector=None,
        chains=tuple(sorted({chain for scope in scopes for chain in scope.chains}, key=str)),
        scopes=scopes,
    )
    # A composed reference proves only the account profiles every component declares
    # on its chains: a bridge that declares EOA accounts alone has no Safe/Roles case.
    profiles = set.intersection(
        *(declared_profiles(scope.connector, scope.chains) for scope in scopes)
    )
    if not profiles:
        raise ValueError(f"{source.name}: its components declare no common account profile")
    yield from (case for case in fork_cases(common, ()) if case.profile in profiles)
    if not all(declares_backtest(scope) for scope in scopes):
        # A component that declares no backtest on its chain leaves the reference
        # without a backtest configuration and without a backtest case.
        return
    if not source.config.with_name("backtest-economic.yaml").is_file():
        raise ValueError(f"{source.name}: missing backtest-economic.yaml")
    for policy in ("pinned", "fresh", "native"):
        yield Case(
            id=f"backtest/{policy}/{source.name}/economic",
            lane="backtest",
            mode="backtest",
            input_policy=policy,
            model=MODEL,
            model_mode="economic",
            **{
                **common,
                "scopes": tuple(
                    replace(
                        scope,
                        model=reference_model(
                            {"connector": scope.connector, "variants": scope.variants}, "economic"
                        ),
                    )
                    for scope in scopes
                ),
            },
        )


def declares_backtest(scope) -> bool:
    """Whether the scope's connector declares backtest mode on every chain it uses here."""
    declared = {
        s.chain: s.modes for s in dict(installed_connectors())[scope.connector].chain_support
    }
    return all(RunMode.BACKTEST in declared.get(chain, ()) for chain in scope.chains)


def proof_scope(reference, author, connector, actions):
    positions = (*author.strategy.allocations.values(), *author.strategy.debts.values())
    markets = tuple(
        author.markets[position.market]
        for position in positions
        if ConnectorId(str(author.markets[position.market].connector)) == connector
    )
    variants = COMPOSED_VARIANTS.get(reference, {}).get(connector, ())
    if variants and len(markets) != 1:
        raise ValueError("variant-scoped composed proof must name exactly one authored market")
    return ProofScope(
        connector=connector,
        chains=tuple(sorted({market.chain for market in markets}, key=str)),
        actions=tuple(map(EconomicAction, actions)),
        variants=variants,
    )
