"""Revision-bound Curve support promoted from retained lifecycle proofs."""

from almanak.caip import ChainId
from almanak.connectors.identifiers import ProtocolVariant
from almanak.connectors.support import (
    ONE_ALLOCATION_PER_FUND,
    ConnectorChainSupport,
    EconomicAction,
    RunMode,
)


def candidate_support():
    return (
        ConnectorChainSupport(
            chain=ChainId(chain),
            modes=(RunMode.FORK,),
            variants=tuple(
                ProtocolVariant(family)
                for family in ("stableswap_ng", "twocrypto_ng", "tricrypto_ng")
            ),
            actions=(
                EconomicAction.SWAP,
                EconomicAction.OPEN_LIQUIDITY,
                EconomicAction.INCREASE_LIQUIDITY,
                EconomicAction.DECREASE_LIQUIDITY,
                EconomicAction.CLOSE_LIQUIDITY,
                EconomicAction.EXIT_EXCHANGE,
                EconomicAction.REVOKE,
            ),
            account_profiles=("eoa", "safe_roles"),
            restrictions=(
                "candidate fork verification only; not promoted",
                "verified StableSwap NG, TwoCrypto NG or TriCrypto NG revision",
                "ordered standard non-rebasing coins; no metapool routing",
                ONE_ALLOCATION_PER_FUND,
                "LP fees embedded in share economics; explicit configured single-coin exit",
            ),
        )
        for chain in ("eip155:1",)
    )


def verified_support():
    from datetime import UTC, datetime

    from almanak.connectors.curve.economic_data import CRYPTO_MODEL, MODEL
    from almanak.connectors.support import HistoricalSupport

    candidate = next(iter(candidate_support()))
    return tuple(
        candidate.model_copy(
            update={
                "chain": ChainId(chain),
                "variants": variants,
                "modes": (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST),
                "restrictions": candidate.restrictions[1:]
                + (
                    "economic simulation assumes execution access "
                    "and fixed external market history",
                    "explicit bounded samples and source age; no missing-input substitution",
                )
                + (
                    ("pinned Jovian L1 and operator fee accounting",)
                    if chain in ("eip155:8453", "eip155:10")
                    else ()
                ),
                "historical": tuple(
                    HistoricalSupport(
                        model=model,
                        mode="economic",
                        actions=candidate.actions,
                        variants=variants,
                        backtest_start_at=(
                            datetime(2026, 9, 8, 5, 28, tzinfo=UTC)
                            if chain == "eip155:4663"
                            else datetime(2026, 9, 7, 20, tzinfo=UTC)
                        ),
                    )
                    for model, variants in (
                        (MODEL, (ProtocolVariant("stableswap_ng"),)),
                        (
                            CRYPTO_MODEL,
                            variants
                            if chain == "eip155:1"
                            else tuple(
                                v for v in variants if v != ProtocolVariant("stableswap_ng")
                            ),
                        ),
                    )
                    if variants
                ),
            }
        )
        for chain, variants in (
            ("eip155:1", candidate.variants),
            ("eip155:8453", candidate.variants),
            ("eip155:42161", candidate.variants),
            ("eip155:43114", candidate.variants[:1]),
            ("eip155:56", candidate.variants[:2]),
            ("eip155:10", candidate.variants[:2] + (ProtocolVariant("tricrypto_ng_2024"),)),
            ("eip155:137", candidate.variants[:2] + (ProtocolVariant("tricrypto_ng_2024"),)),
            ("eip155:4663", candidate.variants[:2] + (ProtocolVariant("tricrypto_ng_2024"),)),
        )
    )
