"""Validation and query contracts for unified connector chain support."""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType

import pytest

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector, StrategyMatrixEntry, SupportedChainsSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.mantle import DESCRIPTOR as MANTLE
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON


def _connector(
    *,
    supported_chains: SupportedChainsSpec | None = SupportedChainsSpec(chains=(ETHEREUM,)),
    intents: tuple[str, ...] | None = ("SWAP",),
    aliases: tuple[str, ...] = (),
) -> Connector:
    return Connector(
        name="example",
        kind=ProtocolKind.SWAP,
        aliases=aliases,
        strategy_intents=intents,
        supported_chains=supported_chains,
    )


def test_registered_descriptors_project_canonical_names() -> None:
    spec = SupportedChainsSpec(chains=(BSC, ETHEREUM))
    assert spec.chains == ("bsc", "ethereum")
    assert spec.all_chains() == ("bsc", "ethereum")
    assert spec.supports(chain="bnb")


@pytest.mark.parametrize(
    ("chains", "error", "match"),
    [
        ((), ValueError, "non-empty tuple"),
        ((ETHEREUM, ETHEREUM), ValueError, "duplicate canonical chains"),
        (("ethereum",), TypeError, "registered ChainDescriptor"),
        ((replace(ETHEREUM, name="ethereuem"),), ValueError, "unregistered ChainDescriptor"),
    ],
)
def test_invalid_chain_declarations_fail(chains: object, error: type[Exception], match: str) -> None:
    with pytest.raises(error, match=match):
        SupportedChainsSpec(chains=chains)  # type: ignore[arg-type]


def test_offchain_declaration_is_explicit() -> None:
    connector = _connector(supported_chains=SupportedChainsSpec(chains=None))
    assert connector.supported_chains is not None
    assert connector.supported_chains.is_offchain
    assert connector.all_supported_chains == ()
    assert connector.supports(chain="ethereum", intent="SWAP") is False


def test_offchain_declaration_rejects_overrides() -> None:
    with pytest.raises(ValueError, match="off-chain"):
        SupportedChainsSpec(
            chains=None,
            intent_overrides={"SWAP": (ETHEREUM,)},
        )


def test_intent_override_replaces_default() -> None:
    connector = Connector(
        name="fluid_like",
        kind=ProtocolKind.SWAP,
        strategy_intents=("SWAP", "SUPPLY"),
        supported_chains=SupportedChainsSpec(
            chains=(ARBITRUM, BASE),
            intent_overrides={"swap": (ARBITRUM, BASE, ETHEREUM, POLYGON)},
        ),
    )
    assert connector.supported_chains_for_intent("SWAP") == (
        "arbitrum",
        "base",
        "ethereum",
        "polygon",
    )
    assert connector.supported_chains_for_intent("SUPPLY") == ("arbitrum", "base")


def test_protocol_override_is_alias_specific() -> None:
    connector = _connector(
        aliases=("fork",),
        supported_chains=SupportedChainsSpec(
            chains=(ETHEREUM,),
            protocol_overrides={"fork": (MANTLE,)},
        ),
    )
    assert connector.supported_chains_for_protocol("example") == ("ethereum",)
    assert connector.supported_chains_for_protocol("fork") == ("mantle",)
    assert connector.supports(chain="mantle", protocol="fork", intent="SWAP")
    assert not connector.supports(chain="mantle", protocol="example", intent="SWAP")


def test_override_mappings_are_frozen() -> None:
    spec = SupportedChainsSpec(
        chains=(ETHEREUM,),
        intent_overrides={"SWAP": (BASE,)},
        protocol_overrides={"fork": (MANTLE,)},
    )
    assert isinstance(spec.intent_overrides, MappingProxyType)
    assert isinstance(spec.protocol_overrides, MappingProxyType)
    with pytest.raises(TypeError):
        spec.intent_overrides["SWAP"] = ("ethereum",)  # type: ignore[index]


def test_undeclared_intent_override_is_rejected() -> None:
    with pytest.raises(ValueError, match="undeclared strategy intents"):
        _connector(
            supported_chains=SupportedChainsSpec(
                chains=(ETHEREUM,),
                intent_overrides={"BORROW": (BASE,)},
            )
        )


def test_unowned_protocol_override_is_rejected() -> None:
    with pytest.raises(ValueError, match="connector aliases"):
        _connector(
            supported_chains=SupportedChainsSpec(
                chains=(ETHEREUM,),
                protocol_overrides={"not_owned": (MANTLE,)},
            )
        )


def test_strategy_intents_require_supported_chains_spec() -> None:
    with pytest.raises(ValueError, match="supported_chains is required"):
        _connector(supported_chains=None)


def test_supported_chains_without_strategy_intents_is_rejected() -> None:
    with pytest.raises(ValueError, match="only be set when strategy_intents is set"):
        _connector(intents=None)


def test_removed_strategy_chains_argument_is_rejected_immediately() -> None:
    with pytest.raises(TypeError, match="strategy_chains"):
        Connector(  # type: ignore[call-arg]
            name="legacy",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            strategy_chains=("ethereum",),
        )


def test_matrix_entries_group_intents_without_owning_chains() -> None:
    connector = Connector(
        name="grouped",
        kind=ProtocolKind.SWAP,
        strategy_intents=("SWAP", "SUPPLY"),
        supported_chains=SupportedChainsSpec(
            chains=(ARBITRUM,),
            intent_overrides={"SWAP": (ARBITRUM, ETHEREUM)},
        ),
        strategy_matrix_entries=(
            StrategyMatrixEntry(matrix_name="grouped", category="swap", intents=("SWAP",)),
            StrategyMatrixEntry(matrix_name="grouped", category="lending", intents=("SUPPLY",)),
        ),
    )
    swap, lending = connector.strategy_matrix_entries or ()
    assert swap.intents == ("SWAP",)
    assert lending.intents == ("SUPPLY",)
    assert not hasattr(swap, "chains")


def test_matrix_entry_rejects_undeclared_or_duplicate_intents() -> None:
    with pytest.raises(ValueError, match="undeclared strategy intents"):
        Connector(
            name="unknown_row_intent",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
            strategy_matrix_entries=(
                StrategyMatrixEntry(
                    matrix_name="unknown_row_intent",
                    category="swap",
                    intents=("BORROW",),
                ),
            ),
        )

    with pytest.raises(ValueError, match="assigns an intent to multiple rows"):
        Connector(
            name="duplicate_row_intent",
            kind=ProtocolKind.SWAP,
            strategy_intents=("SWAP",),
            supported_chains=SupportedChainsSpec(chains=(ETHEREUM,)),
            strategy_matrix_entries=(
                StrategyMatrixEntry(
                    matrix_name="duplicate_row_intent",
                    category="swap",
                    intents=("SWAP",),
                ),
                StrategyMatrixEntry(
                    matrix_name="duplicate_row_intent_alias",
                    category="aggregator",
                    intents=("SWAP",),
                ),
            ),
        )
