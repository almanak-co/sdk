"""Composed QA scope discovery follows current public author declarations."""

from pathlib import Path

from almanak.caip import ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from tests.qa.composed_catalog import COMPOSED_REFERENCES, proof_scope


def test_scope_uses_current_public_market_chain(tmp_path):
    reference = "base_aave_curve_borrow_and_trade"
    source = tmp_path / "config.yaml"
    source.write_text(
        (Path(__file__).parents[2] / "strategies" / reference / "config.yaml").read_text()
    )
    author = read_inputs(source).inputs
    changed = ChainId("eip155:1")
    market = author.markets["pool"].model_copy(update={"chain": changed})
    author = author.model_copy(update={"markets": {**author.markets, "pool": market}})

    actions = dict(COMPOSED_REFERENCES[reference])[ConnectorId("curve")]
    scope = proof_scope(reference, author, ConnectorId("curve"), actions)

    assert scope.chains == (changed,)
    assert scope.variants == (ProtocolVariant("stableswap_ng"),)


def test_variant_mapping_names_one_current_authored_market():
    reference = "base_aave_aerodrome_stable_borrow_and_trade"
    root = Path(__file__).parents[2]
    author = read_inputs(root / "strategies" / reference / "config.yaml").inputs
    actions = dict(COMPOSED_REFERENCES[reference])[ConnectorId("aerodrome")]

    scope = proof_scope(reference, author, ConnectorId("aerodrome"), actions)

    assert scope.variants == (ProtocolVariant("stable"),)
    assert scope.chains == (ChainId("eip155:8453"),)
