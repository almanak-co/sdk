from __future__ import annotations

from types import SimpleNamespace

from scripts.docs import generate_chain_table, generate_connector_matrix
from scripts.docs.support_docs import (
    ChainDoc,
    ConnectorDoc,
    SupportDocsModel,
    _normalize_strategy_chains,
    connector_page_slug,
)

_RAW_UNSET = object()


def _chain(
    name: str,
    *,
    display_name: str | None = None,
    chain_id: int = 1,
    family: str = "EVM",
    aliases: tuple[str, ...] = (),
) -> ChainDoc:
    return ChainDoc(
        name=name,
        display_name=display_name or name.replace("_", " ").title(),
        chain_id=chain_id,
        family=family,
        aliases=aliases,
    )


def _connector(
    name: str,
    *,
    display_name: str | None = None,
    page_slug: str | None = None,
    module_path: str | None = None,
    kind: str = "swap",
    aliases: tuple[str, ...] = (),
    strategy_intents: tuple[str, ...] | None = ("SWAP",),
    strategy_chains: tuple[str, ...] | None = ("arbitrum",),
    raw_strategy_chains: tuple[str, ...] | None | object = _RAW_UNSET,
) -> ConnectorDoc:
    if raw_strategy_chains is _RAW_UNSET:
        raw_strategy_chains = strategy_chains
    return ConnectorDoc(
        name=name,
        display_name=display_name or name.replace("_", " ").title(),
        page_slug=page_slug or name,
        module_path=module_path or f"almanak.connectors.{name}",
        kind=kind,
        aliases=aliases,
        strategy_intents=strategy_intents,
        strategy_chains=strategy_chains,
        raw_strategy_chains=raw_strategy_chains,
    )


def _model(
    *,
    chains: tuple[ChainDoc, ...] = (
        _chain("arbitrum", display_name="Arbitrum", chain_id=42161),
        _chain("bsc", display_name="BNB Chain", chain_id=56, aliases=("bnb",)),
        _chain("solana", display_name="Solana", chain_id=0, family="SVM"),
    ),
    connectors: tuple[ConnectorDoc, ...] = (),
) -> SupportDocsModel:
    return SupportDocsModel(chains=chains, connectors=connectors)


def test_strategy_chain_aliases_normalize_to_canonical_names(monkeypatch) -> None:
    from almanak.core.chains import ChainRegistry

    def try_resolve(chain_name: str) -> SimpleNamespace | None:
        if chain_name == "bnb":
            return SimpleNamespace(name="bsc")
        if chain_name in {"bsc", "ethereum"}:
            return SimpleNamespace(name=chain_name)
        return None

    monkeypatch.setattr(ChainRegistry, "try_resolve", staticmethod(try_resolve))

    normalized = _normalize_strategy_chains(
        ("ethereum", "bnb", "bsc", "unknown"),
        {"ethereum": 0, "bsc": 1},
    )

    assert normalized == ("ethereum", "bsc", "unknown")


def test_balancer_v2_uses_legacy_connector_slug() -> None:
    assert connector_page_slug("balancer_v2") == "balancer"
    assert connector_page_slug("uniswap_v3") == "uniswap_v3"


def test_off_chain_connector_renders_as_na() -> None:
    connector = _connector("kraken", strategy_chains=None)
    model = _model(connectors=(connector,))
    index = generate_connector_matrix.generate(model)
    page = generate_connector_matrix.generate_connector_page(connector, model)

    assert "N/A (off-chain)" in index
    assert "| N/A (off-chain) | N/A | ``SWAP`` |" in page


def test_empty_strategy_chains_render_registered_empty_state() -> None:
    connector = _connector("empty_chains", strategy_chains=())
    model = _model(connectors=(connector,))
    index = generate_connector_matrix.generate(model)
    page = generate_connector_matrix.generate_connector_page(connector, model)

    assert "No strategy chains registered" in index
    assert "| No strategy chains registered | N/A | ``SWAP`` |" in page


def test_no_strategy_connector_renders_registered_empty_state() -> None:
    connector = _connector(
        "beefy",
        kind="vault",
        strategy_intents=None,
        strategy_chains=None,
    )
    model = _model(connectors=(connector,))
    page = generate_connector_matrix.generate_connector_page(connector, model)

    assert "No strategy chains registered" in page
    assert "No strategy intents registered" in page


def test_balancer_page_uses_legacy_docs_slug() -> None:
    connector = _connector(
        "balancer_v2",
        display_name="Balancer",
        page_slug=connector_page_slug("balancer_v2"),
        module_path="almanak.connectors.balancer_v2",
        kind="lp",
        aliases=("balancer",),
        strategy_intents=("LP_OPEN",),
    )
    model = _model(connectors=(connector,))
    page = generate_connector_matrix.generate_connector_page(connector, model)

    assert connector.page_slug == "balancer"
    assert page.startswith("# Balancer\n")
    assert "::: almanak.connectors.balancer_v2" in page


def test_generated_connector_page_has_support_table_and_api_reference() -> None:
    connector = _connector(
        "uniswap_v3",
        display_name="Uniswap V3",
        kind="lp",
        strategy_intents=("SWAP", "LP_OPEN"),
        strategy_chains=("bsc",),
    )
    model = _model(connectors=(connector,))
    index = generate_connector_matrix.generate(model)
    page = generate_connector_matrix.generate_connector_page(connector, model)

    assert "``almanak.connectors.uniswap_v3``" in index
    assert "## Supported Chains And Intents" in page
    assert "| Chain | Family | Supported Intents |" in page
    assert "| [BNB Chain](../../chains/bsc.md) | EVM |" in page
    assert "``LP_OPEN``" in page
    assert "::: almanak.connectors.uniswap_v3" in page


def test_generated_chain_page_lists_only_supported_connectors() -> None:
    connectors = (
        _connector("uniswap_v3", display_name="Uniswap V3", strategy_chains=("arbitrum",)),
        _connector("kraken", strategy_chains=None),
        _connector("jupiter", strategy_chains=("solana",)),
    )
    chain = _chain("arbitrum", display_name="Arbitrum", chain_id=42161)
    model = _model(chains=(chain, _chain("solana", family="SVM", chain_id=0)), connectors=connectors)
    page = generate_chain_table.generate_chain_page(chain, model)

    assert "../api/connectors/uniswap_v3.md" in page
    assert "``almanak.connectors.uniswap_v3``" in page
    assert "../api/connectors/kraken.md" not in page
    assert "../api/connectors/jupiter.md" not in page


def test_chain_index_links_to_generated_chain_pages() -> None:
    model = _model(
        chains=(
            _chain("arbitrum", display_name="Arbitrum", chain_id=42161),
            _chain("solana", display_name="Solana", chain_id=0, family="SVM"),
        ),
    )
    index = generate_chain_table.generate(model)

    assert "[``arbitrum``](chains/arbitrum.md)" in index
    assert "| [``solana``](chains/solana.md) | N/A | SVM | N/A | 0 |" in index
    assert "| Name | Chain ID | Family | Aliases | Connectors |" in index
