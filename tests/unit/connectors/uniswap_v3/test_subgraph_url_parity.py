"""ID-parity test: DexVolumeDecl.volume_subgraph_urls IDs must match
_UNISWAP_V3_VOLUME_SUBGRAPH_IDS in the gateway provider (plan 024).

The connector manifest stores the full gateway.thegraph.com URLs.  Each URL
ends with the subgraph deployment ID (``/id/<ID>``).  This test extracts the
ID from each URL and compares it to the authoritative dict in the gateway
connector, ensuring there is exactly one copy of the truth.
"""

from __future__ import annotations


def test_volume_subgraph_url_ids_match_gateway_provider() -> None:
    """Deployment IDs embedded in DexVolumeDecl URLs must equal gateway provider IDs."""
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.connectors.uniswap_v3.gateway.provider import _UNISWAP_V3_VOLUME_SUBGRAPH_IDS

    connector = CONNECTOR_REGISTRY.get("uniswap_v3")
    assert connector is not None
    assert connector.dex_volume is not None
    urls = connector.dex_volume.volume_subgraph_urls
    assert urls is not None, "uniswap_v3 DexVolumeDecl must declare volume_subgraph_urls"

    # Extract IDs from gateway.thegraph.com/api/subgraphs/id/<ID> URLs
    prefix = "https://gateway.thegraph.com/api/subgraphs/id/"
    mismatches: list[str] = []
    for chain, url in urls.items():
        assert url.startswith(prefix), f"Unexpected URL format for {chain!r}: {url!r}"
        manifest_id = url[len(prefix):]
        gateway_id = _UNISWAP_V3_VOLUME_SUBGRAPH_IDS.get(chain)
        if gateway_id is None:
            mismatches.append(f"  {chain!r}: manifest has ID but gateway dict missing the chain")
        elif manifest_id != gateway_id:
            mismatches.append(f"  {chain!r}: manifest={manifest_id!r} gateway={gateway_id!r}")

    # Every chain in the gateway dict must also appear in the manifest
    for chain in _UNISWAP_V3_VOLUME_SUBGRAPH_IDS:
        if chain not in urls:
            mismatches.append(f"  {chain!r}: in gateway dict but missing from manifest volume_subgraph_urls")

    assert not mismatches, "ID mismatch between manifest and gateway provider:\n" + "\n".join(mismatches)


def test_hosted_subgraph_urls_match_subgraph_module() -> None:
    """Hosted-service URLs in DexVolumeDecl must equal the derived UNISWAP_V3_HOSTED_SUBGRAPHS."""
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.backtesting.pnl.providers.subgraph import UNISWAP_V3_HOSTED_SUBGRAPHS

    connector = CONNECTOR_REGISTRY.get("uniswap_v3")
    assert connector is not None
    assert connector.dex_volume is not None
    hosted = connector.dex_volume.hosted_volume_subgraph_urls
    assert hosted is not None

    assert dict(hosted) == UNISWAP_V3_HOSTED_SUBGRAPHS


def test_volume_subgraph_urls_match_subgraph_module() -> None:
    """volume_subgraph_urls in DexVolumeDecl must equal UNISWAP_V3_SUBGRAPHS."""
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.backtesting.pnl.providers.subgraph import UNISWAP_V3_SUBGRAPHS

    connector = CONNECTOR_REGISTRY.get("uniswap_v3")
    assert connector is not None
    assert connector.dex_volume is not None
    urls = connector.dex_volume.volume_subgraph_urls
    assert urls is not None

    assert dict(urls) == UNISWAP_V3_SUBGRAPHS
