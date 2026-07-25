"""VIB-5991: extcodesize gate for sushiswap_v3 registered contract addresses.

The connector's registered base ``swap_router`` had NO CODE on base mainnet:
``exactInputSingle`` calls to it "succeeded" (status=1, ~25k gas, zero events)
as silent no-ops, and the ledger recorded a success row with empty amounts.
A codeless target is undetectable by tx status alone — this gate catches the
whole class by asserting every (chain, contract) address in ``SUSHISWAP_V3``
has non-empty bytecode on its chain.

Opt-in / on-chain (requires ALCHEMY_API_KEY), mirroring the pattern of
``tests/unit/connectors/curve/test_curve_pools_config.py``
(TestCurvePoolsCoinOrderOnChain). Runs in the integration shard only.

To run:
    uv run pytest tests/integration/connectors/test_sushiswap_v3_address_codesize.py -v
"""

from __future__ import annotations

import os
import time

import httpx
import pytest

from almanak.connectors.sushiswap_v3.addresses import SUSHISWAP_V3

pytestmark = pytest.mark.integration

# Chain -> Alchemy RPC URL template (all chains registered in SUSHISWAP_V3).
_CHAIN_RPC = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{key}",
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{key}",
    "avalanche": "https://avax-mainnet.g.alchemy.com/v2/{key}",
    "bsc": "https://bnb-mainnet.g.alchemy.com/v2/{key}",
    "optimism": "https://opt-mainnet.g.alchemy.com/v2/{key}",
}


def _rpc(rpc_url: str, method: str, params: list[object], attempts: int = 3) -> object:
    """Return the JSON-RPC result for ``method``.

    Retries transient transport errors (Alchemy's bnb endpoint occasionally
    drops TLS handshakes) so a network flake cannot masquerade as a verdict.
    """
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = httpx.post(rpc_url, json=payload, timeout=15)
            resp.raise_for_status()
            body = resp.json()
            if "error" in body:
                raise RuntimeError(f"{method} failed for {params}: {body['error']}")
            return body.get("result")
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            last_exc = exc
            time.sleep(1 + attempt)
    raise RuntimeError(f"{method} transport failure for {params} after {attempts} attempts: {last_exc}")


def _get_code(rpc_url: str, address: str) -> str:
    """Return the eth_getCode result ('0x' means no code) for ``address``."""
    return str(_rpc(rpc_url, "eth_getCode", [address, "latest"]) or "0x")


def _selector(signature: str) -> str:
    """4-byte selector for ``signature`` (derived, never hand-typed)."""
    from web3 import Web3

    return "0x" + Web3.keccak(text=signature)[:4].hex()


def _call_address_getter(rpc_url: str, address: str, signature: str) -> str | None:
    """``eth_call`` a zero-arg getter returning ``address``; None when unsupported.

    A contract that does not implement the getter reverts or returns empty
    data — both mean "this is not the contract we think it is", which the
    caller turns into a failed assertion rather than a silent pass.
    """
    try:
        raw = _rpc(rpc_url, "eth_call", [{"to": address, "data": _selector(signature)}, "latest"])
    except RuntimeError:
        return None
    if not isinstance(raw, str) or len(raw) < 66:
        return None
    return "0x" + raw[-40:]


class TestSushiSwapV3AddressesHaveCode:
    """Every registered sushiswap_v3 contract address must have bytecode."""

    def test_all_registered_chains_have_rpc_mapping(self) -> None:
        """A new chain in SUSHISWAP_V3 must be added to _CHAIN_RPC to stay gated.

        Deliberately credential-free: this invariant is deterministic, and if
        it only ran with ``ALCHEMY_API_KEY`` set, a newly registered chain
        could be silently dropped from the parametrised on-chain gate below
        (which filters on ``chain in _CHAIN_RPC``) with nothing complaining.
        """
        missing = set(SUSHISWAP_V3) - set(_CHAIN_RPC)
        assert not missing, f"chains missing from _CHAIN_RPC (add so the codesize gate covers them): {sorted(missing)}"

    @pytest.fixture
    def alchemy_key(self) -> str:
        key = os.environ.get("ALCHEMY_API_KEY")
        if not key:
            pytest.skip("ALCHEMY_API_KEY not set")
        return key

    @pytest.mark.parametrize(
        "chain,contract_key,address",
        [
            (chain, contract_key, address)
            for chain, contracts in SUSHISWAP_V3.items()
            for contract_key, address in contracts.items()
            if chain in _CHAIN_RPC
        ],
    )
    def test_address_has_code(self, chain: str, contract_key: str, address: str, alchemy_key: str) -> None:
        """eth_getCode must be non-empty — a codeless target no-ops silently."""
        rpc_url = _CHAIN_RPC[chain].format(key=alchemy_key)
        code = _get_code(rpc_url, address)
        assert code not in ("0x", "0x0", "", None), (
            f"{chain}/{contract_key} address {address} has NO CODE on-chain — "
            "calls to it succeed as silent no-ops (VIB-5991)"
        )

    @pytest.mark.parametrize(
        "chain,contract_key,address,expected_factory",
        [
            (chain, contract_key, contracts[contract_key], contracts["factory"])
            for chain, contracts in SUSHISWAP_V3.items()
            for contract_key in ("swap_router", "position_manager", "quoter_v2")
            if chain in _CHAIN_RPC and contract_key in contracts and "factory" in contracts
        ],
    )
    def test_periphery_contract_reports_registered_factory(
        self,
        chain: str,
        contract_key: str,
        address: str,
        expected_factory: str,
        alchemy_key: str,
    ) -> None:
        """Identity, not just presence: ``factory()`` must be the registered factory.

        ``eth_getCode != "0x"`` still passes a *wrong-but-deployed* contract —
        exactly the Avalanche case, where the registered ``swap_router`` was a
        Sushi RouteProcessor (deployed, plenty of code, but no
        ``exactInputSingle``). Every V3 periphery contract exposes an immutable
        ``factory()``; cross-checking it against this chain's registered factory
        proves the address really belongs to *this* deployment and cannot drift
        from the address book, since both sides come from ``SUSHISWAP_V3``.
        """
        rpc_url = _CHAIN_RPC[chain].format(key=alchemy_key)
        reported = _call_address_getter(rpc_url, address, "factory()")
        assert reported is not None, (
            f"{chain}/{contract_key} address {address} does not implement factory() — "
            "it is not a SushiSwap V3 periphery contract (VIB-5991 RouteProcessor class)"
        )
        assert reported.lower() == expected_factory.lower(), (
            f"{chain}/{contract_key} address {address} reports factory() == {reported}, "
            f"but the registered factory for {chain} is {expected_factory} — "
            "the address belongs to a different deployment (VIB-5991)"
        )
