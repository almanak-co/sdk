"""The operator gateway adapter must satisfy the compile path's gateway contract.

The compiler prefers ``gateway_client`` for every on-chain read and converts
ANY exception into "read unavailable" (``compiler_queries.query_erc20_balance``
returns ``None``). A method missing from this adapter therefore never raises:
it silently degrades into whatever fallback the connector chose — for Aave
``repay_full`` that was ``approve(pool, MAX_UINT256)`` on a reusable live
wallet. These tests pin the adapter to the real ``GatewayClient`` signatures
and enumerate, from the AST, every gateway method the compile path can call.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import SimpleNamespace

from web3 import Web3

from almanak.framework.gateway_client import GatewayClient
from scripts.qa.operator_gateway import OperatorGatewayClient

REPO = Path(__file__).resolve().parents[3]
WALLET = "0x" + "11" * 20
TOKEN = "0x" + "22" * 20

#: Gateway methods the compile path can name that this adapter deliberately
#: does not provide. Each entry carries the EXACT set of files allowed to name
#: it — the reason as an assertion, not prose. The 2026-08-30 mainnet incident
#: proved a prose reason can be false while the test stays green: this list said
#: query_position_liquidity was "V4 only" while the V3 LP_CLOSE compile path
#: called it, and both LP cells crashed after funding. The test fails when a gap
#: becomes implemented, stops being called, or is named from a NEW file.
KNOWN_GAPS: dict[str, tuple[str, frozenset[str]]] = {
    "connector_stub": (
        "raw gRPC Polymarket stub; no mainnet recipe compiles it",
        frozenset({"almanak/connectors/polymarket/gateway_client.py"}),
    ),
    "estimate_gas": (
        "strategy-base RPC helper; not on a mainnet recipe compile path",
        frozenset({"almanak/connectors/_strategy_base/rpc.py"}),
    ),
    "health_check": (
        "multichain orchestration only; mainnet recipes are single-chain",
        frozenset({"almanak/framework/execution/multichain.py"}),
    ),
    "query_v4_position_closure": (
        "Uniswap V4 teardown post-condition; no V4 mainnet recipe",
        frozenset({"almanak/connectors/uniswap_v4/teardown_post_condition.py"}),
    ),
    "query_v4_position_state": (
        "Uniswap V4 runner hook; no V4 mainnet recipe",
        frozenset({"almanak/connectors/uniswap_v4/runner_hooks.py"}),
    ),
}


class _Eth:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, object]] = []
        self.balance_calls: list[tuple[str, object]] = []
        self.result = (10**18).to_bytes(32, "big")
        self.native = 5 * 10**17
        self.fail = False

    def call(self, tx: dict, block_identifier: object = "latest") -> bytes:
        if self.fail:
            raise RuntimeError("rpc down")
        self.calls.append((tx, block_identifier))
        return self.result

    def get_balance(self, address: str, block_identifier: object = "latest") -> int:
        if self.fail:
            raise RuntimeError("rpc down")
        self.balance_calls.append((address, block_identifier))
        return self.native


def _client() -> tuple[OperatorGatewayClient, _Eth]:
    eth = _Eth()
    web3 = SimpleNamespace(eth=eth, is_connected=lambda: True)
    return OperatorGatewayClient(web3, "arbitrum"), eth  # type: ignore[arg-type]


def test_query_erc20_balance_issues_balance_of_calldata_at_the_requested_block() -> None:
    client, eth = _client()
    assert client.query_erc20_balance("arbitrum", TOKEN, WALLET, block=123) == 10**18
    (tx, block), *_ = eth.calls
    assert tx["to"] == Web3.to_checksum_address(TOKEN)
    assert tx["data"] == "0x70a08231" + WALLET.removeprefix("0x").zfill(64)
    assert block == 123
    # Keyword form is how compiler_queries calls it.
    assert client.query_erc20_balance(chain="arbitrum", token_address=TOKEN, wallet_address=WALLET) == 10**18
    assert eth.calls[-1][1] == "latest"


def test_query_native_balance_reads_get_balance_at_the_requested_block() -> None:
    client, eth = _client()
    assert client.query_native_balance("arbitrum", WALLET, block=77) == 5 * 10**17
    assert eth.balance_calls == [(Web3.to_checksum_address(WALLET), 77)]


def test_query_position_liquidity_reads_positions_word_seven() -> None:
    client, eth = _client()
    words = [0] * 12
    words[7] = 54_230_821_885
    eth.result = b"".join(value.to_bytes(32, "big") for value in words)
    assert client.query_position_liquidity("arbitrum", TOKEN, 5676758, block=42) == 54_230_821_885
    (tx, block), *_ = eth.calls
    assert tx["to"] == Web3.to_checksum_address(TOKEN)
    assert tx["data"] == "0x99fbab88" + hex(5676758)[2:].zfill(64)
    assert block == 42
    # Keyword form is how compiler_queries calls it.
    assert client.query_position_liquidity(chain="arbitrum", position_manager=TOKEN, token_id=5676758) == 54_230_821_885
    eth.result = (10**18).to_bytes(32, "big")  # truncated positions payload is unavailable, not a guess
    assert client.query_position_liquidity("arbitrum", TOKEN, 5676758) is None


def test_positions_reads_fold_a_burned_nft_revert_to_measured_zero() -> None:
    """Uniswap V3-family NPMs revert ``positions(tokenId)`` with "Invalid token ID" once the NFT is
    burned; the real client folds that to 0 / (0, 0) so a burned position is MEASURED closed."""
    client, eth = _client()

    def _revert(tx: dict, block_identifier: object = "latest") -> bytes:
        raise ValueError("execution reverted: Invalid token ID")

    eth.call = _revert  # type: ignore[method-assign]
    assert client.query_position_liquidity("arbitrum", TOKEN, 5676758) == 0
    assert client.query_position_tokens_owed("arbitrum", TOKEN, 5676758) == (0, 0)


def test_positions_reads_keep_other_failures_unavailable() -> None:
    client, eth = _client()
    eth.fail = True
    assert client.query_position_liquidity("arbitrum", TOKEN, 5676758) is None
    assert client.query_position_tokens_owed("arbitrum", TOKEN, 5676758) is None
    eth.fail = False
    eth.result = (10**18).to_bytes(32, "big")  # truncated payload is unavailable, not a guess
    assert client.query_position_tokens_owed("arbitrum", TOKEN, 5676758) is None


def test_tokens_owed_reads_positions_words_ten_and_eleven() -> None:
    client, eth = _client()
    words = [0] * 12
    words[10], words[11] = 17, 23
    eth.result = b"".join(value.to_bytes(32, "big") for value in words)
    assert client.query_position_tokens_owed("arbitrum", TOKEN, 5676758, block=42) == (17, 23)
    (_, block), *_ = eth.calls
    assert block == 42


def test_burned_nft_chain_verifies_through_the_shared_npm_closure_rule() -> None:
    """The closure post-condition must certify a burned NFT read through this shim."""
    from almanak.connectors._strategy_base.teardown_post_condition import verify_npm_position_closure

    client, eth = _client()

    def _revert(tx: dict, block_identifier: object = "latest") -> bytes:
        raise ValueError("execution reverted: Invalid token ID")

    eth.call = _revert  # type: ignore[method-assign]
    result = verify_npm_position_closure(
        protocol="uniswap_v3",
        position_id="5676758",
        chain="arbitrum",
        token_id=5676758,
        npm_address=TOKEN,
        gateway_client=client,
        block=42,
    )
    assert result.closed is True and not result.unmeasured


def test_balance_reads_fail_to_none_like_the_real_client() -> None:
    client, eth = _client()
    assert client.query_erc20_balance("base", TOKEN, WALLET) is None, "chain mismatch is unavailable, not a guess"
    assert client.query_native_balance("base", WALLET) is None
    assert client.query_position_liquidity("base", TOKEN, 7) is None
    eth.fail = True
    assert client.query_erc20_balance("arbitrum", TOKEN, WALLET) is None
    assert client.query_native_balance("arbitrum", WALLET) is None
    assert client.query_position_liquidity("arbitrum", TOKEN, 7) is None


def test_balance_read_signatures_match_gateway_client_exactly() -> None:
    for name in (
        "query_erc20_balance",
        "query_native_balance",
        "query_allowance",
        "eth_call",
        "query_position_liquidity",
    ):
        real = inspect.signature(getattr(GatewayClient, name))
        shim = inspect.signature(getattr(OperatorGatewayClient, name))
        assert list(real.parameters) == list(shim.parameters), name
        for param in real.parameters.values():
            assert shim.parameters[param.name].kind == param.kind, f"{name}.{param.name} kind"
            assert shim.parameters[param.name].default == param.default, f"{name}.{param.name} default"


def _gateway_method_sites() -> dict[str, set[str]]:
    """Every ``<...>gateway_client.<name>(`` call reachable from a compile, by AST, with its files."""
    sites: dict[str, set[str]] = {}
    roots = (
        REPO / "almanak" / "framework" / "intents",
        REPO / "almanak" / "framework" / "execution",
        REPO / "almanak" / "connectors",
    )
    for root in roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                owner = node.func.value
                owner_name = owner.attr if isinstance(owner, ast.Attribute) else getattr(owner, "id", "")
                if owner_name.endswith("gateway_client"):
                    sites.setdefault(node.func.attr, set()).add(path.relative_to(REPO).as_posix())
    return sites


def _gateway_methods_named_on_the_compile_path() -> set[str]:
    return set(_gateway_method_sites())


def test_adapter_covers_every_gateway_method_the_compile_path_can_call() -> None:
    sites = _gateway_method_sites()
    named = set(sites)
    assert {"query_erc20_balance", "eth_call", "query_allowance", "query_position_liquidity"} <= named, (
        "enumeration lost the known call sites"
    )
    provided = {name for name in dir(OperatorGatewayClient) if not name.startswith("_")}
    missing = sorted(named - provided - set(KNOWN_GAPS))
    assert not missing, f"OperatorGatewayClient lacks gateway methods the compile path calls: {missing}"
    stale = sorted(set(KNOWN_GAPS) & provided)
    assert not stale, f"KNOWN_GAPS lists methods the adapter now implements: {stale}"
    unnamed = sorted(set(KNOWN_GAPS) - named)
    assert not unnamed, f"KNOWN_GAPS lists methods nothing on the compile path calls: {unnamed}"
    for name, (reason, allowed) in KNOWN_GAPS.items():
        stray = sorted(sites.get(name, set()) - allowed)
        assert not stray, (
            f"KNOWN_GAPS reason for {name!r} ({reason}) no longer holds: also named from {stray}. "
            "Either the gap is now reachable from a mainnet compile and must be implemented, "
            "or the allowed-site set needs a reviewed update."
        )
