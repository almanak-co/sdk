"""Named public endpoints: reads and action previews without a provider key.

Offline: no test here opens a connection. The endpoints themselves are exercised by
running the commands against them, which is not part of this suite.
"""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import ax, ax_actions
from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.cli_arguments import build_parser, parse_validated
from almanak.gateway import providers, read_endpoints
from almanak.gateway.ethereum import DataError
from almanak.gateway.onchain_lookup import VendorLookups
from almanak.gateway.providers import PublicProvider, provider_endpoint, public_endpoint

ETHEREUM = ChainId("eip155:1")
ACTION = [
    "ax",
    "supply",
    "--connector",
    "aave_v4",
    "--chain",
    "eip155:1",
    "--spoke",
    "0x94e7a5dcbe816e498b89ab752661904e2f56c485",
    "--reserve-id",
    "0",
    "--amount",
    "5",
    "--wallet",
    "wallet",
    "--max-gas-price-wei",
    "5",
    "--max-transaction-fee",
    "5",
]
BALANCE = [
    "ax",
    "balance",
    "--asset",
    "eip155:1/slip44:60",
    "--account",
    "0x" + "11" * 20,
]


def parse(*arguments):
    return parse_validated(build_parser(), list(arguments))


def test_every_listed_endpoint_is_https_on_a_chain_with_a_pinned_genesis():
    assert providers.PUBLIC_RPC
    for name, chains in providers.PUBLIC_RPC.items():
        assert PublicProvider(kind="public_rpc", name=name).name == name
        for chain, url in chains.items():
            providers.validate_provider_url(url)
            assert url.startswith("https://") and evm_network(chain).genesis


def test_no_public_endpoint_is_listed_for_hyperevm():
    assert all(ChainId("eip155:999") not in chains for chains in providers.PUBLIC_RPC.values())


def test_a_name_resolves_only_on_a_chain_it_serves(monkeypatch):
    monkeypatch.setattr(
        providers,
        "PUBLIC_RPC",
        {"one": {ETHEREUM: "https://one.example"}, "two": {ETHEREUM: "https://two.example"}},
    )
    assert public_endpoint("one", ETHEREUM) == "https://one.example"
    with pytest.raises(ValueError, match="choose from one, two"):
        public_endpoint("three", ETHEREUM)
    # A chain with no entry names the alternative instead of another endpoint.
    with pytest.raises(ValueError, match="use a provider key instead"):
        public_endpoint("one", ChainId("eip155:999"))


def test_a_public_provider_is_refused_outside_ax(tmp_path):
    """Production strategy, fork, check and backtest paths all resolve through this function."""
    provider = PublicProvider(kind="public_rpc", name="publicnode")
    with pytest.raises(ValueError, match="only `almanak ax` reads and direct actions"):
        provider_endpoint(provider, tmp_path / "missing.env")


class Reader:
    def __init__(self, genesis, head_time):
        self.chain = SimpleNamespace(chain_id=ETHEREUM)
        self.blocks = {0: genesis, "latest": "0xhead"}
        self.head_time = head_time
        self.pins = []

    @contextmanager
    def retained_pin(self):
        yield

    def pin_block(self, selector):
        self.pins.append(selector)
        return SimpleNamespace(hash=self.blocks[selector], timestamp=self.head_time)


def test_a_public_endpoint_must_show_the_pinned_genesis_and_a_recent_head():
    genesis = evm_network(ETHEREUM).genesis
    read_endpoints.verify_public(Reader(genesis, 1_000), clock=lambda: 1_100)
    with pytest.raises(DataError, match="different genesis") as other:
        read_endpoints.verify_public(Reader("0x" + "00" * 32, 1_000), clock=lambda: 1_000)
    assert other.value.kind == "chain_mismatch"
    with pytest.raises(DataError, match="head is 500 seconds old") as stale:
        read_endpoints.verify_public(Reader(genesis, 1_000), clock=lambda: 1_500)
    assert stale.value.kind == "stale"


def test_a_reader_that_fails_verification_is_closed_and_never_returned(monkeypatch):
    closed = []

    class Failing:
        def verify_chain(self):
            raise DataError("chain_mismatch", "RPC chain does not match the requested chain")

        def __exit__(self, *arguments):
            closed.append(True)

    seen = {}

    def reader(chain, endpoint, *, observation):
        seen.update(chain=chain, endpoint=endpoint, observation=observation)
        return Failing()

    monkeypatch.setattr(read_endpoints, "mainnet_reader", reader)
    with pytest.raises(DataError):
        read_endpoints.public_reader(ETHEREUM, "publicnode")
    assert closed == [True]
    assert seen == {
        "chain": ETHEREUM,
        "endpoint": providers.PUBLIC_RPC["publicnode"][ETHEREUM],
        "observation": True,
    }


def test_a_read_names_exactly_one_transport(monkeypatch, capsys):
    for name in ("ALMANAK_AX_GATEWAY_URL", "ALMANAK_AX_GATEWAY_AUTH_FILE"):
        monkeypatch.delenv(name, raising=False)
    read = parse(*BALANCE, "--public-rpc", "publicnode")
    assert (read.public_rpc, read.env_file, read.gateway_url) == ("publicnode", None, None)
    for arguments in (
        # No transport: a public endpoint is never chosen for the caller.
        BALANCE,
        [*BALANCE, "--public-rpc", "publicnode", "--env-file", ".env"],
        [*BALANCE, "--public-rpc", "publicnode", "--gateway-auth-file", "token"],
        [*BALANCE, "--public-rpc", "unlisted"],
    ):
        with pytest.raises(SystemExit):
            parse(*arguments)
    assert "--public-rpc" in capsys.readouterr().err


def test_the_session_gateway_does_not_override_a_named_public_endpoint(monkeypatch):
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("ALMANAK_AX_GATEWAY_AUTH_FILE", "token")
    read = parse(*BALANCE, "--public-rpc", "drpc")
    assert read.public_rpc == "drpc" and read.gateway_url is None


def test_reads_on_a_public_endpoint_use_the_verified_public_reader(monkeypatch):
    monkeypatch.setattr(ax, "public_reader", lambda chain, name: (chain, name))
    args = SimpleNamespace(env_file=None, public_rpc="drpc", gateway_url=None)
    assert ax.observation_reader(args, ETHEREUM) == (ETHEREUM, "drpc")


def test_a_lookup_that_needs_a_vendor_key_is_refused_only_when_it_is_made():
    args = SimpleNamespace(env_file=None, public_rpc="drpc", gateway_url=None)
    with ax.lookup_source(args) as lookups:
        assert isinstance(lookups, VendorLookups)
        with pytest.raises(DataError, match="uses a vendor key") as refused:
            lookups.coingecko_api()
    assert refused.value.kind == "unavailable"


def test_an_action_takes_a_key_or_a_public_endpoint_to_preview_and_to_execute(capsys):
    preview = parse(*ACTION, "--public-rpc", "publicnode")
    assert (preview.public_rpc, preview.env_file, preview.execute) == ("publicnode", None, False)
    executing = parse(*ACTION, "--public-rpc", "publicnode", "--execute")
    assert executing.execute
    for args in (preview, executing):
        provider, endpoint = ax_actions.transport(args, ETHEREUM)
        assert provider == PublicProvider(kind="public_rpc", name="publicnode")
        assert endpoint == providers.PUBLIC_RPC["publicnode"][ETHEREUM]
    for arguments in (ACTION, [*ACTION, "--public-rpc", "publicnode", "--env-file", ".env"]):
        with pytest.raises(SystemExit):
            parse(*arguments)
    capsys.readouterr()


def test_an_action_with_a_key_resolves_the_keyed_provider(tmp_path):
    env = tmp_path / ".env"
    env.write_text("ALCHEMY_API_KEY=k\n")
    args = SimpleNamespace(public_rpc=None, env_file=env, execute=True)
    provider, endpoint = ax_actions.transport(args, ETHEREUM)
    assert provider.kind == "alchemy" and endpoint.endswith("/v2/k")


def test_a_preview_configuration_records_the_public_provider_it_used(recorded):
    import json

    reserve = recorded.config.allocations[0].reserve
    document = json.loads(reserve.model_dump_json().replace("eip155:31337", "eip155:1"))
    document["chain"]["environment"] = "mainnet"
    from almanak.connectors.aave_v4 import direct
    from almanak.connectors.direct import ActionRequest, Wallet

    reserve = type(reserve).model_validate_json(json.dumps(document))
    wallet = Wallet(account="wallet", address="0x" + "ab" * 20)
    resolved = SimpleNamespace(reserve=reserve)
    config = ax_actions.configuration(
        "aave_v4",
        direct.positions(None, None, resolved, wallet, ActionRequest("supply", 1)),
        wallet,
        PublicProvider(kind="public_rpc", name="publicnode"),
        reserve.chain,
        ax_actions.FeeLimits(max_gas_price_wei=5, max_transaction_fee=9),
    )
    provider = config.production.networks[0].provider
    assert provider == PublicProvider(kind="public_rpc", name="publicnode")
    # A strategy, fork or backtest path resolving this configuration's endpoint refuses it.
    with pytest.raises(ValueError, match="reads and direct actions"):
        provider_endpoint(provider, Path(".env"))
    # Recovery reaches the endpoint the action ran on, with no credentials.
    assert ax_actions.record_endpoint(config, None) == providers.PUBLIC_RPC["publicnode"][ETHEREUM]


def test_recovering_a_keyed_record_needs_the_env_file(tmp_path):
    network = SimpleNamespace(
        provider=ax_actions.alchemy(ETHEREUM), chain=SimpleNamespace(chain_id=ETHEREUM)
    )
    config = SimpleNamespace(production=SimpleNamespace(networks=[network]))
    with pytest.raises(ValueError, match="--env-file is required"):
        ax_actions.record_endpoint(config, None)
    env = tmp_path / ".env"
    env.write_text("ALCHEMY_API_KEY=k\n")
    assert ax_actions.record_endpoint(config, env).endswith("/v2/k")


def test_head_age_counts_the_time_the_read_itself_took():
    genesis = evm_network(ETHEREUM).genesis
    reader = Reader(genesis, 1_000)
    now = [1_050]
    pin = reader.pin_block

    def slow(selector):
        if selector == "latest":
            # The header was fresh when asked for and old by the time it arrived.
            now[0] = 1_200
        return pin(selector)

    reader.pin_block = slow
    with pytest.raises(DataError, match="head is 200 seconds old"):
        read_endpoints.verify_public(reader, clock=lambda: now[0])


def test_a_preview_on_a_public_endpoint_proves_the_endpoint_before_resolving(monkeypatch):
    steps = []

    class Opened:
        def __init__(self, chain, endpoint):
            steps.append(("open", endpoint))

        def __enter__(self):
            return self

        def __exit__(self, *arguments):
            steps.append("closed")

        def verify_chain(self):
            steps.append("chain")

        def pin_block(self, selector):
            return selector

    def stale(reader):
        steps.append("public")
        raise DataError("stale", "public endpoint head is 500 seconds old")

    monkeypatch.setattr("almanak.gateway.ethereum.EthereumReader", Opened)
    monkeypatch.setattr(read_endpoints, "verify_public", stale)
    monkeypatch.setattr(
        "almanak.connectors.lending_lookup.resolve",
        lambda *arguments: pytest.fail("resolved a market from an unproven endpoint"),
    )
    args = SimpleNamespace(
        public_rpc="publicnode",
        connector="aave_v4",
        max_gas_price_wei=5,
        max_transaction_fee=9,
        max_gas_limit=None,
    )
    request = SimpleNamespace(chain=ETHEREUM)
    wallet = {"account": "wallet", "address": "0x" + "ab" * 20}
    with pytest.raises(DataError, match="500 seconds old"):
        ax_actions.action_config(args, request, None, wallet, None, "https://public.example")
    assert steps == [("open", "https://public.example"), "chain", "public", "closed"]
