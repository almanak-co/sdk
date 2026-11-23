"""Named public endpoints: reads and action previews without a provider key.

Offline: no test here opens a connection. The endpoints themselves are exercised by
running the commands against them, which is not part of this suite.
"""

import re
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


def test_each_name_is_the_registered_domain_of_its_endpoints():
    hosts = []
    for name, chains in providers.PUBLIC_RPC.items():
        for url in chains.values():
            host = url.split("/")[2]
            hosts.append(host)
            # ethereum-rpc.publicnode.com is `publicnode`; mainnet.base.org is `base`.
            assert host.split(".")[-2] == name
    assert len(set(hosts)) == len(hosts)


def test_the_option_help_names_each_endpoint_and_both_uses(capsys):
    for command in (["ax", "balance"], ["ax", "supply"], ["ax", "recover"]):
        with pytest.raises(SystemExit):
            parse(*command, "--help")
        text = " ".join(capsys.readouterr().out.split())
        assert "a public endpoint that needs no key, for reads and direct actions" in text
        for name in providers.PUBLIC_RPC:
            assert f"{name}: eip155:" in text


def test_the_operating_guide_lists_the_endpoints_the_code_lists():
    guide = (Path(__file__).parents[1] / "docs/operating.md").read_text()
    section = guide.split("#### Public endpoints")[1].split("\n#### ")[0]
    rows = re.findall(r"^\| `([a-z0-9-]+)` \| [^|]+ \| (.+) \|$", section, flags=re.MULTILINE)
    documented = {name: re.findall(r"`(eip155:\d+)`", chains) for name, chains in rows}
    assert documented == {
        name: [str(chain) for chain in chains] for name, chains in providers.PUBLIC_RPC.items()
    }
    # The chains with one endpoint are named as such, and HyperEVM as having none.
    serving = [chain for chains in providers.PUBLIC_RPC.values() for chain in chains]
    single = {str(chain) for chain in serving if serving.count(chain) == 1}
    stated = section.split("have two endpoints.")[1].split("have one")[0]
    assert set(re.findall(r"`(eip155:\d+)`", stated)) == single
    assert "HyperEVM (`eip155:999`) has none" in section


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


AVALANCHE = ChainId("eip155:43114")


def record(provider, chain=AVALANCHE):
    """The part of a kept record's configuration that names its provider."""
    from almanak.models import Chain

    network = SimpleNamespace(provider=provider, chain=Chain(chain_id=chain, environment="mainnet"))
    return SimpleNamespace(production=SimpleNamespace(networks=[network]))


def public(name):
    return PublicProvider(kind="public_rpc", name=name)


@pytest.fixture
def proofs(monkeypatch):
    """The public proofs a recovery runs: each endpoint opened, proved and closed."""
    steps = []

    class Opened:
        def __init__(self, chain, endpoint, **options):
            steps.append(("open", str(chain.chain_id), endpoint))

        def __enter__(self):
            return self

        def __exit__(self, *arguments):
            steps.append("closed")

        def verify_chain(self):
            steps.append("chain")

    monkeypatch.setattr("almanak.gateway.ethereum.EthereumReader", Opened)
    monkeypatch.setattr(read_endpoints, "verify_public", lambda reader: steps.append("public"))
    return steps


@pytest.fixture
def env(tmp_path):
    path = tmp_path / ".env"
    path.write_text("ALCHEMY_API_KEY=k\n")
    return path


def test_recovering_a_keyed_record_needs_the_env_file(env, proofs):
    config = record(ax_actions.alchemy(ETHEREUM), ETHEREUM)
    with pytest.raises(ValueError, match="--env-file is required"):
        ax_actions.record_endpoint(config, None)
    assert ax_actions.record_endpoint(config, env) == "https://eth-mainnet.g.alchemy.com/v2/k"
    # A named public endpoint of the record's chain resolves it without the key.
    listed = providers.PUBLIC_RPC["publicnode"][ETHEREUM]
    assert ax_actions.record_endpoint(config, None, "publicnode") == listed
    assert proofs == [("open", "eip155:1", listed), "chain", "public", "closed"]


def test_a_record_on_a_listed_pair_resolves_through_its_own_endpoint(proofs):
    config = record(public("publicnode"))
    own = providers.PUBLIC_RPC["publicnode"][AVALANCHE]
    assert ax_actions.record_endpoint(config, None) == own
    # The adapter proves the chain and genesis there, as before a transport could be named.
    assert proofs == []


def test_a_named_transport_replaces_the_records_own_endpoint(monkeypatch, env, proofs):
    listed = {
        "one": {AVALANCHE: "https://one.example"},
        "two": {AVALANCHE: "https://two.example", ETHEREUM: "https://two.eth.example"},
    }
    monkeypatch.setattr(providers, "PUBLIC_RPC", listed)
    config = record(public("one"))
    assert ax_actions.record_endpoint(config, None, "two") == "https://two.example"
    assert proofs == [("open", "eip155:43114", "https://two.example"), "chain", "public", "closed"]
    # The env file names the keyed provider of the record's chain.
    keyed = ax_actions.record_endpoint(config, env)
    assert keyed == "https://avax-mainnet.g.alchemy.com/v2/k"
    # A name is looked up on the record's chain, never on another one it serves.
    with pytest.raises(ValueError, match="named three serves eip155:43114; choose from one, two"):
        ax_actions.record_endpoint(config, None, "three")


def test_a_record_on_a_delisted_pair_needs_a_named_transport(env, proofs):
    """`drpc` served Avalanche when its records were made and is no longer listed there."""
    assert AVALANCHE not in providers.PUBLIC_RPC["drpc"]
    config = record(public("drpc"))
    with pytest.raises(ValueError) as stranded:
        ax_actions.record_endpoint(config, None)
    assert str(stranded.value) == (
        "the public endpoint drpc this record's action ran on no longer serves "
        "eip155:43114; name a transport with --public-rpc or --env-file"
    )
    assert proofs == []
    listed = providers.PUBLIC_RPC["publicnode"][AVALANCHE]
    assert ax_actions.record_endpoint(config, None, "publicnode") == listed
    assert proofs == [("open", "eip155:43114", listed), "chain", "public", "closed"]
    assert ax_actions.record_endpoint(config, env).endswith("avax-mainnet.g.alchemy.com/v2/k")
    # A name that was never listed is stranded the same way.
    with pytest.raises(ValueError, match="gone this record's action ran on no longer serves"):
        ax_actions.record_endpoint(record(public("gone")), None)


def test_a_named_public_endpoint_that_fails_its_proof_resolves_nothing(monkeypatch, proofs):
    def stale(reader):
        raise DataError("stale", "public endpoint head is 500 seconds old")

    monkeypatch.setattr(read_endpoints, "verify_public", stale)
    with pytest.raises(DataError, match="500 seconds old"):
        ax_actions.record_endpoint(record(public("drpc")), None, "publicnode")
    assert proofs[-1] == "closed"


def test_the_recovery_proof_re_sends_a_read_that_failed_for_a_moment(monkeypatch):
    """The reader `ax recover` proves a named endpoint with has the execution budget."""
    import json

    import httpx

    from almanak.gateway import ethereum
    from almanak.models import Chain

    sent, waits, budgets = [], [], []

    def handle(request):
        body = json.loads(request.content)
        sent.append(body["method"])
        if len(sent) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": "0x1"})

    real = ethereum.EthereumReader

    def opened(chain, endpoint, **options):
        budgets.append(options.get("retry_budget"))
        client = httpx.Client(transport=httpx.MockTransport(handle))
        return real(chain, endpoint, client=client, **options)

    monkeypatch.setattr(ethereum, "EthereumReader", opened)
    monkeypatch.setattr(ethereum, "sleep", waits.append)
    monkeypatch.setattr(read_endpoints, "verify_public", lambda reader: None)
    chain = Chain(chain_id=ETHEREUM, environment="mainnet")
    assert ax_actions.proven_public(chain, "https://public.example") == "https://public.example"
    assert budgets == [ethereum.EXECUTION_RETRY_BUDGET]
    # Only reads go through this reader, and the failed one is sent again.
    assert sent == ["eth_chainId", "eth_chainId"] and waits == [ethereum.BUSY_BACKOFF]


def test_recover_takes_one_transport_and_resolves_the_record_through_it(monkeypatch, capsys):
    named = parse("ax", "recover", "--record", "kept", "--public-rpc", "publicnode")
    assert (named.public_rpc, named.env_file) == ("publicnode", None)
    keyed = parse("ax", "recover", "--record", "kept", "--env-file", ".env")
    assert (keyed.public_rpc, keyed.env_file) == (None, Path(".env"))
    own = parse("ax", "recover", "--record", "kept")
    assert (own.public_rpc, own.env_file) == (None, None)
    for arguments in (
        ["--public-rpc", "publicnode", "--env-file", ".env"],
        ["--public-rpc", "unlisted"],
    ):
        with pytest.raises(SystemExit):
            parse("ax", "recover", "--record", "kept", *arguments)
    capsys.readouterr()
    seen = {}

    def endpoint(config, env_file, public_rpc):
        seen.update(config=config, env_file=env_file, public_rpc=public_rpc)
        return "https://named.example"

    def recover(record, backend_for):
        seen["backend"] = backend_for("stored")
        return {"resolved": True}

    monkeypatch.setattr(ax_actions, "record_endpoint", endpoint)
    monkeypatch.setattr(ax_actions, "production_backend", lambda *arguments: arguments)
    monkeypatch.setattr(ax_actions, "recover", recover)
    assert ax_actions.recover_command(named) == 0
    assert seen == {
        "config": "stored",
        "env_file": None,
        "public_rpc": "publicnode",
        "backend": ("stored", "https://named.example", None),
    }
    capsys.readouterr()


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
