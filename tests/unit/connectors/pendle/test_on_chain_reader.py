"""Unit tests for PendleOnChainReader with mocked RPC responses.

The PT-to-asset rate is read from the per-chain PendlePYLpOracle via the 2-arg
``getPtToAssetRate(market, duration)`` TWAP call, gated on oracle readiness
(``getOracleState``). See VIB-5333: the legacy RouterStatic spot read was dead on
Arbitrum (no code) and valued every Arbitrum PT at $0.
"""

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors.pendle.on_chain_reader import (
    PT_ORACLE_ADDRESSES,
    ROUTER_STATIC_ADDRESSES,
    PendleOnChainError,
    PendleOnChainReader,
)

# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def mock_web3():
    """Create a mock Web3 instance."""
    mock = MagicMock()
    mock.to_checksum_address = lambda addr: addr
    mock.eth.contract.return_value = MagicMock()
    return mock


@pytest.fixture
def reader(mock_web3):
    """Create a PendleOnChainReader with mocked Web3 (ethereum)."""
    with patch("web3.Web3") as MockWeb3:
        MockWeb3.return_value = mock_web3
        MockWeb3.HTTPProvider = MagicMock()
        r = PendleOnChainReader(rpc_url="http://localhost:8545", chain="ethereum")
        r._cache.clear()
        return r


@pytest.fixture
def arb_reader(mock_web3):
    """Create a PendleOnChainReader with mocked Web3 (arbitrum — no RouterStatic)."""
    with patch("web3.Web3") as MockWeb3:
        MockWeb3.return_value = mock_web3
        MockWeb3.HTTPProvider = MagicMock()
        r = PendleOnChainReader(rpc_url="http://localhost:8545", chain="arbitrum")
        r._cache.clear()
        return r


def _set_oracle_ready(r, *, increase_required=False, cardinality=901, oldest_ok=True):
    """Stub the direct-mode PT oracle ``getOracleState`` return."""
    r.pt_oracle.functions.getOracleState.return_value.call.return_value = (
        increase_required,
        cardinality,
        oldest_ok,
    )


# =========================================================================
# Initialization Tests
# =========================================================================


class TestOnChainReaderInit:
    """Test reader initialization."""

    def test_valid_chain(self):
        with patch("web3.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="ethereum")
        assert reader.chain == "ethereum"
        assert reader.pt_oracle_address == PT_ORACLE_ADDRESSES["ethereum"]
        assert reader.router_static_address == ROUTER_STATIC_ADDRESSES["ethereum"]

    def test_arbitrum_chain(self):
        with patch("web3.Web3"):
            reader = PendleOnChainReader(rpc_url="http://localhost:8545", chain="arbitrum")
        assert reader.chain == "arbitrum"
        assert reader.pt_oracle_address == PT_ORACLE_ADDRESSES["arbitrum"]
        # Pendle decommissioned RouterStatic on Arbitrum — no address registered.
        assert reader.router_static_address is None

    def test_unsupported_chain_raises(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            with patch("web3.Web3"):
                PendleOnChainReader(rpc_url="http://localhost:8545", chain="polygon")


# =========================================================================
# Dead-address regression pins (VIB-5333)
# =========================================================================


class TestNoDeadAddresses:
    """Pin that no chain maps the PT rate read to a code-less / wrong address."""

    # 0xADB09F65… has NO code on Arbitrum (Pendle decommissioned RouterStatic).
    DEAD_ARB_ROUTER_STATIC = "0xadb09f65bd90d19e3148db7b340e4b65d6063a90"

    def test_dead_router_static_absent_everywhere(self):
        addrs = {v.lower() for v in PT_ORACLE_ADDRESSES.values()}
        addrs |= {v.lower() for v in ROUTER_STATIC_ADDRESSES.values()}
        assert self.DEAD_ARB_ROUTER_STATIC not in addrs

    def test_arbitrum_has_no_router_static(self):
        assert "arbitrum" not in ROUTER_STATIC_ADDRESSES

    def test_addresses_arbitrum_router_static_removed(self):
        from almanak.connectors.pendle.addresses import PENDLE

        assert "router_static" not in PENDLE["arbitrum"]

    def test_arbitrum_rate_uses_pt_oracle(self):
        from almanak.connectors.pendle.addresses import PENDLE

        assert PT_ORACLE_ADDRESSES["arbitrum"].lower() == PENDLE["arbitrum"]["pt_oracle"].lower()
        assert PT_ORACLE_ADDRESSES["arbitrum"].lower() == "0x1fd95db7b7c0067de8d45c0cb35d59796adfd187"

    def test_pt_oracle_addresses_single_source_of_truth(self):
        from almanak.connectors.pendle.addresses import PENDLE

        for chain, addr in PT_ORACLE_ADDRESSES.items():
            assert addr == PENDLE[chain]["pt_oracle"]


# =========================================================================
# PT Rate Tests (direct mode)
# =========================================================================


class TestGetPtToAssetRate:
    """Test get_pt_to_asset_rate method (direct/web3 mode)."""

    def test_returns_normalized_rate(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        rate = reader.get_pt_to_asset_rate("0xmarket")
        assert rate == Decimal("0.97")

    def test_rate_of_one(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 10**18
        rate = reader.get_pt_to_asset_rate("0xmarket")
        assert rate == Decimal("1")

    def test_arbitrum_rate_uses_pt_oracle(self, arb_reader):
        """The Arbitrum money path resolves via the PT oracle, not RouterStatic."""
        _set_oracle_ready(arb_reader)
        arb_reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 999751264450592760
        rate = arb_reader.get_pt_to_asset_rate("0xmarket")
        assert rate == Decimal("999751264450592760") / Decimal("1000000000000000000")
        # Duration argument is the fixed non-zero TWAP window.
        called_args = arb_reader.pt_oracle.functions.getPtToAssetRate.call_args.args
        assert called_args[1] == 900

    def test_increase_cardinality_required_still_returns_rate(self, reader):
        """increaseCardinalityRequired=True is advisory only — the rate is still valid.

        Verified against the live Ethereum production market, which reports this
        flag True yet returns a correct rate. Gating on it would zero out an
        otherwise-valid PT valuation.
        """
        _set_oracle_ready(reader, increase_required=True)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 993888507436938092
        rate = reader.get_pt_to_asset_rate("0xmarket")
        assert rate > 0

    def test_oracle_not_ready_raises_unmeasured(self, reader):
        """oldestObservationSatisfied=False → UNMEASURED (raise), never fabricated."""
        _set_oracle_ready(reader, oldest_ok=False)
        with pytest.raises(PendleOnChainError, match="not ready"):
            reader.get_pt_to_asset_rate("0xmarket")
        # The rate call must NOT have been attempted.
        assert reader.pt_oracle.functions.getPtToAssetRate.return_value.call.call_count == 0

    def test_caches_result(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        rate1 = reader.get_pt_to_asset_rate("0xmarket")
        rate2 = reader.get_pt_to_asset_rate("0xmarket")
        assert rate1 == rate2
        # Should only call once due to caching.
        assert reader.pt_oracle.functions.getPtToAssetRate.return_value.call.call_count == 1

    def test_rpc_error_raises(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.side_effect = Exception("RPC error")
        with pytest.raises(PendleOnChainError, match="getPtToAssetRate failed"):
            reader.get_pt_to_asset_rate("0xmarket")


# =========================================================================
# Implied APY Tests (direct mode)
# =========================================================================


class TestGetImpliedApy:
    """Test get_implied_apy method."""

    def test_returns_normalized_apy(self, reader):
        # 5% APY in 1e18 scale
        reader.router_static.functions.getImpliedApy.return_value.call.return_value = 50000000000000000
        apy = reader.get_implied_apy("0xmarket")
        assert apy == Decimal("0.05")

    def test_rpc_error_raises(self, reader):
        reader.router_static.functions.getImpliedApy.return_value.call.side_effect = Exception("timeout")
        with pytest.raises(PendleOnChainError, match="getImpliedApy failed"):
            reader.get_implied_apy("0xmarket")

    def test_unavailable_without_router_static(self, arb_reader):
        """On a chain with no RouterStatic (Arbitrum), implied APY degrades cleanly."""
        assert arb_reader.router_static is None
        with pytest.raises(PendleOnChainError, match="getImpliedApy unavailable"):
            arb_reader.get_implied_apy("0xmarket")


# =========================================================================
# Market Expiry Tests
# =========================================================================


class TestIsMarketExpired:
    """Test is_market_expired method."""

    def test_expired_market(self, reader):
        past_expiry = int(time.time()) - 86400  # Yesterday
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = past_expiry
        reader.web3.eth.contract.return_value = mock_contract

        assert reader.is_market_expired("0xmarket") is True

    def test_active_market(self, reader):
        future_expiry = int(time.time()) + 86400 * 365  # 1 year from now
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = future_expiry
        reader.web3.eth.contract.return_value = mock_contract

        assert reader.is_market_expired("0xmarket") is False

    def test_rpc_error_raises(self, reader):
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.side_effect = Exception("error")
        reader.web3.eth.contract.return_value = mock_contract

        with pytest.raises(PendleOnChainError, match="expiry\\(\\) failed"):
            reader.is_market_expired("0xmarket")

    def test_arbitrum_succeeds_without_router_static(self, arb_reader):
        """expiry() reads the MARKET contract — must work on Arbitrum (router_static None).

        Regression: a stale ``and self.router_static is not None`` in the direct-mode
        assert raised AssertionError on Arbitrum even though expiry() never touches
        RouterStatic.
        """
        assert arb_reader.router_static is None
        future_expiry = int(time.time()) + 86400 * 200
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = future_expiry
        arb_reader.web3.eth.contract.return_value = mock_contract

        assert arb_reader.is_market_expired("0xmarket") is False


# =========================================================================
# Market Tokens Tests
# =========================================================================


class TestGetMarketTokens:
    """Test get_market_tokens method (reads market contract no-arg readTokens())."""

    def test_returns_token_addresses(self, reader):
        mock_contract = MagicMock()
        mock_contract.functions.readTokens.return_value.call.return_value = (
            "0xSY_ADDR",
            "0xPT_ADDR",
            "0xYT_ADDR",
        )
        reader.web3.eth.contract.return_value = mock_contract
        tokens = reader.get_market_tokens("0xmarket")
        assert tokens["sy"] == "0xsy_addr"
        assert tokens["pt"] == "0xpt_addr"
        assert tokens["yt"] == "0xyt_addr"

    def test_arbitrum_tokens_no_router_static(self, arb_reader):
        """readTokens works on Arbitrum even though RouterStatic is gone."""
        mock_contract = MagicMock()
        mock_contract.functions.readTokens.return_value.call.return_value = (
            "0xAA",
            "0xBB",
            "0xCC",
        )
        arb_reader.web3.eth.contract.return_value = mock_contract
        tokens = arb_reader.get_market_tokens("0xmarket")
        assert tokens == {"sy": "0xaa", "pt": "0xbb", "yt": "0xcc"}


# =========================================================================
# PT Output Estimation Tests
# =========================================================================


class TestEstimatePtOutput:
    """Test estimate_pt_output method."""

    def test_basic_estimate(self, reader):
        # Rate = 0.95 (PT is at 5% discount)
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 950000000000000000
        # For 1 unit input, PT output = 1 / 0.95 > input (discount)
        output = reader.estimate_pt_output("0xmarket", 1000000000000000000)
        assert output > 1000000000000000000

    def test_invalid_rate_raises(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 0
        with pytest.raises(PendleOnChainError, match="Invalid PT rate"):
            reader.estimate_pt_output("0xmarket", 1000000)


# =========================================================================
# Cache Tests
# =========================================================================


class TestOnChainCache:
    """Test cache behavior."""

    def test_clear_cache(self, reader):
        _set_oracle_ready(reader)
        reader.pt_oracle.functions.getPtToAssetRate.return_value.call.return_value = 970000000000000000
        reader.get_pt_to_asset_rate("0xmarket")
        reader.clear_cache()
        reader.get_pt_to_asset_rate("0xmarket")
        assert reader.pt_oracle.functions.getPtToAssetRate.return_value.call.call_count == 2


# =========================================================================
# Gateway Mode Helpers
# =========================================================================


def _mock_rpc_response(result_hex: str, success: bool = True, error: str = ""):
    """Create a mock gateway RPC response."""
    resp = MagicMock()
    resp.success = success
    resp.result = json.dumps(result_hex)
    resp.error = error
    return resp


def _oracle_state_hex(increase_required: bool, cardinality: int, oldest_ok: bool) -> str:
    """Encode a ``getOracleState`` (bool, uint16, bool) return as 3 ABI words."""
    return "0x" + format(int(increase_required), "064x") + format(cardinality, "064x") + format(int(oldest_ok), "064x")


@pytest.fixture
def gateway_client():
    """Create a mock GatewayClient for gateway mode tests."""
    client = MagicMock()
    client.is_connected = True
    return client


@pytest.fixture
def gw_reader(gateway_client):
    """Create a PendleOnChainReader in gateway mode (ethereum)."""
    r = PendleOnChainReader(gateway_client=gateway_client, chain="ethereum")
    r.clear_cache()
    return r


@pytest.fixture
def gw_arb_reader(gateway_client):
    """Create a PendleOnChainReader in gateway mode (arbitrum)."""
    r = PendleOnChainReader(gateway_client=gateway_client, chain="arbitrum")
    r.clear_cache()
    return r


# =========================================================================
# Gateway Mode Init Tests
# =========================================================================


class TestGatewayModeInit:
    """Test reader initialization in gateway mode."""

    def test_gateway_mode_init(self, gateway_client):
        reader = PendleOnChainReader(gateway_client=gateway_client, chain="ethereum")
        assert reader.chain == "ethereum"
        assert reader.web3 is None
        assert reader.pt_oracle is None
        assert reader.router_static is None

    def test_gateway_mode_arbitrum(self, gateway_client):
        reader = PendleOnChainReader(gateway_client=gateway_client, chain="arbitrum")
        assert reader.chain == "arbitrum"
        assert reader.pt_oracle_address == PT_ORACLE_ADDRESSES["arbitrum"]
        assert reader.router_static_address is None

    def test_no_client_no_url_raises(self):
        with pytest.raises(ValueError, match="Either rpc_url or gateway_client"):
            PendleOnChainReader(chain="ethereum")

    def test_unsupported_chain_gateway_raises(self, gateway_client):
        with pytest.raises(ValueError, match="Unsupported chain"):
            PendleOnChainReader(gateway_client=gateway_client, chain="polygon")


# =========================================================================
# Gateway Mode PT Rate Tests
# =========================================================================


class TestGatewayPtRate:
    """Test get_pt_to_asset_rate via gateway mode (oracle-state read then rate read)."""

    def test_returns_normalized_rate(self, gw_reader, gateway_client):
        rate_hex = hex(970000000000000000)
        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(False, 901, True)),
            _mock_rpc_response(rate_hex),
        ]
        rate = gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate == Decimal("0.97")

    def test_arbitrum_rate(self, gw_arb_reader, gateway_client):
        rate_hex = hex(999751264450592760)
        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(False, 901, True)),
            _mock_rpc_response(rate_hex),
        ]
        rate = gw_arb_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate == Decimal("999751264450592760") / Decimal("1000000000000000000")

    def test_increase_cardinality_required_still_returns(self, gw_reader, gateway_client):
        rate_hex = hex(993888507436938092)
        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(True, 91, True)),
            _mock_rpc_response(rate_hex),
        ]
        rate = gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate > 0

    def test_oracle_not_ready_raises_unmeasured(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(False, 901, False)),
        ]
        with pytest.raises(PendleOnChainError, match="not ready"):
            gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        # Only the oracle-state read happened; the rate read was skipped.
        assert gateway_client.rpc.Call.call_count == 1

    def test_caches_result(self, gw_reader, gateway_client):
        rate_hex = hex(970000000000000000)
        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(False, 901, True)),
            _mock_rpc_response(rate_hex),
        ]
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        gw_reader.get_pt_to_asset_rate(addr)
        gw_reader.get_pt_to_asset_rate(addr)
        # 2 calls on the first read (state + rate); second read is cached.
        assert gateway_client.rpc.Call.call_count == 2

    def test_rpc_failure_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.return_value = _mock_rpc_response("", success=False, error="rpc error")
        with pytest.raises(PendleOnChainError, match="Gateway RPC call error"):
            gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")

    def test_exception_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.side_effect = Exception("connection refused")
        with pytest.raises(PendleOnChainError, match="Gateway RPC call failed"):
            gw_reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")


# =========================================================================
# Gateway Mode Implied APY Tests
# =========================================================================


class TestGatewayImpliedApy:
    """Test get_implied_apy via gateway mode."""

    def test_returns_normalized_apy(self, gw_reader, gateway_client):
        apy_hex = hex(50000000000000000)
        gateway_client.rpc.Call.return_value = _mock_rpc_response(apy_hex)
        apy = gw_reader.get_implied_apy("0x1234567890abcdef1234567890abcdef12345678")
        assert apy == Decimal("0.05")

    def test_unavailable_without_router_static(self, gw_arb_reader, gateway_client):
        with pytest.raises(PendleOnChainError, match="getImpliedApy unavailable"):
            gw_arb_reader.get_implied_apy("0x1234567890abcdef1234567890abcdef12345678")
        # No RPC call is even attempted.
        assert gateway_client.rpc.Call.call_count == 0


# =========================================================================
# Gateway Mode Market Expiry Tests
# =========================================================================


class TestGatewayMarketExpiry:
    """Test is_market_expired via gateway mode."""

    def test_expired_market(self, gw_reader, gateway_client):
        past_expiry = int(time.time()) - 86400
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(past_expiry))

        assert gw_reader.is_market_expired("0x1234567890abcdef1234567890abcdef12345678") is True

    def test_active_market(self, gw_reader, gateway_client):
        future_expiry = int(time.time()) + 86400 * 365
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(future_expiry))

        assert gw_reader.is_market_expired("0x1234567890abcdef1234567890abcdef12345678") is False


# =========================================================================
# Gateway Mode Market Tokens Tests
# =========================================================================


class TestGatewayMarketTokens:
    """Test get_market_tokens via gateway mode (no-arg readTokens on the market)."""

    def test_returns_token_addresses(self, gw_reader, gateway_client):
        sy_addr = "0000000000000000000000001111111111111111111111111111111111111111"
        pt_addr = "0000000000000000000000002222222222222222222222222222222222222222"
        yt_addr = "0000000000000000000000003333333333333333333333333333333333333333"
        result_hex = "0x" + sy_addr + pt_addr + yt_addr
        gateway_client.rpc.Call.return_value = _mock_rpc_response(result_hex)

        tokens = gw_reader.get_market_tokens("0x1234567890abcdef1234567890abcdef12345678")
        assert tokens["sy"] == "0x1111111111111111111111111111111111111111"
        assert tokens["pt"] == "0x2222222222222222222222222222222222222222"
        assert tokens["yt"] == "0x3333333333333333333333333333333333333333"

    def test_short_response_raises(self, gw_reader, gateway_client):
        gateway_client.rpc.Call.return_value = _mock_rpc_response("0x" + "00" * 32)

        with pytest.raises(PendleOnChainError, match="unexpected data length"):
            gw_reader.get_market_tokens("0x1234567890abcdef1234567890abcdef12345678")


# =========================================================================
# Days-to-Maturity Tests
# =========================================================================


class TestGetDaysToMaturity:
    """Tests for PendleOnChainReader.get_days_to_maturity().

    Covers three branches:
    1. Active market → positive integer days.
    2. Expired market → clamped to 0.
    3. RPC/exception → returns None (never raises).
    """

    def test_active_market_returns_positive_days(self, reader):
        """Future expiry returns a positive day count."""
        future_expiry = int(time.time()) + 180 * 86400  # 180 days
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = future_expiry
        reader.web3.eth.contract.return_value = mock_contract

        days = reader.get_days_to_maturity("0xmarket")
        assert days is not None
        assert 175 <= days <= 185  # allow a few seconds of clock drift

    def test_expired_market_returns_zero(self, reader):
        """Past expiry is clamped to 0, not a negative number."""
        past_expiry = int(time.time()) - 86400  # expired yesterday
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.return_value = past_expiry
        reader.web3.eth.contract.return_value = mock_contract

        days = reader.get_days_to_maturity("0xmarket")
        assert days == 0

    def test_rpc_failure_returns_none(self, reader):
        """Any exception is swallowed and None is returned (never raises)."""
        mock_contract = MagicMock()
        mock_contract.functions.expiry.return_value.call.side_effect = Exception("RPC error")
        reader.web3.eth.contract.return_value = mock_contract

        days = reader.get_days_to_maturity("0xmarket")
        assert days is None


class TestGatewayGetDaysToMaturity:
    """Tests for get_days_to_maturity() in gateway mode."""

    def test_active_market_returns_positive_days(self, gw_reader, gateway_client):
        """Future expiry via gateway → positive day count."""
        future_expiry = int(time.time()) + 90 * 86400  # 90 days
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(future_expiry))

        days = gw_reader.get_days_to_maturity("0x1234567890abcdef1234567890abcdef12345678")
        assert days is not None
        assert 85 <= days <= 95

    def test_expired_market_returns_zero(self, gw_reader, gateway_client):
        """Past expiry via gateway → 0."""
        past_expiry = int(time.time()) - 86400
        gateway_client.rpc.Call.return_value = _mock_rpc_response(hex(past_expiry))

        days = gw_reader.get_days_to_maturity("0x1234567890abcdef1234567890abcdef12345678")
        assert days == 0

    def test_gateway_failure_returns_none(self, gw_reader, gateway_client):
        """Gateway RPC error is swallowed and None is returned."""
        gateway_client.rpc.Call.side_effect = Exception("connection lost")

        days = gw_reader.get_days_to_maturity("0x1234567890abcdef1234567890abcdef12345678")
        assert days is None


# =========================================================================
# VIB-5305 — connected-gateway egress-unreachability guards
# =========================================================================


class TestConnectedGatewayForcesRpcUrlNone:
    """Pin the LOAD-BEARING link: a connected gateway forces ``rpc_url=None``.

    The two Pendle ``# vib-2986-exempt`` HTTPProvider fallbacks (this reader and
    ``sdk.py``) fire only when ``gateway_client is None``. The hosted-container
    safety claim therefore reduces to a code invariant: when a connected gateway
    client is present, the compiler resolves ``rpc_url`` to ``None`` so the gateway
    branch (not HTTPProvider) is taken.

    (Premise, asserted only as documentation: a hosted runner ALWAYS wires a
    connected gateway client — strategy containers hold no RPC credentials, see
    blueprint 20 §Gateway boundary. These tests prove the *code link* downstream of
    that premise; a future edit that makes ``_get_chain_rpc_url`` return a URL while
    a gateway is connected — re-enabling egress — fails here.)
    """

    def test_framework_compiler_get_chain_rpc_url_is_none_when_gateway_connected(self):
        """``IntentCompiler._get_chain_rpc_url`` returns None when a gateway is connected.

        Even with a non-empty ``self.rpc_url`` set, a connected gateway must win.
        """
        from almanak.framework.intents.compiler import IntentCompiler

        compiler = IntentCompiler.__new__(IntentCompiler)
        compiler._gateway_client = MagicMock(is_connected=True)
        compiler.rpc_url = "http://should-not-be-used:8545"
        compiler.chain = "ethereum"

        assert compiler._get_chain_rpc_url() is None

    def test_pendle_compiler_forces_rpc_url_none_when_gateway_connected(self):
        """``_resolve_pendle_adapter_inputs`` forces ``rpc_url=None`` with a connected gateway.

        Even if ``_get_chain_rpc_url`` were to (wrongly) return a URL, the connected
        gateway path must zero it so the adapter/SDK take the gateway branch.
        """
        from almanak.connectors.pendle.compiler import _resolve_pendle_adapter_inputs

        compiler = MagicMock()
        compiler._gateway_client = MagicMock(is_connected=True)
        compiler._get_chain_rpc_url.return_value = "http://should-not-be-used:8545"

        result = _resolve_pendle_adapter_inputs(compiler, "intent-1")
        assert isinstance(result, tuple), f"expected (gateway_client, rpc_url), got {result!r}"
        gateway_client, rpc_url = result
        assert rpc_url is None
        assert gateway_client is compiler._gateway_client


class TestConnectedGatewayBuildsNoHttpProvider:
    """Lock in that the connected-gateway build path never opens a direct socket.

    Downstream of the ``rpc_url=None`` decision above: the reader, the adapter, and
    the PT-health registry must all build in gateway mode (``GatewayWeb3Provider`` /
    gateway-mode reader) without ever instantiating an HTTPProvider. Companion
    evidence: ``tests/reports/pendle_egress_trace_vib5305.md``.
    """

    def test_gateway_mode_reader_instantiates_no_httpprovider(self, gateway_client, monkeypatch):
        """Gateway-mode reader build + PT read must never construct an HTTPProvider."""
        import web3

        def _boom(*_args, **_kwargs):  # pragma: no cover - only runs on regression
            raise AssertionError("Web3.HTTPProvider instantiated on the gateway read path")

        monkeypatch.setattr(web3.Web3, "HTTPProvider", _boom)

        reader = PendleOnChainReader(gateway_client=gateway_client, chain="ethereum")
        assert reader.web3 is None  # gateway mode holds no web3 instance

        gateway_client.rpc.Call.side_effect = [
            _mock_rpc_response(_oracle_state_hex(False, 901, True)),
            _mock_rpc_response(hex(950000000000000000)),
        ]
        rate = reader.get_pt_to_asset_rate("0x1234567890abcdef1234567890abcdef12345678")
        assert rate == Decimal("0.95")

    def test_adapter_with_connected_gateway_builds_gateway_mode_reader(self, gateway_client, monkeypatch):
        """The adapter, built as the compiler builds it with a connected gateway, is gateway-mode.

        Mirrors ``pendle/compiler._resolve_pendle_adapter_inputs``: a connected
        gateway_client forces ``rpc_url=None``. The adapter must then build BOTH
        its SDK (``GatewayWeb3Provider``) and its on-chain reader (gateway mode)
        without ever instantiating an HTTPProvider.
        """
        import web3

        from almanak.connectors.pendle.adapter import PendleAdapter

        def _boom(*_args, **_kwargs):  # pragma: no cover - only runs on regression
            raise AssertionError("Web3.HTTPProvider instantiated on the strategy-container path")

        monkeypatch.setattr(web3.Web3, "HTTPProvider", _boom)

        adapter = PendleAdapter(rpc_url=None, chain="ethereum", gateway_client=gateway_client)
        reader = adapter._get_on_chain_reader()
        assert reader.web3 is None  # gateway mode
        assert type(adapter.sdk.web3.provider).__name__ == "GatewayWeb3Provider"

    def test_position_health_registry_builds_gateway_mode_reader(self, gateway_client, monkeypatch):
        """PT-health reader build (``position_health`` path) is gateway-mode with a connected gateway.

        ``position_health`` builds via ``PRINCIPAL_TOKEN_MARKET_READ_REGISTRY.build_reader``
        passing ``gateway_client`` when present (else ``rpc_url``). With a connected
        gateway it must yield a gateway-mode reader and never an HTTPProvider.
        """
        import web3

        from almanak.connectors._strategy_principal_token_market_reader_registry import (
            PRINCIPAL_TOKEN_MARKET_READ_REGISTRY,
        )

        def _boom(*_args, **_kwargs):  # pragma: no cover - only runs on regression
            raise AssertionError("Web3.HTTPProvider instantiated on the PT-health path")

        monkeypatch.setattr(web3.Web3, "HTTPProvider", _boom)

        reader = PRINCIPAL_TOKEN_MARKET_READ_REGISTRY.build_reader(
            "pendle", chain="ethereum", gateway_client=gateway_client
        )
        assert reader.web3 is None  # gateway mode
