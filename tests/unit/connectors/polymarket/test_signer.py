"""Tests for Polymarket signer helpers (local + remote).

The remote-signing path POSTs the EIP-712 digest to the Almanak Signer Service
``/sign/hash`` endpoint and reassembles the response into ``0x<r><s><v>``. These
tests use ``MagicMock(spec=httpx.Client)`` to fake the HTTP layer — same pattern
already used in ``test_clob_client.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest
from eth_account import Account
from eth_account.messages import encode_typed_data

from almanak.framework.connectors.polymarket.exceptions import PolymarketSignatureError
from almanak.framework.connectors.polymarket.signer import (
    SIGN_HASH_PATH,
    Signer,
    build_clob_auth_typed_data,
    make_local_signer,
    make_remote_signer,
    sign_typed_data_local,
    sign_typed_data_remote,
    signer_from_env,
)


@pytest.fixture
def test_account():
    return Account.from_key("0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")


@pytest.fixture
def sample_typed_data(test_account):
    return build_clob_auth_typed_data(
        wallet_address=test_account.address,
        timestamp="1700000000",
        nonce=0,
    )


def _make_mock_response(status_code: int, json_body: dict | None = None, text_body: str = "") -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = json_body if json_body is not None else {}
    response.text = text_body or (str(json_body) if json_body else "")
    return response


# =============================================================================
# build_clob_auth_typed_data
# =============================================================================


class TestBuildClobAuthTypedData:
    def test_typed_data_shape(self):
        td = build_clob_auth_typed_data("0xabc", "1700000000", 0)
        assert td["primaryType"] == "ClobAuth"
        assert td["message"]["address"] == "0xabc"
        assert td["message"]["timestamp"] == "1700000000"
        assert td["message"]["nonce"] == 0
        assert "types" in td
        assert "domain" in td
        assert "ClobAuth" in td["types"]

    def test_typed_data_is_encodable(self, test_account):
        td = build_clob_auth_typed_data(test_account.address, "1700000000", 0)
        signable = encode_typed_data(full_message=td)
        assert len(signable.body) == 32


# =============================================================================
# sign_typed_data_local
# =============================================================================


class TestSignTypedDataLocal:
    def test_signature_recovers_to_signer(self, sample_typed_data, test_account):
        sig_hex = sign_typed_data_local(sample_typed_data, test_account.key.hex())
        signable = encode_typed_data(full_message=sample_typed_data)
        recovered = Account.recover_message(signable, signature=sig_hex)
        assert recovered == test_account.address

    def test_signature_is_0x_prefixed_65_bytes(self, sample_typed_data, test_account):
        sig_hex = sign_typed_data_local(sample_typed_data, test_account.key.hex())
        assert sig_hex.startswith("0x")
        assert len(sig_hex) == 2 + 65 * 2  # 0x + 130 hex chars


# =============================================================================
# sign_typed_data_remote — happy path
# =============================================================================


class TestSignTypedDataRemoteHappyPath:
    def test_request_shape(self, sample_typed_data):
        """POSTs to /sign/hash with eoa_address, digest hex, EVM signing_type, JWT bearer."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {
                        "_type": "signature",
                        "r": "0x" + "11" * 32,
                        "s": "0x" + "22" * 32,
                        "v": 27,
                        "networkV": None,
                    }
                ]
            },
        )

        sign_typed_data_remote(
            sample_typed_data,
            eoa_address="0xabc0000000000000000000000000000000000def",
            signer_service_url="https://signer.example.com",
            signer_service_jwt="jwt-token",
            http_client=client,
        )

        assert client.post.call_count == 1
        call = client.post.call_args
        assert call.args[0] == f"https://signer.example.com{SIGN_HASH_PATH}"
        body = call.kwargs["json"]
        assert body["eoa_address"] == "0xabc0000000000000000000000000000000000def"
        assert body["signing_type"] == "EVM"
        assert len(body["transaction_payload"]) == 1
        digest_hex = body["transaction_payload"][0]
        assert digest_hex.startswith("0x")
        assert len(digest_hex) == 2 + 64  # 32-byte digest
        # The remote payload must carry the FULL EIP-712 digest — the same
        # value ``Account.sign_message(signable)`` would sign locally — not
        # just the struct hash (``signable.body``). Mismatch here would mean
        # local-signed and remote-signed flows produce different signatures
        # for the same typed data and Polymarket's verifier recovers a
        # different address.
        from eth_utils import keccak

        signable = encode_typed_data(full_message=sample_typed_data)
        expected_digest = keccak(b"\x19" + signable.version + signable.header + signable.body).hex()
        assert digest_hex == "0x" + expected_digest
        headers = call.kwargs["headers"]
        assert headers["Authorization"] == "Bearer jwt-token"
        assert headers["Content-Type"] == "application/json"

    def test_strips_trailing_slash_from_url(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": 27, "networkV": None}
                ]
            },
        )
        sign_typed_data_remote(
            sample_typed_data,
            "0xabc",
            "https://signer.example.com/",  # trailing slash
            "jwt",
            http_client=client,
        )
        url = client.post.call_args.args[0]
        assert url == "https://signer.example.com/sign/hash"

    def test_reassembles_signature_with_0x_prefixed_rs(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {
                        "_type": "signature",
                        "r": "0x" + "ab" * 32,
                        "s": "0x" + "cd" * 32,
                        "v": 28,
                        "networkV": None,
                    }
                ]
            },
        )
        sig = sign_typed_data_remote(
            sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client
        )
        assert sig == "0x" + "ab" * 32 + "cd" * 32 + "1c"  # v=28 → 0x1c
        assert len(sig) == 2 + 65 * 2

    def test_reassembles_signature_with_unprefixed_rs(self, sample_typed_data):
        """Tolerates Signature.toJSON() variants that drop the 0x prefix on r/s."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "ab" * 32, "s": "cd" * 32, "v": 27, "networkV": None}
                ]
            },
        )
        sig = sign_typed_data_remote(
            sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client
        )
        assert sig == "0x" + "ab" * 32 + "cd" * 32 + "1b"  # v=27 → 0x1b

    def test_handles_non_null_network_v(self, sample_typed_data):
        """ethers v6 sets networkV when EIP-155 chain id is mixed in; we ignore it and use raw v."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": 27, "networkV": "0x1"}
                ]
            },
        )
        sig = sign_typed_data_remote(
            sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client
        )
        # networkV ignored; we use v=27 → "1b"
        assert sig.endswith("1b")

    def test_accepts_flat_hex_response(self, sample_typed_data):
        """Defensive: some signer-service builds may flatten to a hex string in signed_transactions[0]."""
        client = MagicMock(spec=httpx.Client)
        flat_sig = "0x" + "ab" * 32 + "cd" * 32 + "1b"
        client.post.return_value = _make_mock_response(200, {"signed_transactions": [flat_sig]})
        sig = sign_typed_data_remote(
            sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client
        )
        assert sig == flat_sig


# =============================================================================
# sign_typed_data_remote — error paths
# =============================================================================


class TestSignTypedDataRemoteErrors:
    def test_401_raises_with_jwt_hint(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(401, text_body="Unauthorized")
        with pytest.raises(PolymarketSignatureError, match="JWT may be expired"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    def test_500_raises_with_status_code(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(500, text_body="Internal Server Error")
        with pytest.raises(PolymarketSignatureError, match=r"HTTP 500"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    def test_transport_error_raises(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.side_effect = httpx.ConnectError("connection refused")
        with pytest.raises(PolymarketSignatureError, match="Failed to reach signer service"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    def test_missing_signed_transactions_raises(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(200, {"message": "ok"})
        with pytest.raises(PolymarketSignatureError, match="missing signed_transactions"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    def test_missing_r_field_raises(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200, {"signed_transactions": [{"_type": "signature", "s": "0x" + "22" * 32, "v": 27}]}
        )
        with pytest.raises(PolymarketSignatureError, match="missing field"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    def test_invalid_v_field_raises(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": "not-a-number"}
                ]
            },
        )
        with pytest.raises(PolymarketSignatureError, match="invalid v field"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    @pytest.mark.parametrize("bad_v", [0xFF, 256, 26, 29, "256", "0x100", -1])
    def test_non_canonical_v_raises(self, sample_typed_data, bad_v):
        """Anything other than 0/1/27/28 is non-canonical and must raise.
        A regression that silently formats e.g. ``v=256`` would produce a
        malformed 65-byte signature that recovers to junk."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {"signed_transactions": [{"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": bad_v}]},
        )
        with pytest.raises(PolymarketSignatureError, match="invalid v field"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)

    @pytest.mark.parametrize(
        "compact_v,expected_byte",
        [
            (0, "1b"),  # EIP-2098 0 → 27
            (1, "1c"),  # EIP-2098 1 → 28
            (27, "1b"),
            (28, "1c"),
            ("27", "1b"),  # decimal string, NOT hex (0x27 = 39, would be invalid)
            ("28", "1c"),
            ("0x1b", "1b"),  # hex string
            ("0x1c", "1c"),
        ],
    )
    def test_v_canonicalised_to_27_28(self, sample_typed_data, compact_v, expected_byte):
        """ethers v6 emits 27/28 by default but EIP-2098-aware signers emit
        0/1; both must round-trip to the legacy 27/28 byte that Polymarket's
        verifier expects. Strings are decimal unless explicitly hex-prefixed
        or contain hex-only digits — guarding the previous bug where ``\"27\"``
        parsed as hex (0x27 = 39) and silently produced a malformed signature."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": compact_v}
                ]
            },
        )
        sig = sign_typed_data_remote(
            sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client
        )
        assert sig.endswith(expected_byte)

    def test_non_json_response_raises(self, sample_typed_data):
        client = MagicMock(spec=httpx.Client)
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.json.side_effect = ValueError("not json")
        response.text = "<html>500 internal error</html>"
        client.post.return_value = response
        with pytest.raises(PolymarketSignatureError, match="non-JSON"):
            sign_typed_data_remote(sample_typed_data, "0xabc", "https://signer.example.com", "jwt", http_client=client)


# =============================================================================
# Factories — make_local_signer / make_remote_signer (issue #1961)
# =============================================================================


class TestMakeLocalSigner:
    """``make_local_signer`` returns a Signer-shaped callable that wraps
    ``sign_typed_data_local`` with the captured private key."""

    def test_returns_callable_satisfying_signer_protocol(self, test_account):
        signer = make_local_signer(test_account.key.hex())
        # Signer is a structural Protocol; callability with the right shape
        # is what matters. The round-trip below is the load-bearing
        # assertion that the wired signature actually matches the EOA.
        assert callable(signer)
        # Reference Signer to keep the Protocol import live + give an
        # at-a-glance hint that the factory is meant to satisfy it.
        _: Signer = signer  # type: ignore[assignment]

    def test_signature_recovers_to_signer(self, sample_typed_data, test_account):
        signer = make_local_signer(test_account.key.hex())
        sig_hex = signer(sample_typed_data)
        signable = encode_typed_data(full_message=sample_typed_data)
        recovered = Account.recover_message(signable, signature=sig_hex)
        assert recovered == test_account.address

    def test_factory_matches_underlying_primitive(self, sample_typed_data, test_account):
        """The factory's output must be byte-identical to calling the
        primitive directly — the factory is a closure, not a behaviour
        change. A regression that adds preprocessing here would change the
        recovered EOA and break Polymarket auth."""
        signer = make_local_signer(test_account.key.hex())
        from_factory = signer(sample_typed_data)
        from_primitive = sign_typed_data_local(sample_typed_data, test_account.key.hex())
        assert from_factory == from_primitive


class TestMakeRemoteSigner:
    """``make_remote_signer`` wraps ``sign_typed_data_remote`` with captured
    eoa/url/jwt + an optional reused HTTP client."""

    def test_calls_remote_primitive_with_captured_args(self, sample_typed_data, test_account):
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "ab" * 32, "s": "0x" + "cd" * 32, "v": 27, "networkV": None}
                ]
            },
        )
        signer = make_remote_signer(
            eoa_address=test_account.address,
            signer_service_url="https://signer.example.com",
            signer_service_jwt="jwt-token",
            http_client=client,
        )
        sig = signer(sample_typed_data)

        assert client.post.call_count == 1
        body = client.post.call_args.kwargs["json"]
        # eoa_address propagated from factory
        assert body["eoa_address"] == test_account.address
        # JWT propagated from factory
        assert client.post.call_args.kwargs["headers"]["Authorization"] == "Bearer jwt-token"
        # URL propagated from factory
        assert client.post.call_args.args[0].startswith("https://signer.example.com")
        # Reassembled signature shape
        assert sig.startswith("0x")
        assert len(sig) == 2 + 65 * 2

    def test_reuses_http_client_across_calls(self, sample_typed_data, test_account):
        """A single ``http_client`` passed to the factory must be reused on
        every signer call — that's the whole point of the optional kwarg
        (amortises TLS handshake in hot paths)."""
        client = MagicMock(spec=httpx.Client)
        client.post.return_value = _make_mock_response(
            200,
            {
                "signed_transactions": [
                    {"_type": "signature", "r": "0x" + "11" * 32, "s": "0x" + "22" * 32, "v": 28, "networkV": None}
                ]
            },
        )
        signer = make_remote_signer(
            eoa_address=test_account.address,
            signer_service_url="https://signer.example.com",
            signer_service_jwt="jwt",
            http_client=client,
        )
        signer(sample_typed_data)
        signer(sample_typed_data)
        assert client.post.call_count == 2
        # The client we passed should NOT be closed by the signer — closure
        # ownership stays with the caller. ``close`` is never invoked.
        client.close.assert_not_called()


# =============================================================================
# signer_from_env — environment-variable composition (issue #1961)
# =============================================================================


class TestSignerFromEnv:
    """``signer_from_env`` builds a Signer from POLYMARKET_PRIVATE_KEY (local)
    or ALMANAK_SIGNER_SERVICE_URL+JWT+POLYMARKET_WALLET_ADDRESS (remote), or
    returns ``None`` when neither path is configured."""

    def test_local_wins_when_private_key_set(self, monkeypatch, test_account, sample_typed_data):
        monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", test_account.key.hex())
        # Even with remote envs ALSO set, local mode wins — trading EOA in
        # process is the unambiguous choice when the key is available.
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_URL", "https://signer.example.com")
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt")
        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)

        signer = signer_from_env()
        assert signer is not None
        sig_hex = signer(sample_typed_data)
        signable = encode_typed_data(full_message=sample_typed_data)
        assert Account.recover_message(signable, signature=sig_hex) == test_account.address

    def test_remote_when_only_remote_envs_set(self, monkeypatch, test_account):
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_URL", "https://signer.example.com")
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_JWT", "jwt-token")
        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)

        signer = signer_from_env()
        assert signer is not None  # remote signer is callable even before first call

    def test_none_when_nothing_configured(self, monkeypatch):
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("ALMANAK_SIGNER_SERVICE_URL", raising=False)
        monkeypatch.delenv("ALMANAK_SIGNER_SERVICE_JWT", raising=False)
        monkeypatch.delenv("POLYMARKET_WALLET_ADDRESS", raising=False)
        assert signer_from_env() is None

    def test_none_when_remote_partially_configured(self, monkeypatch, test_account):
        """Either all three remote envs are present or remote mode does NOT
        engage — a partial config is the same as no config (returns None)
        rather than a half-built Signer that raises at first use."""
        monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
        # Missing JWT
        monkeypatch.setenv("ALMANAK_SIGNER_SERVICE_URL", "https://signer.example.com")
        monkeypatch.delenv("ALMANAK_SIGNER_SERVICE_JWT", raising=False)
        monkeypatch.setenv("POLYMARKET_WALLET_ADDRESS", test_account.address)
        assert signer_from_env() is None
