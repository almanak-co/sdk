"""Unit tests for Polymarket V2 wallet auto-setup in PolymarketServiceServicer.

The V2 cutover moved the on-chain pre-flight (token approvals + source-asset
→ pUSD wrap) into the gateway service. The first BUY/market order acquires
``_wallet_ready_lock``, applies any missing approvals, and wraps source asset
to cover the order. Subsequent calls short-circuit on ``_allowances_applied``.

These behaviors have no test coverage in the cutover PR — this file fills that
gap. Anvil-based end-to-end coverage (real fork, real contracts) is tracked
separately.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest
from eth_account import Account

from almanak.framework.connectors.polymarket import SignatureType, TransactionData
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.services.polymarket_service import PolymarketServiceServicer

# Deterministic Anvil-style key — never funded.
TEST_PRIVATE_KEY = "0x" + "ab" * 32
TEST_ACCOUNT = Account.from_key(TEST_PRIVATE_KEY)
TEST_WALLET = TEST_ACCOUNT.address


@pytest.fixture
def settings() -> MagicMock:
    """Minimal GatewaySettings for the Polymarket servicer."""
    s = MagicMock(spec=GatewaySettings)
    s.private_key = TEST_PRIVATE_KEY
    s.polymarket_private_key = None
    s.eoa_address = TEST_WALLET
    s.polymarket_wallet_address = None
    s.safe_address = None
    s.safe_mode = None
    s.polymarket_api_key = "k"
    s.polymarket_secret = "c2VjcmV0"  # base64("secret")
    s.polymarket_passphrase = "p"
    return s


@pytest.fixture
def servicer(settings: MagicMock) -> PolymarketServiceServicer:
    return PolymarketServiceServicer(settings=settings)


# =============================================================================
# _required_pusd_units_for_buy — pure helper, no mocking needed
# =============================================================================


class TestRequiredPusdUnitsForBuy:
    """V2 BUY pre-flight math: how many pUSD base units are needed."""

    @pytest.mark.parametrize(
        ("price", "size", "expected_units"),
        [
            ("0.50", "10", 5_000_000),  # 5 pUSD = 5_000_000 (6 decimals)
            ("0.01", "100", 1_000_000),  # 1 pUSD floor
            ("0.99", "1", 990_000),  # under-1 pUSD allowed at the helper level
            ("0.123456", "1", 123_456),  # exact 6-dec precision
            ("0.123457", "1", 123_457),
            ("0.1234567", "1", 123_456),  # ROUND_DOWN at the 7th decimal
        ],
    )
    def test_buy_units(self, price: str, size: str, expected_units: int) -> None:
        result = PolymarketServiceServicer._required_pusd_units_for_buy(price, size)
        assert result == expected_units

    def test_zero_price_returns_zero(self) -> None:
        assert PolymarketServiceServicer._required_pusd_units_for_buy("0", "10") == 0

    def test_negative_returns_zero(self) -> None:
        # Defensive — negative price is nonsensical but the helper should not
        # propagate a negative or otherwise lie about how much to wrap.
        assert PolymarketServiceServicer._required_pusd_units_for_buy("-0.5", "10") == 0

    def test_invalid_price_returns_zero(self) -> None:
        assert PolymarketServiceServicer._required_pusd_units_for_buy("not-a-number", "10") == 0


# =============================================================================
# _ensure_wallet_ready — idempotency, wrap, lock, error paths
# =============================================================================


class _FakeCtfSDK:
    """Hand-rolled fake — easier to read than wiring six MagicMock chains.

    Tracks call counts on ensure_allowances/get_pusd_balance/get_source_asset_balance
    /build_wrap_to_pusd_tx so tests can assert exact behavior.
    """

    def __init__(
        self,
        *,
        approval_txs: list[TransactionData] | None = None,
        pusd_balances: list[int] | None = None,
        source_balance: int = 10_000_000_000,
        source_asset: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
    ) -> None:
        self._approval_txs = approval_txs or []
        self._pusd_balances = list(pusd_balances or [0])
        self._source_balance = source_balance
        self.source_asset = source_asset
        self.ensure_allowances_calls = 0
        self.wrap_calls: list[int] = []  # amounts requested

    def ensure_allowances(self, wallet: str, web3) -> list[TransactionData]:  # noqa: ARG002
        self.ensure_allowances_calls += 1
        return list(self._approval_txs)

    def get_pusd_balance(self, wallet: str, web3) -> int:  # noqa: ARG002
        # Pop one balance per call so tests can simulate "balance unchanged
        # before wrap" → "balance present after wrap".
        if len(self._pusd_balances) > 1:
            return self._pusd_balances.pop(0)
        return self._pusd_balances[0]

    def get_source_asset_balance(self, wallet: str, web3) -> int:  # noqa: ARG002
        return self._source_balance

    def build_wrap_to_pusd_tx(self, wallet: str, amount: int) -> TransactionData:  # noqa: ARG002
        self.wrap_calls.append(amount)
        return TransactionData(to="0xWrap", data="0x", gas_estimate=150_000, description=f"wrap {amount}")


class TestEnsureWalletReady:
    """Idempotent on-chain auto-setup behavior."""

    @pytest.mark.asyncio
    async def test_idempotent_second_call_skips_approvals_and_wrap(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Once approvals are applied and pUSD covers the order, the second
        call must NOT re-emit the approval set or attempt another wrap."""
        ctf = _FakeCtfSDK(approval_txs=[], pusd_balances=[10_000_000])  # 10 pUSD already on hand
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = MagicMock()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)
        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        # Approvals: still asks the SDK to compute the diff each call (cheap),
        # but the SDK returned no missing approvals so no tx is signed.
        # Wrap: pUSD balance covers, so no wrap tx either.
        assert servicer._sign_and_submit_setup_tx.await_count == 0
        assert ctf.wrap_calls == []
        assert servicer._allowances_applied is True

    @pytest.mark.asyncio
    async def test_buy_wraps_when_pusd_insufficient(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """If pUSD balance < min_pusd_units, the gateway must wrap the deficit
        from the source asset before letting the order proceed."""
        ctf = _FakeCtfSDK(
            approval_txs=[],
            pusd_balances=[2_000_000],  # 2 pUSD held; need 5
            source_balance=100_000_000,  # plenty of source asset
        )
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = MagicMock()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=5_000_000)

        assert ctf.wrap_calls == [3_000_000]  # exact deficit
        assert servicer._sign_and_submit_setup_tx.await_count == 1

    @pytest.mark.asyncio
    async def test_raises_on_insufficient_source_asset(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """When source asset can't cover the wrap, fail fast with a clear
        message — letting the order proceed would result in a CLOB rejection
        with much less actionable error text."""
        ctf = _FakeCtfSDK(
            approval_txs=[],
            pusd_balances=[1_000_000],  # 1 pUSD held; need 10
            source_balance=2_000_000,  # only 2 source asset → can't cover deficit of 9
        )
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = MagicMock()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        with pytest.raises(ValueError, match="Insufficient source asset"):
            await servicer._ensure_wallet_ready(min_pusd_units=10_000_000)

        # Critically: no wrap submitted. The ValueError must come BEFORE the
        # tx signing, so the strategy gets a fast-fail and doesn't burn gas.
        assert servicer._sign_and_submit_setup_tx.await_count == 0
        assert ctf.wrap_calls == []

    @pytest.mark.asyncio
    async def test_concurrent_first_orders_coalesce_under_lock(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Two concurrent first-orders must NOT each submit the approval set.

        Without ``_wallet_ready_lock``, two simultaneous BUYs would both see
        ``_allowances_applied is False``, both call ``ensure_allowances``, and
        both submit duplicate (revert-on-second) approval txs. The lock makes
        the slow leg wait while the fast leg flips the flag.
        """
        approve_tx = TransactionData(to="0xPusd", data="0x", gas_estimate=80_000, description="approve")
        ctf = _FakeCtfSDK(approval_txs=[approve_tx], pusd_balances=[10_000_000])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = MagicMock()

        # Make the tx submission take a beat so the second call queues on the lock.
        async def slow_submit(_tx: TransactionData) -> str:
            await asyncio.sleep(0.01)
            return "0xhash"

        servicer._sign_and_submit_setup_tx = AsyncMock(side_effect=slow_submit)

        await asyncio.gather(
            servicer._ensure_wallet_ready(min_pusd_units=5_000_000),
            servicer._ensure_wallet_ready(min_pusd_units=5_000_000),
        )

        # The first holder runs ensure_allowances; the second sees
        # _allowances_applied=True after the lock releases and skips it.
        assert ctf.ensure_allowances_calls == 1
        assert servicer._sign_and_submit_setup_tx.await_count == 1

    @pytest.mark.asyncio
    async def test_no_op_when_min_pusd_units_zero(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """SELL orders compute ``min_pusd_units == 0`` (maker spends shares,
        not pUSD). The setup should not query pUSD balance or attempt to wrap
        — wasted RPC and a confusing wrap tx for SELL flows."""
        ctf = _FakeCtfSDK(approval_txs=[], pusd_balances=[0])
        servicer._ctf_sdk = ctf
        servicer._polygon_web3 = MagicMock()
        servicer._sign_and_submit_setup_tx = AsyncMock(return_value="0xhash")

        await servicer._ensure_wallet_ready(min_pusd_units=0)

        assert ctf.wrap_calls == []
        assert servicer._sign_and_submit_setup_tx.await_count == 0


# =============================================================================
# _build_l1_headers — 0x prefix repair (regression: VIB-3013)
# =============================================================================


class TestL1SignatureZeroXPrefix:
    """Polymarket /auth/* endpoints reject signatures without the 0x prefix
    with HTTP 401. eth-account's ``signed.signature.hex()`` returns hex
    WITHOUT the prefix in modern versions, so the gateway must prepend it."""

    def test_signature_is_prefixed_with_0x(self, servicer: PolymarketServiceServicer) -> None:
        headers = servicer._build_l1_headers(nonce=0)
        sig = headers["POLY_SIGNATURE"]
        assert sig.startswith("0x")
        # 65-byte secp256k1 signature → 130 hex chars + "0x"
        assert len(sig) == 132
        assert all(c in "0123456789abcdef" for c in sig[2:])

    def test_signature_repair_when_eth_account_returns_unprefixed(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Force eth_account to return an unprefixed hex string and assert the
        gateway repairs it. Without the repair, /auth/derive-api-key returns
        HTTP 401 and the strategy can't trade."""
        with patch("almanak.gateway.services.polymarket_service.Account") as mock_account:
            signed = MagicMock()
            signed.signature.hex.return_value = "ab" * 65  # 130 hex chars, no 0x
            mock_account.sign_message.return_value = signed

            headers = servicer._build_l1_headers(nonce=0)

            assert headers["POLY_SIGNATURE"] == "0x" + "ab" * 65


# =============================================================================
# Address resolution helpers
# =============================================================================


class TestResolveSignerAddress:
    """The signer address (POLY_ADDRESS) is the EIP-712 caller for L1 auth.
    It must come from settings.eoa_address first, then fall back to deriving
    from a private key. None when neither is configured."""

    def test_uses_eoa_address_when_set(self) -> None:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = "0x1111111111111111111111111111111111111111"
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._wallet_address == s.eoa_address

    def test_derives_from_private_key_when_no_eoa(self) -> None:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = None
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._wallet_address == TEST_WALLET

    def test_derives_from_polymarket_private_key_when_general_key_unset(self) -> None:
        """polymarket_private_key is the per-service override."""
        s = MagicMock(spec=GatewaySettings)
        s.private_key = None
        s.polymarket_private_key = TEST_PRIVATE_KEY
        s.eoa_address = None
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._wallet_address == TEST_WALLET

    def test_returns_none_when_unconfigured(self) -> None:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = None
        s.polymarket_private_key = None
        s.eoa_address = None
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._wallet_address is None
        assert servicer._available is False


class TestResolveFunderAddress:
    """funder is the on-chain holder of pUSD (may differ from signer when
    using a Safe). Order: polymarket_wallet_address > safe_address > signer."""

    def test_polymarket_wallet_address_wins(self) -> None:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = TEST_WALLET
        s.polymarket_wallet_address = "0x2222222222222222222222222222222222222222"
        s.safe_address = "0x3333333333333333333333333333333333333333"
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._funder_address == s.polymarket_wallet_address

    def test_falls_back_to_safe_address(self) -> None:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = TEST_WALLET
        s.polymarket_wallet_address = None
        s.safe_address = "0x3333333333333333333333333333333333333333"
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._funder_address == s.safe_address

    def test_falls_back_to_signer(self) -> None:
        """No Safe configured → funder == signer (EOA-only deployment)."""
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = TEST_WALLET
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = None
        s.polymarket_secret = None
        s.polymarket_passphrase = None
        servicer = PolymarketServiceServicer(settings=s)
        assert servicer._funder_address == TEST_WALLET


class TestSignatureTypeResolution:
    """signature_type drives how the CLOB validator interprets the EIP-712
    signature: EOA (caller signs) vs POLY_GNOSIS_SAFE (Safe wrapper)."""

    def test_eoa_when_no_safe(self, settings: MagicMock) -> None:
        settings.safe_address = None
        settings.safe_mode = None
        servicer = PolymarketServiceServicer(settings=settings)
        assert servicer._signature_type == SignatureType.EOA

    def test_safe_with_direct_mode(self, settings: MagicMock) -> None:
        settings.safe_address = "0x1234567890123456789012345678901234567890"
        settings.safe_mode = "direct"
        servicer = PolymarketServiceServicer(settings=settings)
        assert servicer._signature_type == SignatureType.POLY_GNOSIS_SAFE

    def test_safe_with_zodiac_mode(self, settings: MagicMock) -> None:
        settings.safe_address = "0x1234567890123456789012345678901234567890"
        settings.safe_mode = "zodiac"
        servicer = PolymarketServiceServicer(settings=settings)
        assert servicer._signature_type == SignatureType.POLY_GNOSIS_SAFE

    def test_safe_with_unknown_mode_falls_back_to_eoa(self, settings: MagicMock) -> None:
        """An unrecognized safe_mode (typo / future) must default to EOA — the
        safer choice when we don't know how to wrap the signature."""
        settings.safe_address = "0x1234567890123456789012345678901234567890"
        settings.safe_mode = "future-mode-not-yet-supported"
        servicer = PolymarketServiceServicer(settings=settings)
        assert servicer._signature_type == SignatureType.EOA

    def test_safe_address_without_mode_falls_back_to_eoa(self, settings: MagicMock) -> None:
        """A Safe address without an explicit mode is also EOA — agent code
        likely set the address but didn't switch on Safe execution yet."""
        settings.safe_address = "0x1234567890123456789012345678901234567890"
        settings.safe_mode = None
        servicer = PolymarketServiceServicer(settings=settings)
        assert servicer._signature_type == SignatureType.EOA


# =============================================================================
# L2 (HMAC) signing
# =============================================================================


class TestL2Signature:
    """L2 signature path: HMAC-SHA256 over `timestamp+method+path+body`,
    keyed by the base64-decoded api_secret. The signature is base64-encoded."""

    def test_signature_is_base64_with_valid_secret(self, servicer: PolymarketServiceServicer) -> None:
        sig = servicer._build_l2_signature("GET", "/markets", "1700000000")
        # Base64 decode round-trip — ensures the value is well-formed b64.
        import base64 as _b64
        decoded = _b64.b64decode(sig)
        assert len(decoded) == 32  # HMAC-SHA256 → 32 bytes

    def test_signature_changes_with_body(self, servicer: PolymarketServiceServicer) -> None:
        """Body is part of the signed message; different body → different sig."""
        sig_a = servicer._build_l2_signature("POST", "/order", "1700000000", body='{"a":1}')
        sig_b = servicer._build_l2_signature("POST", "/order", "1700000000", body='{"a":2}')
        assert sig_a != sig_b

    def test_signature_changes_with_timestamp(self, servicer: PolymarketServiceServicer) -> None:
        """Replay protection: same content at a different time signs differently."""
        sig_a = servicer._build_l2_signature("GET", "/markets", "1700000000")
        sig_b = servicer._build_l2_signature("GET", "/markets", "1700000001")
        assert sig_a != sig_b

    def test_invalid_base64_secret_raises_value_error(self, settings: MagicMock) -> None:
        """A non-base64 secret must fail loudly at signing time, not silently
        produce a garbage signature that gets a confusing 401 from Polymarket."""
        settings.polymarket_secret = "not-base64!!!"
        servicer = PolymarketServiceServicer(settings=settings)
        with pytest.raises(ValueError, match="not valid base64"):
            servicer._build_l2_signature("GET", "/markets", "1700000000")

    def test_signature_manual_verification(self) -> None:
        """Pin the exact HMAC: tests don't catch a subtle byte-encoding bug
        unless they verify the expected value against an independent computation."""
        import base64 as _b64
        import hashlib as _hashlib
        import hmac as _hmac

        secret_b64 = _b64.b64encode(b"secret").decode()
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = TEST_WALLET
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        s.polymarket_api_key = "k"
        s.polymarket_secret = secret_b64
        s.polymarket_passphrase = "p"
        servicer = PolymarketServiceServicer(settings=s)

        timestamp = "1700000000"
        method = "GET"
        path = "/markets"
        body = ""
        sig = servicer._build_l2_signature(method, path, timestamp, body)

        message = f"{timestamp}{method}{path}{body}".encode()
        expected = _b64.b64encode(_hmac.new(b"secret", message, _hashlib.sha256).digest()).decode()
        assert sig == expected


class TestL2HeadersErrors:
    """_build_l2_headers must enumerate every missing field — strategies see
    a single clear error rather than a series of cryptic 401s from Polymarket."""

    def _missing_creds_servicer(self, **overrides) -> PolymarketServiceServicer:
        s = MagicMock(spec=GatewaySettings)
        s.private_key = TEST_PRIVATE_KEY
        s.polymarket_private_key = None
        s.eoa_address = TEST_WALLET
        s.polymarket_wallet_address = None
        s.safe_address = None
        s.safe_mode = None
        defaults = {"polymarket_api_key": "k", "polymarket_secret": "c2VjcmV0", "polymarket_passphrase": "p"}
        defaults.update(overrides)
        for k, v in defaults.items():
            setattr(s, k, v)
        return PolymarketServiceServicer(settings=s)

    def test_missing_api_key(self) -> None:
        servicer = self._missing_creds_servicer(polymarket_api_key=None)
        with pytest.raises(ValueError, match="api_key"):
            servicer._build_l2_headers("GET", "/markets")

    def test_missing_api_secret(self) -> None:
        servicer = self._missing_creds_servicer(polymarket_secret=None)
        with pytest.raises(ValueError, match="api_secret"):
            servicer._build_l2_headers("GET", "/markets")

    def test_missing_passphrase(self) -> None:
        servicer = self._missing_creds_servicer(polymarket_passphrase=None)
        with pytest.raises(ValueError, match="api_passphrase"):
            servicer._build_l2_headers("GET", "/markets")

    def test_lists_all_missing_at_once(self) -> None:
        """If multiple fields are missing the error mentions all of them."""
        servicer = self._missing_creds_servicer(
            polymarket_api_key=None, polymarket_secret=None, polymarket_passphrase=None
        )
        with pytest.raises(ValueError) as exc_info:
            servicer._build_l2_headers("GET", "/markets")
        msg = str(exc_info.value)
        assert "api_key" in msg
        assert "api_secret" in msg
        assert "api_passphrase" in msg

    def test_headers_present_when_fully_configured(self, servicer: PolymarketServiceServicer) -> None:
        """All five POLY_* headers must be set when configured — missing one
        causes an immediate 401 from the CLOB."""
        headers = servicer._build_l2_headers("GET", "/markets")
        assert set(headers.keys()) == {
            "POLY_ADDRESS",
            "POLY_SIGNATURE",
            "POLY_TIMESTAMP",
            "POLY_API_KEY",
            "POLY_PASSPHRASE",
        }
        assert headers["POLY_ADDRESS"] == TEST_WALLET
        assert headers["POLY_API_KEY"] == "k"
        assert headers["POLY_PASSPHRASE"] == "p"
        # Signature is non-empty base64
        import base64 as _b64
        assert len(_b64.b64decode(headers["POLY_SIGNATURE"])) == 32


# =============================================================================
# _market_response_from_gamma — JSON-string vs list parsing of array fields
# =============================================================================


class TestMarketResponseFromGamma:
    """Gamma may return JSON-string-encoded arrays or actual lists for
    ``outcomes`` / ``outcomePrices`` / ``clobTokenIds`` / ``tags``. The
    helper must accept both and degrade gracefully on bad input."""

    def test_parses_json_string_arrays(self) -> None:
        data = {
            "id": "abc",
            "conditionId": "0x" + "00" * 32,
            "question": "Will X happen?",
            "slug": "x",
            "outcomes": '["Yes", "No"]',
            "outcomePrices": '["0.6", "0.4"]',
            "clobTokenIds": '["111", "222"]',
            "tags": '["politics", "us"]',
            "active": True,
            "closed": False,
        }
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        assert list(resp.outcomes) == ["Yes", "No"]
        assert list(resp.outcome_prices) == ["0.6", "0.4"]
        assert list(resp.clob_token_ids) == ["111", "222"]
        assert list(resp.tags) == ["politics", "us"]

    def test_parses_actual_lists(self) -> None:
        """Some endpoints return real lists; both shapes must work."""
        data = {
            "id": "abc",
            "conditionId": "0x" + "00" * 32,
            "outcomes": ["Yes", "No"],
            "outcomePrices": ["0.5", "0.5"],
            "clobTokenIds": ["111", "222"],
            "tags": ["x", "y"],
            "active": True,
            "closed": False,
        }
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        assert list(resp.outcomes) == ["Yes", "No"]
        assert list(resp.outcome_prices) == ["0.5", "0.5"]
        assert list(resp.clob_token_ids) == ["111", "222"]
        assert list(resp.tags) == ["x", "y"]

    def test_handles_missing_arrays(self) -> None:
        """Missing array fields default to empty — no crash."""
        data = {"id": "abc", "conditionId": "0x" + "00" * 32, "active": True, "closed": False}
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        assert list(resp.outcomes) == []
        assert list(resp.outcome_prices) == []
        assert list(resp.clob_token_ids) == []
        assert list(resp.tags) == []

    def test_handles_malformed_json(self) -> None:
        """Bad JSON for any array field falls back to an empty list rather
        than raising — gateway robustness for upstream regressions."""
        data = {
            "id": "abc",
            "conditionId": "0x" + "00" * 32,
            "outcomes": '["Yes", "No"',  # truncated JSON
            "outcomePrices": "not-json",
            "clobTokenIds": "not-json",
            "tags": "not-json",
            "active": True,
            "closed": False,
        }
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        assert list(resp.outcomes) == []
        assert list(resp.outcome_prices) == []
        assert list(resp.clob_token_ids) == []
        assert list(resp.tags) == []

    def test_question_id_field_aliases(self) -> None:
        """``questionID`` (camelCase) and ``questionId`` are both accepted."""
        d_camel = {"id": "1", "conditionId": "0x", "questionID": "qid-camel", "active": True, "closed": False}
        d_pascal = {"id": "1", "conditionId": "0x", "questionId": "qid-pascal", "active": True, "closed": False}
        assert PolymarketServiceServicer._market_response_from_gamma(d_camel).question_id == "qid-camel"
        assert PolymarketServiceServicer._market_response_from_gamma(d_pascal).question_id == "qid-pascal"

    def test_accepting_orders_falls_back_to_active(self) -> None:
        """When ``acceptingOrders`` is missing, fall back to ``active``."""
        data_with = {
            "id": "abc",
            "conditionId": "0x" + "00" * 32,
            "active": True,
            "closed": False,
            "acceptingOrders": False,
        }
        data_without = {"id": "abc", "conditionId": "0x" + "00" * 32, "active": True, "closed": False}
        assert PolymarketServiceServicer._market_response_from_gamma(data_with).accepting_orders is False
        assert PolymarketServiceServicer._market_response_from_gamma(data_without).accepting_orders is True

    def test_default_min_size_and_tick_size(self) -> None:
        """Missing min-size / tick-size fields fall back to V2 defaults
        ("5" and "0.01") so the strategy doesn't divide by zero."""
        data = {"id": "1", "conditionId": "0x", "active": True, "closed": False}
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        assert resp.minimum_order_size == "5"
        assert resp.minimum_tick_size == "0.01"

    def test_raw_json_round_trip(self) -> None:
        """raw_json field carries the full upstream payload for debugging."""
        data = {"id": "abc", "conditionId": "0x" + "00" * 32, "active": True, "closed": False, "extra": 42}
        resp = PolymarketServiceServicer._market_response_from_gamma(data)
        roundtripped = json.loads(resp.raw_json)
        assert roundtripped["extra"] == 42


# =============================================================================
# _required_pusd_units_for_buy — boundary behavior
# =============================================================================


class TestRequiredPusdUnitsBoundary:
    """Pin the rounding direction (down) and zero-handling for the BUY
    pre-flight calc. ROUND_DOWN protects against over-wrapping."""

    def test_size_with_many_decimals_rounds_down(self) -> None:
        """0.5 × 10.99999999 = 5.499999995 → quantize to 5.499999 → 5_499_999."""
        assert PolymarketServiceServicer._required_pusd_units_for_buy("0.5", "10.99999999") == 5_499_999

    def test_zero_size_returns_zero(self) -> None:
        assert PolymarketServiceServicer._required_pusd_units_for_buy("0.5", "0") == 0

    def test_empty_string_returns_zero(self) -> None:
        """Defensive: malformed input must not raise."""
        assert PolymarketServiceServicer._required_pusd_units_for_buy("", "10") == 0

    def test_handles_decimal_size(self) -> None:
        """Sizes can be fractional — half a share at $1 is 50¢."""
        assert PolymarketServiceServicer._required_pusd_units_for_buy("1", "0.5") == 500_000


# =============================================================================
# Historical Data RPCs (VIB-3695) — proto conversion correctness
# =============================================================================
#
# The wrapper relies on these handlers producing well-formed proto responses;
# field-level drift between ``ClobClient`` model attributes and the proto
# schema would surface as silent data loss (e.g. missing prices, wrong
# timestamps). Mock ``ClobClient.get_price_history`` / ``get_trade_tape`` and
# assert the proto fields the handler emits.


class _FakeAuthenticatedClient:
    """Minimal ClobClient stand-in: tracks the (token_id, interval, ...) call
    args the service forwards, returns whatever the test pinned, and counts
    ``close()`` calls so we can assert the handler honors the close-on-exit
    contract."""

    def __init__(self, *, get_price_history=None, get_trade_tape=None) -> None:
        self._get_price_history_return = get_price_history
        self._get_trade_tape_return = get_trade_tape
        self.price_history_calls: list[tuple] = []
        self.trade_tape_calls: list[tuple] = []
        self.close_calls = 0

    def get_price_history(self, *args, **kwargs):
        self.price_history_calls.append((args, kwargs))
        if isinstance(self._get_price_history_return, Exception):
            raise self._get_price_history_return
        return self._get_price_history_return

    def get_trade_tape(self, *args, **kwargs):
        self.trade_tape_calls.append((args, kwargs))
        if isinstance(self._get_trade_tape_return, Exception):
            raise self._get_trade_tape_return
        return self._get_trade_tape_return

    def close(self) -> None:
        self.close_calls += 1


class TestGetPriceHistoryHandler:
    """``GetPriceHistory`` proxies ``ClobClient.get_price_history`` and must
    convert the returned ``PriceHistory`` model into the proto faithfully.
    """

    @pytest.mark.asyncio
    async def test_converts_price_history_to_proto(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        from datetime import UTC, datetime
        from decimal import Decimal

        from almanak.framework.connectors.polymarket.models import (
            HistoricalPrice,
            PriceHistory,
        )
        from almanak.gateway.proto import gateway_pb2

        history = PriceHistory(
            token_id="111",
            interval="1h",
            prices=[
                HistoricalPrice(
                    timestamp=datetime.fromtimestamp(1700000000, tz=UTC),
                    price=Decimal("0.42"),
                ),
                HistoricalPrice(
                    timestamp=datetime.fromtimestamp(1700003600, tz=UTC),
                    price=Decimal("0.50"),
                ),
            ],
            start_time=datetime.fromtimestamp(1700000000, tz=UTC),
            end_time=datetime.fromtimestamp(1700003600, tz=UTC),
        )
        fake = _FakeAuthenticatedClient(get_price_history=history)
        # Patch _build_client (sync, public path) so the handler picks up the fake.
        servicer._build_client = MagicMock(return_value=fake)

        request = gateway_pb2.PolymarketGetPriceHistoryRequest(
            token_id="111",
            interval="1h",
        )
        response = await servicer.GetPriceHistory(request, MagicMock())

        assert response.success is True
        assert response.token_id == "111"
        assert response.interval == "1h"
        assert len(response.prices) == 2
        assert response.prices[0].timestamp == 1700000000
        assert response.prices[0].price == "0.42"
        assert response.prices[1].timestamp == 1700003600
        assert response.prices[1].price == "0.50"
        assert response.start_time == 1700000000
        assert response.end_time == 1700003600
        # Client must be closed even on the happy path.
        assert fake.close_calls == 1

    @pytest.mark.asyncio
    async def test_zero_request_fields_are_treated_as_unset(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Proto3 primitives default to 0/empty-string. The handler must
        treat unset fields as ``None`` so the SDK's mutual-exclusion logic
        (``interval`` vs ``start_ts``+``end_ts``) doesn't trip on a phantom
        ``start_ts=0`` that the caller never set."""
        from almanak.framework.connectors.polymarket.models import PriceHistory
        from almanak.gateway.proto import gateway_pb2

        empty_history = PriceHistory(token_id="111", interval="1h", prices=[])
        fake = _FakeAuthenticatedClient(get_price_history=empty_history)
        servicer._build_client = MagicMock(return_value=fake)

        request = gateway_pb2.PolymarketGetPriceHistoryRequest(
            token_id="111", interval="1h"
            # start_ts / end_ts / fidelity intentionally omitted — should land as None
        )
        await servicer.GetPriceHistory(request, MagicMock())

        # Inspect the call site: positional args (token_id, interval, start_ts, end_ts, fidelity)
        args, _kwargs = fake.price_history_calls[0]
        assert args == ("111", "1h", None, None, None)

    @pytest.mark.asyncio
    async def test_conflicting_interval_and_range_aborts_invalid_argument(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Mutual exclusion of ``interval`` vs ``start_ts``/``end_ts`` is
        validated at the gateway boundary (per the security model: never
        delegate input validation to downstream layers). The handler must
        ``context.abort(INVALID_ARGUMENT)`` BEFORE building a client or
        invoking the SDK, so a misuse never reaches the upstream."""
        from unittest.mock import AsyncMock

        from almanak.gateway.proto import gateway_pb2

        fake = _FakeAuthenticatedClient()
        servicer._build_client = MagicMock(return_value=fake)

        # ``context.abort`` raises ``grpc.aio.AbortError`` in production. The
        # mock configures it to raise a sentinel so the handler bails out at
        # the same place it would in a real gRPC stack — and we can introspect
        # the abort call args after the raise.
        class _AbortSentinel(Exception):
            pass

        context = AsyncMock()
        context.abort = AsyncMock(side_effect=_AbortSentinel())

        request = gateway_pb2.PolymarketGetPriceHistoryRequest(
            token_id="111", interval="1h", start_ts=1, end_ts=2
        )
        with pytest.raises(_AbortSentinel):
            await servicer.GetPriceHistory(request, context)

        context.abort.assert_awaited_once()
        args = context.abort.await_args.args
        assert args[0] == grpc.StatusCode.INVALID_ARGUMENT
        assert "mutually exclusive" in args[1]
        # Critical: the SDK must not have been touched.
        assert fake.price_history_calls == []


class TestGetTradeTapeHandler:
    """``GetTradeTape`` proxies the authenticated ``/data/trades`` endpoint
    via ``ClobClient.get_trade_tape`` and converts the result to proto."""

    @pytest.mark.asyncio
    async def test_converts_trades_to_proto(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        from datetime import UTC, datetime
        from decimal import Decimal
        from unittest.mock import AsyncMock

        from almanak.framework.connectors.polymarket.models import HistoricalTrade
        from almanak.gateway.proto import gateway_pb2

        trades = [
            HistoricalTrade(
                id="t1",
                token_id="111",
                side="BUY",
                price=Decimal("0.42"),
                size=Decimal("10"),
                timestamp=datetime.fromtimestamp(1700000000, tz=UTC),
                maker="0xMaker",
                taker="0xTaker",
            ),
            HistoricalTrade(
                id="t2",
                token_id="111",
                side="SELL",
                price=Decimal("0.45"),
                size=Decimal("5"),
                timestamp=datetime.fromtimestamp(1700001000, tz=UTC),
            ),
        ]
        fake = _FakeAuthenticatedClient(get_trade_tape=trades)
        # _build_authenticated_client is async — patch with AsyncMock.
        servicer._build_authenticated_client = AsyncMock(return_value=fake)

        request = gateway_pb2.PolymarketGetTradeTapeRequest(token_id="111", limit=50)
        response = await servicer.GetTradeTape(request, MagicMock())

        assert response.success is True
        assert len(response.trades) == 2
        assert response.trades[0].id == "t1"
        assert response.trades[0].side == "BUY"
        assert response.trades[0].price == "0.42"
        assert response.trades[0].size == "10"
        assert response.trades[0].timestamp == 1700000000
        assert response.trades[0].maker == "0xMaker"
        assert response.trades[0].taker == "0xTaker"
        # token_id mirrored on the asset_id alias for upstream compatibility.
        assert response.trades[0].asset_id == "111"

        assert response.trades[1].side == "SELL"
        # Optional fields default to empty strings, not crashes.
        assert response.trades[1].maker == ""
        assert response.trades[1].taker == ""
        assert fake.close_calls == 1

    @pytest.mark.asyncio
    async def test_default_limit_is_100_when_not_set(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """The handler must default ``limit`` to 100 when the wire field is
        zero — proto3 doesn't distinguish "unset" from "explicitly zero" for
        primitives, so the handler is the place to apply the SDK's default."""
        from unittest.mock import AsyncMock

        from almanak.gateway.proto import gateway_pb2

        fake = _FakeAuthenticatedClient(get_trade_tape=[])
        servicer._build_authenticated_client = AsyncMock(return_value=fake)

        # Send an explicit request with limit=0 (the proto default).
        request = gateway_pb2.PolymarketGetTradeTapeRequest(token_id="111")
        await servicer.GetTradeTape(request, MagicMock())

        args, _kwargs = fake.trade_tape_calls[0]
        # (token_id, limit) — limit is the SDK's default 100.
        assert args == ("111", 100)

    @pytest.mark.asyncio
    async def test_empty_token_id_passes_none(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        """Empty token_id means market-wide tape; must pass ``None`` to the
        SDK so it omits the upstream ``market`` filter param."""
        from unittest.mock import AsyncMock

        from almanak.gateway.proto import gateway_pb2

        fake = _FakeAuthenticatedClient(get_trade_tape=[])
        servicer._build_authenticated_client = AsyncMock(return_value=fake)

        request = gateway_pb2.PolymarketGetTradeTapeRequest(token_id="", limit=10)
        await servicer.GetTradeTape(request, MagicMock())

        args, _kwargs = fake.trade_tape_calls[0]
        assert args == (None, 10)

    @pytest.mark.asyncio
    async def test_clob_exception_returns_failure_response(
        self, servicer: PolymarketServiceServicer
    ) -> None:
        from unittest.mock import AsyncMock

        from almanak.gateway.proto import gateway_pb2

        fake = _FakeAuthenticatedClient(get_trade_tape=RuntimeError("rate limited"))
        servicer._build_authenticated_client = AsyncMock(return_value=fake)

        request = gateway_pb2.PolymarketGetTradeTapeRequest(token_id="111", limit=10)
        response = await servicer.GetTradeTape(request, MagicMock())

        assert response.success is False
        assert "rate limited" in response.error


import json  # noqa: E402 — late import to avoid clashing at top of file
