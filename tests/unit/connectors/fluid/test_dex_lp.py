"""Unit tests for the Fluid DEX LP (SmartLending) connector — VIB-5032.

UAT card coverage: D2.1 (routing), D2.2 (position-key shape via accounting),
D3.1 (deposit-disabled refused at compile), D3.2 (slippage math, non-tautological),
D1.1/D1.3 (open/close calldata shape), receipt parsing (fungible Transfer scan),
D3.3 (valuation fail-closed / measured-zero). The SDK network boundary is mocked;
``build_*_tx`` encoders run for real so the calldata selectors/args are byte-real.
On-chain behaviour is covered by ``tests/intents/arbitrum/test_fluid_dex_lp.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.base.compiler import BaseCompilerContext
from almanak.connectors.fluid.dex_lp_compiler import DEFAULT_LP_SLIPPAGE, FluidDexLpCompiler
from almanak.connectors.fluid.dex_lp_receipt_parser import FluidDexLpReceiptParser
from almanak.connectors.fluid.smart_lending_sdk import (
    FluidDexLpDepositDisabledError,
    FluidSmartLendingSDK,
    _extract_revert_data,
    _scrub_hex_payload,
)
from almanak.framework.intents.compiler_models import CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import IntentType, LPCloseIntent, LPOpenIntent

FSL9 = "0x1F0bFd9862ae58208d26db0d80797974434EC013"  # sUSDai/USDC (enabled)
FSL12 = "0xdC1dF9E55f3B7EBD4F19001b294d1e537320BC2E"  # RLP/USDC (disabled)
FSL5 = "0x82C53239c4CFC89A8E55A691422af24c18A944b1"  # FLUID/native-ETH
SUSDAI = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WALLET = "0x2222222222222222222222222222222222222222"
RESOLVER = "0x3E69A3Af4305b65598b228d3da70786Bd9cfeB0e"

DEPOSIT_SEL = "0xfad3cc4b"
WITHDRAW_SEL = "0xd331bef7"


def _services() -> MagicMock:
    services = MagicMock()
    services.build_approve_tx.side_effect = lambda token_address, spender, amount: [
        TransactionData(
            to=token_address,
            value=0,
            data="0x095ea7b3" + spender[2:].lower().zfill(64) + f"{amount:064x}",
            gas_estimate=46_000,
            tx_type="approve",
            description=f"approve {amount} to {spender}",
        )
    ]
    services.format_amount.side_effect = lambda amount, decimals: str(amount)
    return services


def _ctx(services: MagicMock, chain: str = "arbitrum", **overrides) -> BaseCompilerContext:
    defaults = {
        "chain": chain,
        "wallet_address": WALLET,
        "rpc_url": "http://localhost:8545",
        "rpc_timeout": 10.0,
        "permission_discovery": False,
        "allow_placeholder_prices": True,
        "token_resolver": None,
        "gateway_client": None,
        "price_oracle": {},
        "cache": {},
        "services": services,
    }
    defaults.update(overrides)
    return BaseCompilerContext(**defaults)


def _sdk(**method_overrides) -> FluidSmartLendingSDK:
    """A real SDK (real build_*_tx encoders) with network reads stubbed."""
    sdk = FluidSmartLendingSDK(chain="arbitrum", resolver_address=RESOLVER, rpc_url="http://localhost:8545")
    sdk.check_deposit_enabled = MagicMock(return_value=None)  # type: ignore[method-assign]
    sdk.quote_deposit_shares = MagicMock(
        return_value=1_000_000_000_000_000_000_000
    )  # 1000e18  # type: ignore[method-assign]
    sdk.get_share_balance = MagicMock(return_value=945_000_000_000_000_000_000)  # 945e18  # type: ignore[method-assign]
    sdk.position_token_amounts = MagicMock(return_value=(683_000_000_000_000_000_000, 1_255_000_000))  # type: ignore[method-assign]
    for name, val in method_overrides.items():
        setattr(sdk, name, val)
    return sdk


def _lp_open(pool: str = FSL9, amount0="0", amount1="2000", **kw) -> LPOpenIntent:
    return LPOpenIntent(
        protocol="fluid_dex_lp",
        pool=pool,
        amount0=Decimal(amount0),
        amount1=Decimal(amount1),
        range_lower=Decimal("0.5"),  # dummy positive bounds — fungible, no range
        range_upper=Decimal("2"),
        chain="arbitrum",
        **kw,
    )


def _decode_word(calldata: str, word_index: int) -> int:
    body = calldata[10:]  # strip 0x + 4-byte selector
    return int(body[word_index * 64 : (word_index + 1) * 64], 16)


def _deposit_tx(bundle) -> dict:
    return next(tx for tx in bundle.transactions if str(tx.get("data", "")).startswith(DEPOSIT_SEL))


# ----------------------------------------------------------------------------
# D2.1 — routing
# ----------------------------------------------------------------------------


def test_compiler_protocol_and_intents():
    assert FluidDexLpCompiler.protocols == frozenset({"fluid_dex_lp"})
    assert FluidDexLpCompiler.intents == frozenset({IntentType.LP_OPEN, IntentType.LP_CLOSE})


def test_unknown_wrapper_rejected():
    compiler = FluidDexLpCompiler()
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), _lp_open(pool="0x" + "9" * 40))
    assert res.status == CompilationStatus.FAILED
    assert "Unknown Fluid SmartLending wrapper" in res.error


def test_unsupported_chain_rejected():
    compiler = FluidDexLpCompiler()
    res = compiler.compile(_ctx(_services(), chain="optimism"), _lp_open())
    assert res.status == CompilationStatus.FAILED
    assert "not supported on optimism" in res.error


# ----------------------------------------------------------------------------
# D1.1 — LP_OPEN calldata shape
# ----------------------------------------------------------------------------


def test_lp_open_single_sided_usdc():
    compiler = FluidDexLpCompiler()
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), _lp_open(amount0="0", amount1="2000"))
    assert res.status == CompilationStatus.SUCCESS
    txs = res.action_bundle.transactions
    # one approve (USDC only) + one deposit
    approves = [t for t in txs if str(t["data"]).startswith("0x095ea7b3")]
    assert len(approves) == 1
    dep = _deposit_tx(res.action_bundle)
    assert dep["value"] == 0
    assert _decode_word(dep["data"], 0) == 0  # token0 (sUSDai) amount = 0
    assert _decode_word(dep["data"], 1) == 2000 * 10**6  # token1 (USDC) wei


def test_action_txs_carry_gas_estimate_key():
    """Regression (VIB-5032 on-chain proof): the deposit/withdraw txs MUST carry
    ``gas_estimate`` — the key the orchestrator's ``_build_unsigned_transactions``
    reads (it defaults to 100k otherwise). A DEX-LP deposit is ~190k gas, so the
    wrong key (``gas``) silently OOGs the tx after the approve is applied.
    """
    compiler = FluidDexLpCompiler()
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        open_res = compiler.compile(_ctx(_services()), _lp_open(amount0="0", amount1="2000"))
    dep = _deposit_tx(open_res.action_bundle)
    assert "gas" not in dep, "raw 'gas' key is ignored by the orchestrator — must be 'gas_estimate'"
    assert dep.get("gas_estimate", 0) >= 200_000, "deposit gas_estimate must exceed real ~190k DEX-LP cost"

    close_intent = LPCloseIntent(protocol="fluid_dex_lp", pool=FSL9, position_id=FSL9, chain="arbitrum")
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        close_res = compiler.compile(_ctx(_services()), close_intent)
    wtx = close_res.action_bundle.transactions[0]
    assert "gas" not in wtx
    assert wtx.get("gas_estimate", 0) >= 200_000


def test_lp_open_both_token_two_approves():
    compiler = FluidDexLpCompiler()
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), _lp_open(amount0="100", amount1="2000"))
    assert res.status == CompilationStatus.SUCCESS
    approves = [t for t in res.action_bundle.transactions if str(t["data"]).startswith("0x095ea7b3")]
    assert len(approves) == 2  # both ERC-20 legs approved


def test_lp_open_native_leg_refused_at_compile():
    # fSL5 token1 is native ETH. v1 refuses native-leg wrappers at COMPILE: the
    # log-based receipt parser cannot measure native ETH (no ERC-20 Transfer), so
    # executing would mis-account the native leg as a measured zero (Empty≠Zero).
    # Refused — not faked-success, not executed with wrong books (VIB-5121).
    compiler = FluidDexLpCompiler()
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), _lp_open(pool=FSL5, amount0="0", amount1="0.5"))
    assert res.status == CompilationStatus.FAILED
    assert "native" in res.error.lower()
    assert res.action_bundle is None  # no transaction produced for a native-leg wrapper


# ----------------------------------------------------------------------------
# D3.2 — slippage (non-tautological): minShares = floor(quote * (1-tol)), not 0
# ----------------------------------------------------------------------------


def test_lp_open_min_shares_default_slippage():
    compiler = FluidDexLpCompiler()
    quote = 1_000_000_000_000_000_000_000  # 1000e18
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), _lp_open())
    dep = _deposit_tx(res.action_bundle)
    min_shares = _decode_word(dep["data"], 2)
    expected = int(Decimal(quote) * (Decimal(1) - DEFAULT_LP_SLIPPAGE))
    assert min_shares == expected
    assert min_shares > 0  # never a hardcoded zero
    assert min_shares < quote  # strictly below the quote


def test_lp_open_min_shares_custom_slippage():
    compiler = FluidDexLpCompiler()
    quote = 1_000_000_000_000_000_000_000
    intent = _lp_open(protocol_params={"max_slippage": "0.10"})
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), intent)
    min_shares = _decode_word(_deposit_tx(res.action_bundle)["data"], 2)
    assert min_shares == int(Decimal(quote) * Decimal("0.90"))


# ----------------------------------------------------------------------------
# D3.1 — deposit-disabled refused at COMPILE
# ----------------------------------------------------------------------------


def test_lp_open_deposit_disabled_refused():
    compiler = FluidDexLpCompiler()
    sdk = _sdk(check_deposit_enabled=MagicMock(side_effect=FluidDexLpDepositDisabledError("deposits disabled (51013)")))
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(sdk, None)):
        res = compiler.compile(_ctx(_services()), _lp_open(pool=FSL12))
    assert res.status == CompilationStatus.FAILED
    assert "disabled" in res.error.lower()
    # No transaction produced.
    assert res.action_bundle is None


# ----------------------------------------------------------------------------
# D1.3 — LP_CLOSE calldata shape
# ----------------------------------------------------------------------------


def test_lp_close_withdraw_shape_full_drain():
    # Full drain (no deliberate residual): the withdraw requests the FULL live
    # proportional claim (position_token_amounts) and caps the burn at the held
    # balance. The earlier code under-withdrew by *(1-tol), stranding ~0.5% of
    # the position as residual shares on every close (VIB-5032 teardown defect).
    compiler = FluidDexLpCompiler()
    shares = 945_000_000_000_000_000_000  # _sdk().get_share_balance
    t0_claim, t1_claim = 683_000_000_000_000_000_000, 1_255_000_000  # _sdk().position_token_amounts
    intent = LPCloseIntent(protocol="fluid_dex_lp", pool=FSL9, position_id=FSL9, chain="arbitrum")
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
        res = compiler.compile(_ctx(_services()), intent)
    assert res.status == CompilationStatus.SUCCESS
    tx = res.action_bundle.transactions[0]
    assert str(tx["data"]).startswith(WITHDRAW_SEL)
    t0_out = _decode_word(tx["data"], 0)
    t1_out = _decode_word(tx["data"], 1)
    max_shares = _decode_word(tx["data"], 2)
    assert t0_out == t0_claim, "token0-out must be the FULL claim (no *(1-tol) under-withdraw)"
    assert t1_out == t1_claim, "token1-out must be the FULL claim (no *(1-tol) under-withdraw)"
    assert max_shares == shares, "burn cap is the held balance (drain all shares), not shares*(1+tol)"


def test_lp_close_no_position_rejected():
    compiler = FluidDexLpCompiler()
    sdk = _sdk(get_share_balance=MagicMock(return_value=0))
    intent = LPCloseIntent(protocol="fluid_dex_lp", pool=FSL9, position_id=FSL9, chain="arbitrum")
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(sdk, None)):
        res = compiler.compile(_ctx(_services()), intent)
    assert res.status == CompilationStatus.FAILED
    assert "No Fluid DEX LP position" in res.error


def test_lp_open_invalid_slippage_fails_closed():
    """A bad caller-supplied max_slippage must return CompilationStatus.FAILED,
    not raise an uncaught exception that aborts the compile pipeline (CodeRabbit)."""
    compiler = FluidDexLpCompiler()
    for bad in ("abc", "1", "-0.1"):
        intent = _lp_open(protocol_params={"max_slippage": bad})
        with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(_sdk(), None)):
            res = compiler.compile(_ctx(_services()), intent)
        assert res.status == CompilationStatus.FAILED, f"max_slippage={bad!r} must fail closed"
        assert "max_slippage" in res.error


def test_lp_close_dust_claim_fails_closed():
    """Non-zero shares whose proportional claim floors to (0, 0) must fail the
    close compile — an exact-out withdraw(0, 0, ...) would revert / waste gas (Gemini)."""
    compiler = FluidDexLpCompiler()
    sdk = _sdk(
        get_share_balance=MagicMock(return_value=5),  # tiny dust shares
        position_token_amounts=MagicMock(return_value=(0, 0)),
    )
    intent = LPCloseIntent(protocol="fluid_dex_lp", pool=FSL9, position_id=FSL9, chain="arbitrum")
    with patch.object(FluidDexLpCompiler, "_build_sdk", return_value=(sdk, None)):
        res = compiler.compile(_ctx(_services()), intent)
    assert res.status == CompilationStatus.FAILED
    assert "dust" in res.error.lower() or "nothing to withdraw" in res.error.lower()


# ----------------------------------------------------------------------------
# _build_sdk — gateway / rpc resolution + fail-closed
# ----------------------------------------------------------------------------


def test_build_sdk_no_gateway_no_rpc_fails_closed():
    compiler = FluidDexLpCompiler()
    sdk, err = compiler._build_sdk(_ctx(_services(), rpc_url=None, gateway_client=None), "iid")
    assert sdk is None
    assert err is not None and err.status == CompilationStatus.FAILED
    assert "connected gateway" in err.error


def test_build_sdk_disconnected_gateway_no_rpc_fails_closed():
    compiler = FluidDexLpCompiler()
    gw = MagicMock()
    gw.is_connected = False
    sdk, err = compiler._build_sdk(_ctx(_services(), rpc_url=None, gateway_client=gw), "iid")
    assert sdk is None
    assert err is not None and err.status == CompilationStatus.FAILED


def test_build_sdk_rpc_fallback_builds_sdk():
    # Local/test path: no gateway + rpc_url → SDK over the direct-RPC fallback
    # (is_local, so the is_hosted guard does not fire).
    compiler = FluidDexLpCompiler()
    sdk, err = compiler._build_sdk(_ctx(_services(), rpc_url="http://localhost:8545", gateway_client=None), "iid")
    assert err is None
    assert isinstance(sdk, FluidSmartLendingSDK)


def test_build_sdk_prefers_connected_gateway():
    compiler = FluidDexLpCompiler()
    gw = MagicMock()
    gw.is_connected = True
    sdk, err = compiler._build_sdk(_ctx(_services(), rpc_url="http://localhost:8545", gateway_client=gw), "iid")
    assert err is None
    assert isinstance(sdk, FluidSmartLendingSDK)
    assert sdk._gateway_client is gw  # routed through the gateway, not rpc


# ----------------------------------------------------------------------------
# get_smart_lending_data — positional struct decode + self-verify (VIB-5024)
# ----------------------------------------------------------------------------


def _raw_sdk() -> FluidSmartLendingSDK:
    """SDK with NO method mocks (so the real decode/quote functions run)."""
    return FluidSmartLendingSDK(chain="arbitrum", resolver_address=RESOLVER, rpc_url="http://localhost:8545")


def _struct_blob(total_supply: int, reserve0: int, reserve1: int, t0: str, t1: str, dex: str, exch: int) -> bytes:
    def wi(v: int) -> str:
        return f"{v:064x}"

    def wa(a: str) -> str:
        return a[2:].lower().rjust(64, "0")

    words = [wi(0)] * 15  # indices 0-14; decoder reads 6,7,8,9,10,11,14
    words[6], words[7], words[8] = wi(total_supply), wi(reserve0), wi(reserve1)
    words[9], words[10], words[11] = wa(t0), wa(t1), wa(dex)
    words[14] = wi(exch)
    return bytes.fromhex("".join(words))


def _wrapper_getters(t0: str, t1: str, dex: str, ts: int) -> MagicMock:
    wc = MagicMock()
    wc.functions.TOKEN0.return_value.call.return_value = Web3.to_checksum_address(t0)
    wc.functions.TOKEN1.return_value.call.return_value = Web3.to_checksum_address(t1)
    wc.functions.DEX.return_value.call.return_value = Web3.to_checksum_address(dex)
    wc.functions.totalSupply.return_value.call.return_value = ts
    return wc


_DEX9 = "0x86f874212335Af27C41cDb855C2255543d1499cE"


def test_get_smart_lending_data_decodes_and_self_verifies():
    sdk = _raw_sdk()
    ts = 945 * 10**18
    sdk.w3 = MagicMock()
    sdk.w3.keccak.return_value = bytes.fromhex("abcdef01" + "00" * 28)
    sdk.w3.eth.call.return_value = _struct_blob(ts, 100, 200, SUSDAI, USDC, _DEX9, 10**18)
    sdk._resolver = MagicMock()
    sdk._resolver.address = RESOLVER
    sdk._wrapper = MagicMock(return_value=_wrapper_getters(SUSDAI, USDC, _DEX9, ts))
    d = sdk.get_smart_lending_data(FSL9)
    assert d.total_supply == ts
    assert d.token0 == Web3.to_checksum_address(SUSDAI)
    assert d.token1 == Web3.to_checksum_address(USDC)
    assert d.dex == Web3.to_checksum_address(_DEX9)
    assert d.reserve0 == 100 and d.reserve1 == 200


def test_get_smart_lending_data_self_verify_mismatch_raises():
    from almanak.connectors.fluid.smart_lending_sdk import FluidDexLpError

    sdk = _raw_sdk()
    ts = 945 * 10**18
    sdk.w3 = MagicMock()
    sdk.w3.keccak.return_value = bytes.fromhex("abcdef01" + "00" * 28)
    sdk.w3.eth.call.return_value = _struct_blob(ts, 100, 200, SUSDAI, USDC, _DEX9, 10**18)
    sdk._resolver = MagicMock()
    sdk._resolver.address = RESOLVER
    # wrapper getter disagrees on token0 → fail closed (decode-fragility guard)
    sdk._wrapper = MagicMock(return_value=_wrapper_getters(FSL12, USDC, _DEX9, ts))
    with pytest.raises(FluidDexLpError, match="self-verification"):
        sdk.get_smart_lending_data(FSL9)


def test_get_smart_lending_data_short_struct_raises():
    from almanak.connectors.fluid.smart_lending_sdk import FluidDexLpError

    sdk = _raw_sdk()
    sdk.w3 = MagicMock()
    sdk.w3.keccak.return_value = bytes.fromhex("abcdef01" + "00" * 28)
    sdk.w3.eth.call.return_value = bytes.fromhex("00" * 32 * 5)  # only 5 words (<15)
    sdk._resolver = MagicMock()
    sdk._resolver.address = RESOLVER
    with pytest.raises(FluidDexLpError, match="<15"):
        sdk.get_smart_lending_data(FSL9)


def test_quote_deposit_shares_extracts_from_revert_carrier():
    sdk = _raw_sdk()
    shares = 945 * 10**18
    carrier = "0xe8d35d06" + f"{shares:064x}"
    err = Exception(f"execution reverted {carrier}")
    err.data = carrier  # type: ignore[attr-defined]
    dex_contract = MagicMock()
    dex_contract.functions.deposit.return_value.call.side_effect = err
    sdk.w3 = MagicMock()
    sdk.w3.eth.contract.return_value = dex_contract
    out = sdk.quote_deposit_shares(_DEX9, 0, 2000 * 10**6)
    assert out == shares


def test_quote_deposit_shares_unexpected_revert_raises():
    from almanak.connectors.fluid.smart_lending_sdk import FluidDexLpError

    sdk = _raw_sdk()
    err = Exception("execution reverted 0xdeadbeef")  # not the share carrier
    err.data = "0xdeadbeef"  # type: ignore[attr-defined]
    dex_contract = MagicMock()
    dex_contract.functions.deposit.return_value.call.side_effect = err
    sdk.w3 = MagicMock()
    sdk.w3.eth.contract.return_value = dex_contract
    with pytest.raises(FluidDexLpError, match="estimate failed"):
        sdk.quote_deposit_shares(_DEX9, 0, 2000 * 10**6)


# ----------------------------------------------------------------------------
# _extract_revert_data / _scrub_hex_payload — contiguous hex only
# ----------------------------------------------------------------------------


def test_scrub_hex_payload_stops_at_first_non_hex():
    # trailing prose must NOT leak into the payload (the "d"/"a" of "data")
    assert _scrub_hex_payload("execution reverted 0x08c379a0, data: 0x...") == "0x08c379a0"
    assert _scrub_hex_payload("0xfad3cc4b") == "0xfad3cc4b"
    assert _scrub_hex_payload("no hex here") is None
    assert _scrub_hex_payload("0x") is None  # bare 0x, no body


def test_extract_revert_data_from_each_carrier():
    class _Err(Exception):
        pass

    e_data = _Err()
    e_data.data = "0x08c379a0deadbeef"  # type: ignore[attr-defined]
    assert _extract_revert_data(e_data) == "0x08c379a0deadbeef"

    e_msg = _Err()
    e_msg.message = "reverted with 0xabcdef00 trailing"  # type: ignore[attr-defined]
    assert _extract_revert_data(e_msg) == "0xabcdef00"

    assert _extract_revert_data(_Err("carrier in 0x1234 args")) == "0x1234"
    assert _extract_revert_data(_Err("no payload")) is None


# ----------------------------------------------------------------------------
# Receipt parsing — fungible Transfer scan
# ----------------------------------------------------------------------------

_TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ZERO = "0x" + "0" * 64


def _topic_addr(addr: str) -> str:
    return "0x" + addr[2:].lower().rjust(64, "0")


def _transfer_log(emitter: str, frm: str, to: str, amount: int) -> dict:
    return {
        "address": emitter,
        "topics": [_TRANSFER, _topic_addr(frm), _topic_addr(to)],
        "data": "0x" + f"{amount:064x}",
    }


def test_receipt_parser_accepts_chain_kwarg():
    """Regression (VIB-5032 E2E): the receipt registry constructs every parser as
    ``parser_class(chain=...)`` — the parser MUST accept it or the runner's
    result enrichment throws and all accounting fields go null."""
    parser = FluidDexLpReceiptParser(chain="arbitrum")
    assert parser.chain == "arbitrum"
    # registry passes lowercased/any-case chain; constructor must normalize
    assert FluidDexLpReceiptParser(chain="Arbitrum").chain == "arbitrum"


def test_receipt_extract_lp_open_data():
    parser = FluidDexLpReceiptParser()
    receipt = {
        "status": 1,
        "logs": [
            # deposit leg: USDC out of the wallet (Fluid routes it via the
            # Liquidity Layer; the parser keys on emitter==token + frm==wallet,
            # not the counterparty — single-intent / no_zodiac scope).
            _transfer_log(USDC, WALLET, "0x0000000000000000000000000000000000000099", 2000 * 10**6),
            _transfer_log(FSL9, "0x0000000000000000000000000000000000000000", WALLET, 945 * 10**18),
        ],
    }
    data = parser.extract_lp_open_data(receipt)
    assert data is not None
    assert data.position_id == 0  # fungible: no NFT id
    assert data.amount1 == 2000 * 10**6  # USDC leg (token1)
    # Empty != Zero: no token0 transfer is a MEASURED zero (single-sided), not
    # None — None would fail the typed LPOpenEventPayload Decimal field.
    assert data.amount0 == 0
    assert data.liquidity == 945 * 10**18  # shares minted
    assert data.pool_address.lower() == FSL9.lower()
    assert data.tick_lower is None and data.tick_upper is None
    # VIB-4426 mechanism: token addresses stamped so the LP accounting handler
    # (_v4_realign_token_pair) resolves symbols/decimals by address — the
    # wrapper-tailed position key has no <t0>/<t1>/<fee> descriptor.
    assert data.currency0 and data.currency1, "currency0/currency1 must be stamped for address-based symbol resolution"
    assert data.currency1.lower() == USDC.lower()  # token1 = USDC


def test_receipt_extract_lp_close_data_fees_none():
    parser = FluidDexLpReceiptParser()
    receipt = {
        "status": 1,
        "logs": [
            _transfer_log(FSL9, WALLET, "0x0000000000000000000000000000000000000000", 945 * 10**18),
            # withdraw leg: USDC into the wallet (Fluid Liquidity Layer source).
            _transfer_log(USDC, "0x0000000000000000000000000000000000000099", WALLET, 1900 * 10**6),
        ],
    }
    data = parser.extract_lp_close_data(receipt)
    assert data is not None
    assert data.amount1_collected == 1900 * 10**6
    assert data.fees0 is None and data.fees1 is None  # Empty != Zero
    assert data.liquidity_removed == 945 * 10**18
    assert data.pool_address.lower() == FSL9.lower()
    assert data.currency0 and data.currency1, "currency0/currency1 must be stamped (address-based symbol resolution)"
    assert data.currency1.lower() == USDC.lower()


def test_receipt_position_id_is_wrapper():
    parser = FluidDexLpReceiptParser()
    receipt = {
        "status": 1,
        "logs": [_transfer_log(FSL9, "0x0000000000000000000000000000000000000000", WALLET, 1)],
    }
    assert parser.extract_position_id(receipt).lower() == FSL9.lower()


def test_receipt_unrelated_logs_return_none():
    parser = FluidDexLpReceiptParser()
    receipt = {"status": 1, "logs": [_transfer_log(USDC, WALLET, "0x0000000000000000000000000000000000000099", 5)]}
    assert parser.extract_lp_open_data(receipt) is None  # no wrapper mint


# ----------------------------------------------------------------------------
# D3.3 — valuation fail-closed / measured-zero
# ----------------------------------------------------------------------------


def test_valuation_reader_none_without_gateway():
    from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPositionReader

    reader = FungibleLpPositionReader(gateway_client=None)
    assert reader.read_position(protocol="fluid_dex_lp", chain="arbitrum", wrapper=FSL9, wallet_address=WALLET) is None


def test_valuation_reader_unregistered_protocol():
    from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPositionReader

    reader = FungibleLpPositionReader(gateway_client=MagicMock())
    assert not reader.supports("uniswap_v3")
    assert reader.read_position(protocol="uniswap_v3", chain="arbitrum", wrapper=FSL9, wallet_address=WALLET) is None


def test_valuation_reader_supports_fluid_dex_lp():
    from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPositionReader

    reader = FungibleLpPositionReader(gateway_client=MagicMock())
    assert reader.supports("fluid_dex_lp")  # lazy bootstrap registers it


def test_valuation_measured_zero_for_empty_position():
    from almanak.connectors.fluid import dex_lp_valuation

    sdk = MagicMock()
    sdk.get_share_balance.return_value = 0
    with patch("almanak.connectors.fluid.smart_lending_sdk.FluidSmartLendingSDK", return_value=sdk):
        pos = dex_lp_valuation.read_fungible_lp_position(MagicMock(), "arbitrum", FSL9, WALLET)
    assert pos is not None
    assert pos.shares_wei == 0
    assert not pos.is_active  # measured zero, not None


def test_valuation_active_position_token_amounts():
    from almanak.connectors.fluid import dex_lp_valuation

    sdk = MagicMock()
    sdk.get_share_balance.return_value = 945 * 10**18
    sdk.position_token_amounts.return_value = (683 * 10**18, 1255 * 10**6)
    with patch("almanak.connectors.fluid.smart_lending_sdk.FluidSmartLendingSDK", return_value=sdk):
        pos = dex_lp_valuation.read_fungible_lp_position(MagicMock(), "arbitrum", FSL9, WALLET)
    assert pos.is_active
    assert pos.amount0_wei == 683 * 10**18
    assert pos.amount1_wei == 1255 * 10**6
    assert pos.token0_symbol == "sUSDai" and pos.token1_symbol == "USDC"
    assert pos.token0_decimals == 18 and pos.token1_decimals == 6
    # Addresses populated so the valuer can price by ADDRESS (VIB-5032) — a bare
    # symbol skips the oracle's CoinGecko/DexScreener by-address sources.
    assert pos.token0_address.lower() == SUSDAI.lower()
    assert pos.token1_address.lower() == USDC.lower()


def test_fungible_valuer_prices_legs_by_address():
    """The fungible-LP valuer must price each leg BY ADDRESS (preferred), not by
    bare symbol — for an exotic token (sUSDai) only the address path engages the
    oracle's CoinGecko/DexScreener by-address sources. VIB-5032 Blocker fix."""
    from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPosition
    from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

    pos = FungibleLpPosition(
        wrapper=FSL9,
        token0_symbol="sUSDai",
        token1_symbol="USDC",
        token0_decimals=18,
        token1_decimals=6,
        amount0_wei=683 * 10**18,
        amount1_wei=1255 * 10**6,
        shares_wei=945 * 10**18,
        token0_address=SUSDAI,
        token1_address=USDC,
    )
    reader = MagicMock()
    reader.read_position.return_value = pos

    priced_keys: list[str] = []

    def _price(token: str, quote: str = "USD") -> Decimal:
        priced_keys.append(token)
        # Only the ADDRESS resolves (the exotic symbol does NOT) — mirrors the
        # gateway, which builds a ResolvedToken only for address-form inputs.
        by_addr = {SUSDAI.lower(): Decimal("1.02"), USDC.lower(): Decimal("1")}
        if token.lower() in by_addr:
            return by_addr[token.lower()]
        raise KeyError(f"unpriceable symbol {token}")

    market = MagicMock()
    market.price.side_effect = _price

    valuer = PortfolioValuer.__new__(PortfolioValuer)
    valuer._fungible_lp_reader = reader  # type: ignore[attr-defined]
    position = MagicMock()
    position.protocol = "fluid_dex_lp"
    position.position_id = "lp:fluid_dex_lp:arbitrum:wallet:fsl9"
    position.details = {"wrapper": FSL9, "wallet_address": WALLET}

    result = valuer._reprice_fungible_lp_enriched(position, "arbitrum", market)
    assert result is not None, "valuer must value the position via address pricing (not UNAVAILABLE)"
    value_usd, details = result
    # 683*1.02 + 1255*1 = 696.66 + 1255 = 1951.66
    assert value_usd == Decimal("683") * Decimal("1.02") + Decimal("1255")
    assert SUSDAI in priced_keys, "sUSDai leg must be priced by ADDRESS"
    assert USDC in priced_keys, "USDC leg must be priced by ADDRESS"


def test_fungible_valuer_nonpositive_address_price_falls_back_to_symbol():
    """A non-positive (≤0) price from the address path is an oracle miss, not a
    measured value — the valuer must keep trying the symbol fallback rather than
    short-circuit and underprice the LP (CodeRabbit). VIB-5032."""
    from almanak.framework.valuation.fungible_lp_position_reader import FungibleLpPosition
    from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

    pos = FungibleLpPosition(
        wrapper=FSL9,
        token0_symbol="sUSDai",
        token1_symbol="USDC",
        token0_decimals=18,
        token1_decimals=6,
        amount0_wei=683 * 10**18,
        amount1_wei=1255 * 10**6,
        shares_wei=945 * 10**18,
        token0_address=SUSDAI,
        token1_address=USDC,
    )
    reader = MagicMock()
    reader.read_position.return_value = pos

    def _price(token: str, quote: str = "USD") -> Decimal:
        # Address path returns 0 (miss) for sUSDai; symbol path has a real price.
        by_addr = {SUSDAI.lower(): Decimal("0"), USDC.lower(): Decimal("1")}
        by_sym = {"susdai": Decimal("1.02")}
        if token.lower() in by_addr:
            return by_addr[token.lower()]
        if token.lower() in by_sym:
            return by_sym[token.lower()]
        raise KeyError(token)

    market = MagicMock()
    market.price.side_effect = _price

    valuer = PortfolioValuer.__new__(PortfolioValuer)
    valuer._fungible_lp_reader = reader  # type: ignore[attr-defined]
    position = MagicMock()
    position.protocol = "fluid_dex_lp"
    position.position_id = "lp:fluid_dex_lp:arbitrum:wallet:fsl9"
    position.details = {"wrapper": FSL9, "wallet_address": WALLET}

    result = valuer._reprice_fungible_lp_enriched(position, "arbitrum", market)
    assert result is not None, "zero address-price must fall back to symbol, not void the position"
    value_usd, _ = result
    # sUSDai priced via symbol fallback (1.02), USDC via address (1).
    assert value_usd == Decimal("683") * Decimal("1.02") + Decimal("1255")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
