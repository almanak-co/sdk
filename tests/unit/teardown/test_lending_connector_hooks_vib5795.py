"""Connector-specific lending teardown hooks: euler_v2 / silo_v2 / benqi (VIB-5795).

Exercises each hook against a scripted gateway double with the REAL connector
address catalogues — multi-target sums (residual parked in a non-preferred
vault / second silo market must be caught), the recorded silo 1,000-share dust
case, the benqi Compound-V2 snapshot decode (error word, exchange-rate math,
WAVAX alias), and block pinning.
"""

from __future__ import annotations

from types import SimpleNamespace

from almanak.connectors.benqi.adapter import BENQI_QI_TOKENS
from almanak.connectors.benqi.teardown_post_condition import benqi_teardown_post_condition
from almanak.connectors.euler_v2.adapter import DEBT_OF_SELECTOR, EULER_V2_VAULTS_BY_CHAIN
from almanak.connectors.euler_v2.teardown_post_condition import euler_v2_teardown_post_condition
from almanak.connectors.silo_v2.adapter import _TOKEN_TO_SILO_MAP
from almanak.connectors.silo_v2.teardown_post_condition import silo_v2_teardown_post_condition

_WALLET = "0x" + "11" * 20

_AVAX_USDC_VAULTS = [
    entry["vault_address"]
    for entry in EULER_V2_VAULTS_BY_CHAIN["avalanche"].values()
    if entry["underlying_symbol"] == "USDC"
]
_WAVAX_SILOS = [silo for _market, silo, _idx in _TOKEN_TO_SILO_MAP["WAVAX"]]


def _pos(protocol, position_type, chain, asset, position_id=None):
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id or f"{protocol}-{position_type.lower()}-{asset}-{chain}",
        chain=chain,
        position_type=position_type,
        details={"asset": asset, "type": "collateral" if position_type == "SUPPLY" else "borrow"},
    )


class _ScriptedGateway:
    """Gateway double: per-contract ERC-20 balances and eth_call return words.

    ``eth_call`` returns, in priority order: a scripted per-``to`` word, else an
    identity ``convertToAssets`` (assets == shares argument) so ERC-4626 sums
    are easy to reason about.
    """

    def __init__(self, *, balances=None, call_words=None):
        self._balances = {k.lower(): v for k, v in (balances or {}).items()}
        self._call_words = {k.lower(): v for k, v in (call_words or {}).items()}
        self.balance_calls: list[dict] = []
        self.eth_calls: list[dict] = []

    def query_erc20_balance(self, **kwargs):
        self.balance_calls.append(kwargs)
        return self._balances.get(kwargs["token_address"].lower(), 0)

    def eth_call(self, **kwargs):
        self.eth_calls.append(kwargs)
        word = self._call_words.get(kwargs["to"].lower())
        if word is None:
            # Identity convertToAssets: echo the uint argument back.
            word = int(kwargs["data"][10:], 16)
        if word == "FAULT":
            return None
        return "0x" + format(word, "064x")


class TestEulerV2Hook:
    def test_supply_closed_when_all_matching_vaults_empty(self):
        gateway = _ScriptedGateway()
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is True
        # ALL catalogued USDC vaults were checked, not just the preferred one.
        assert len(_AVAX_USDC_VAULTS) > 1
        checked = {c["token_address"].lower() for c in gateway.balance_calls}
        assert checked == {v.lower() for v in _AVAX_USDC_VAULTS}

    def test_residual_in_non_preferred_vault_is_caught(self):
        stray_vault = _AVAX_USDC_VAULTS[-1]
        gateway = _ScriptedGateway(balances={stray_vault: 5_000_000})
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual["residual_wei"] == 5_000_000

    def test_debt_leg_reads_debt_of_on_matching_vaults(self):
        gateway = _ScriptedGateway(call_words=dict.fromkeys(_AVAX_USDC_VAULTS, 0))
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "BORROW", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is True
        assert len(gateway.eth_calls) == len(_AVAX_USDC_VAULTS)
        for call in gateway.eth_calls:
            assert call["data"] == DEBT_OF_SELECTOR + f"{int(_WALLET, 16):064x}"
            assert call["block"] == 1

    def test_residual_debt_is_measured_open(self):
        gateway = _ScriptedGateway(call_words={_AVAX_USDC_VAULTS[0]: 7_000_000, **dict.fromkeys(_AVAX_USDC_VAULTS[1:], 0)})
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "BORROW", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is False
        assert result.residual == {"asset": "USDC", "leg": "debt", "residual_wei": 7_000_000}

    def test_uncatalogued_chain_is_unmeasured(self):
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "SUPPLY", "bsc", "USDC"), _WALLET, gateway_client=_ScriptedGateway(), block=1
        )
        assert result.unmeasured is True
        assert result.closed is False

    def test_uncatalogued_asset_is_unmeasured(self):
        result = euler_v2_teardown_post_condition(
            _pos("euler_v2", "SUPPLY", "ethereum", "PEPE"), _WALLET, gateway_client=_ScriptedGateway(), block=1
        )
        assert result.unmeasured is True


class TestSiloV2Hook:
    def test_recorded_field_case_1000_shares_worth_1_wei_is_closed(self):
        """The ticket's silo residue: 1,000 leftover shares ≈ 1 wei of USDC."""
        usdc_silos = [silo for _m, silo, _i in _TOKEN_TO_SILO_MAP["USDC"]]
        gateway = _ScriptedGateway(
            balances=dict.fromkeys(usdc_silos, 1000),
            call_words=dict.fromkeys(usdc_silos, 1),  # convertToAssets(1000) -> 1 wei
        )
        result = silo_v2_teardown_post_condition(
            _pos("silo_v2", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=2
        )
        assert result.closed is True

    def test_all_markets_of_the_asset_are_read(self):
        # WAVAX appears in several markets; first-match-only would miss a
        # residual in a second market.
        assert len(_WAVAX_SILOS) > 1
        gateway = _ScriptedGateway()
        result = silo_v2_teardown_post_condition(
            _pos("silo_v2", "SUPPLY", "avalanche", "WAVAX"), _WALLET, gateway_client=gateway, block=2
        )
        assert result.closed is True
        checked = {c["token_address"].lower() for c in gateway.balance_calls}
        assert checked == {s.lower() for s in _WAVAX_SILOS}

    def test_residual_in_second_market_is_caught(self):
        gateway = _ScriptedGateway(balances={_WAVAX_SILOS[-1]: 10**18})
        result = silo_v2_teardown_post_condition(
            _pos("silo_v2", "SUPPLY", "avalanche", "WAVAX"), _WALLET, gateway_client=gateway, block=2
        )
        assert result.closed is False
        assert result.residual["residual_wei"] == 10**18

    def test_debt_leg_sums_max_repay_across_markets(self):
        # Residuals far above any plausible dust floor so the test always
        # exercises the measured-open branch regardless of catalogue size.
        gateway = _ScriptedGateway(
            call_words={_WAVAX_SILOS[0]: 3_000_000, **dict.fromkeys(_WAVAX_SILOS[1:], 4_000_000)}
        )
        result = silo_v2_teardown_post_condition(
            _pos("silo_v2", "BORROW", "avalanche", "WAVAX"), _WALLET, gateway_client=gateway, block=2
        )
        expected = 3_000_000 + 4_000_000 * (len(_WAVAX_SILOS) - 1)
        assert result.closed is False
        assert result.residual == {"asset": "WAVAX", "leg": "debt", "residual_wei": expected}

    def test_non_avalanche_chain_is_unmeasured(self):
        result = silo_v2_teardown_post_condition(
            _pos("silo_v2", "SUPPLY", "ethereum", "USDC"), _WALLET, gateway_client=_ScriptedGateway(), block=2
        )
        assert result.unmeasured is True


def _snapshot_blob(error: int, qi_balance: int, borrow_balance: int, exchange_rate: int) -> str:
    return "0x" + "".join(format(v, "064x") for v in (error, qi_balance, borrow_balance, exchange_rate))


class _BenqiGateway:
    def __init__(self, blob):
        self._blob = blob
        self.eth_calls: list[dict] = []

    def eth_call(self, **kwargs):
        self.eth_calls.append(kwargs)
        return self._blob


# ~0.02 underlying per qiToken — a realistic Compound-V2 mantissa scale.
_RATE = 20_000_000_000_000_000


class TestBenqiHook:
    def test_flat_account_is_closed_and_block_pinned(self):
        gateway = _BenqiGateway(_snapshot_blob(0, 0, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=777
        )
        assert result.closed is True
        call = gateway.eth_calls[0]
        assert call["to"] == BENQI_QI_TOKENS["USDC"]["qi_token"]
        assert call["block"] == 777

    def test_supply_residual_uses_exchange_rate_math(self):
        qi_balance = 5_000_000_000  # 8-decimal qiToken units
        gateway = _BenqiGateway(_snapshot_blob(0, qi_balance, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is False
        assert result.residual["residual_wei"] == qi_balance * _RATE // 10**18

    def test_borrow_residual_is_borrow_balance_word(self):
        gateway = _BenqiGateway(_snapshot_blob(0, 0, 123_456, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "BORROW", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is False
        assert result.residual == {"asset": "USDC", "leg": "debt", "residual_wei": 123_456}

    def test_snapshot_error_word_is_unmeasured_not_a_value(self):
        gateway = _BenqiGateway(_snapshot_blob(1, 0, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "USDC"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.unmeasured is True
        assert result.closed is False

    def test_wavax_alias_resolves_to_qiavax(self):
        gateway = _BenqiGateway(_snapshot_blob(0, 0, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "WAVAX"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is True
        assert gateway.eth_calls[0]["to"] == BENQI_QI_TOKENS["AVAX"]["qi_token"]

    def test_case_insensitive_catalogue_lookup(self):
        gateway = _BenqiGateway(_snapshot_blob(0, 0, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "btc.b"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.closed is True
        assert gateway.eth_calls[0]["to"] == BENQI_QI_TOKENS["BTC.b"]["qi_token"]

    def test_uncatalogued_asset_is_unmeasured(self):
        gateway = _BenqiGateway(_snapshot_blob(0, 0, 0, _RATE))
        result = benqi_teardown_post_condition(
            _pos("benqi", "SUPPLY", "avalanche", "PEPE"), _WALLET, gateway_client=gateway, block=1
        )
        assert result.unmeasured is True
        assert gateway.eth_calls == []


class TestUnreachableTodayAssumptionsAreBoundToCI:
    """Bind the hooks' documented "unreachable via intents today" assumptions.

    The silo supply read sees only borrowable-collateral shares (Protected-type
    deposits mint a sibling share token it cannot see), and the euler reads use
    the main account only (EVC sub-accounts are not read). Both are safe ONLY
    while the compiler vocabulary cannot produce those shapes. These guards
    fail loudly the moment either surface is plumbed, forcing the hook to be
    extended in the same change instead of silently regressing into a
    false-CHAIN_VERIFIED vector (pr-auditor Important #1, VIB-5795).
    """

    def test_silo_protected_collateral_type_is_not_plumbed(self):
        import inspect

        import almanak.connectors.silo_v2.compiler as silo_compiler

        src = inspect.getsource(silo_compiler)
        assert "collateral_type" not in src and "COLLATERAL_TYPE" not in src, (
            "silo_v2 compiler now plumbs a collateralType — extend "
            "silo_v2_teardown_post_condition to read the "
            "ShareProtectedCollateralToken before shipping this, or Protected "
            "deposits will verify as a false clean close."
        )

    def test_euler_sub_accounts_are_not_plumbed(self):
        import inspect

        import almanak.connectors.euler_v2.adapter as euler_adapter
        import almanak.connectors.euler_v2.compiler as euler_compiler

        for module in (euler_compiler, euler_adapter):
            src = inspect.getsource(module)
            assert "sub_account" not in src and "subaccount" not in src.lower().replace("_", ""), (
                f"{module.__name__} now plumbs EVC sub-accounts — extend "
                "euler_v2_teardown_post_condition to read per sub-account "
                "before shipping this, or sub-account positions will verify "
                "as a false clean close."
            )
