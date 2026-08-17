"""Address-first oracle join in the compiler price lookup (ALM-3174).

Post symbol-deprecation, strategies reference tokens by contract address and
the snapshot warms the oracle under ``chain:0xaddr`` keys. The symbol-first
lookup could only bridge those keys back through the static registry, so a
registry-unknown token (ALM-3173: SPCXB on BSC) failed slippage compilation
with its price sitting in the oracle. ``require_token_price_for`` joins on
the address the compiler already holds; these tests pin that contract.

Negative control: reverting the address-first join makes
``test_registry_unknown_token_prices_via_chain_addr_key`` (and the
``calculate_expected_output`` test) fail — the symbol path cannot name a
token the static registry does not contain.
"""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TokenInfo
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.models.reproduction_bundle import ActionBundle

# ALM-3173: real registry-unknown token (SPCXB on BSC). Absent from
# data/tokens.json — the point of these tests. If it ever gets added to the
# registry, swap in any address that is not registered on BSC.
SPCXB_ADDR = "0xbe9d156892e55e7154bcd3cb0fea677f9d3103e1"
USDT_BSC_ADDR = "0x55d398326f99059ff775485246999027b3197955"


def _compiler(price_oracle: dict | None, chain: str = "bsc") -> IntentCompiler:
    return IntentCompiler(
        chain=chain,
        price_oracle=price_oracle,
        config=IntentCompilerConfig(allow_placeholder_prices=False),
    )


def _spcxb(decimals: int = 18) -> TokenInfo:
    return TokenInfo(symbol="SPCXB", address=SPCXB_ADDR, decimals=decimals)


class TestAddressFirstLookup:
    def test_registry_unknown_token_prices_via_chain_addr_key(self):
        """The export shape: uppercased ``CHAIN:0XADDR`` key prices the token."""
        compiler = _compiler({f"BSC:{SPCXB_ADDR.upper()}": Decimal("115.013")})
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115.013")

    def test_bare_address_key_hits(self):
        compiler = _compiler({SPCXB_ADDR.upper(): Decimal("115.013")})
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115.013")

    def test_hand_built_lowercase_key_hits(self):
        """Hand-built oracle dicts are not uppercased — scan must cover them."""
        compiler = _compiler({f"bsc:{SPCXB_ADDR}": Decimal("115.013")})
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115.013")

    def test_chain_qualified_key_beats_bare_address_key(self):
        """With both key forms present, the active-chain entry wins."""
        compiler = _compiler(
            {
                SPCXB_ADDR.upper(): Decimal("999"),
                f"BSC:{SPCXB_ADDR.upper()}": Decimal("115"),
            }
        )
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115")

    def test_chain_qualified_precedence_holds_in_scan_fallback(self):
        """Same precedence for hand-built lowercase keys (scan path)."""
        compiler = _compiler(
            {
                SPCXB_ADDR: Decimal("999"),
                f"bsc:{SPCXB_ADDR}": Decimal("115"),
            }
        )
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115")

    def test_active_chain_disambiguates_same_address_on_another_chain(self):
        """A same-address entry on another chain can never win by order."""
        compiler = _compiler(
            {
                f"ethereum:{SPCXB_ADDR}": Decimal("999"),
                f"bsc:{SPCXB_ADDR}": Decimal("115"),
            }
        )
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115")

    def test_address_key_wins_over_symbol_key(self):
        """The address is the more precise identity — it takes precedence."""
        compiler = _compiler(
            {
                f"BSC:{SPCXB_ADDR.upper()}": Decimal("115"),
                "SPCXB": Decimal("999"),
            }
        )
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115")

    def test_symbol_key_still_resolves_without_address_key(self):
        """Symbol-form oracles (legacy strategies) behave as before."""
        compiler = _compiler({"SPCXB": Decimal("115")})
        price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("115")

    @pytest.mark.parametrize(
        ("address", "decimals"),
        [
            ("not-an-address", 18),
            (SPCXB_ADDR, 78),
        ],
    )
    def test_invalid_identity_keeps_measured_symbol_fallback(self, address: str, decimals: int):
        compiler = _compiler({"SPCXB": Decimal("115")})
        token = TokenInfo(symbol="SPCXB", address=address, decimals=decimals)

        assert compiler._queries.require_token_price_for(token) == Decimal("115")

    def test_missing_everywhere_still_fails_closed(self):
        compiler = _compiler({"USDT": Decimal("1")})
        with pytest.raises(ValueError, match="missing in the price oracle"):
            compiler._queries.require_token_price_for(_spcxb())

    def test_zero_address_price_is_a_miss_not_a_hit(self):
        """Zero under the address key blocks every lower-priority fallback."""
        compiler = _compiler({f"BSC:{SPCXB_ADDR.upper()}": Decimal("0")})
        with pytest.raises(ValueError, match="price oracle"):
            compiler._queries.require_token_price_for(_spcxb())

    def test_missing_pegged_identity_uses_registry_peg_and_records_provenance(self, caplog):
        compiler = _compiler({})
        usdt = TokenInfo(symbol="USDT", address=USDT_BSC_ADDR, decimals=18)

        with caplog.at_level("WARNING"):
            price = compiler._queries.require_token_price_for(usdt)

        assert price == Decimal("1")
        assert compiler._peg_fallbacks == {f"bsc:{USDT_BSC_ADDR}"}
        assert "used_peg=true" in caplog.text

    def test_measured_zero_for_pegged_identity_refuses_registry_peg(self):
        compiler = _compiler({f"bsc:{USDT_BSC_ADDR}": Decimal("0")})
        usdt = TokenInfo(symbol="USDT", address=USDT_BSC_ADDR, decimals=18)

        with pytest.raises(ValueError, match="refuses to replace measured zero"):
            compiler._queries.require_token_price_for(usdt)

    def test_measured_depeg_wins_over_registry_peg(self):
        compiler = _compiler({f"bsc:{USDT_BSC_ADDR}": Decimal("0.96")})
        usdt = TokenInfo(symbol="USDT", address=USDT_BSC_ADDR, decimals=18)

        assert compiler._queries.require_token_price_for(usdt) == Decimal("0.96")
        assert compiler._peg_fallbacks == set()

    def test_native_token_keeps_symbol_path(self):
        """Natives price by symbol; their sentinel address is not an oracle key."""
        native = TokenInfo(
            symbol="BNB",
            address="0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE",
            decimals=18,
            is_native=True,
        )
        compiler = _compiler({"BNB": Decimal("585.16")})
        price = compiler._queries.require_token_price_for(native)
        assert price == Decimal("585.16")


class TestCompilePathsUseAddressFirstJoin:
    def test_calculate_expected_output_prices_registry_unknown_token(self):
        """The ALM-3173 failure shape: USDT -> SPCXB slippage calculation.

        USDT prices via its symbol key, SPCXB only via its address key.
        Before the address-first join this raised ``Price for 'SPCXB' is
        missing in the price oracle`` and the forced buy failed.
        """
        compiler = _compiler(
            {
                "USDT": Decimal("1"),
                f"BSC:{SPCXB_ADDR.upper()}": Decimal("115"),
            }
        )
        usdt = TokenInfo(symbol="USDT", address=USDT_BSC_ADDR, decimals=18)
        amount_in = 230 * 10**18  # 230 USDT
        expected_output = compiler._calculate_expected_output(amount_in, usdt, _spcxb())
        # $230 / $115 = 2 SPCXB, minus the 0.3% fee estimate.
        assert expected_output == int(Decimal(2) * Decimal("0.997") * 10**18)

    def test_usd_to_token_amount_prices_registry_unknown_token(self):
        compiler = _compiler({f"BSC:{SPCXB_ADDR.upper()}": Decimal("115")})
        amount = compiler._queries.usd_to_token_amount(Decimal("230"), _spcxb())
        assert amount == 2 * 10**18

    def test_symbol_fallback_routes_through_host_wrapper(self):
        """Seam contract: on address-key miss the lookup must go through
        ``compiler._require_token_price`` so instance patches propagate
        (test_compiler_queries_extraction.py seam)."""
        compiler = _compiler({})
        with patch.object(compiler, "_require_token_price", return_value=Decimal("3")):
            price = compiler._queries.require_token_price_for(_spcxb())
        assert price == Decimal("3")

    def test_compile_stamps_peg_provenance_on_result_and_bundle(self):
        compiler = _compiler({})
        usdt = TokenInfo(symbol="USDT", address=USDT_BSC_ADDR, decimals=18)

        def compile_with_peg(_intent):
            compiler._require_token_price_for(usdt)
            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                action_bundle=ActionBundle(intent_type="TEST"),
            )

        with patch.object(compiler, "_compile_intent", side_effect=compile_with_peg):
            result = compiler.compile(SimpleNamespace(intent_type=IntentType.HOLD))  # type: ignore[arg-type]

        identity = f"bsc:{USDT_BSC_ADDR}"
        assert result.used_peg is True
        assert result.peg_tokens == [identity]
        assert result.action_bundle is not None
        assert result.action_bundle.metadata["price_provenance"] == {
            "used_peg": True,
            "peg_tokens": [identity],
        }

    def test_compile_lazily_initializes_tracking_for_lightweight_subclass(self):
        """Compiler test doubles that skip the base initializer still dispatch."""
        compiler = object.__new__(IntentCompiler)
        compiled = CompilationResult(
            status=CompilationStatus.SUCCESS,
            action_bundle=ActionBundle(intent_type="TEST"),
        )

        with patch.object(compiler, "_compile_intent", return_value=compiled):
            result = compiler.compile(SimpleNamespace(intent_type=IntentType.HOLD))  # type: ignore[arg-type]

        assert result.used_peg is False
        assert result.peg_tokens == []
        assert compiler._peg_tracking_depth == 0
        assert compiler._peg_fallbacks == set()
