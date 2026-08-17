"""ALM-3190 address-keyed stablecoin peg registry tests."""

from decimal import Decimal
from pathlib import Path

from almanak.framework.data.tokens import (
    PegClass,
    ResolvedToken,
    TokenRef,
    create_token_resolver,
    is_pegged,
    peg_for_identity,
)

ARBITRUM_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
ETHEREUM_SDAI = "0x83f20f44975d03b1b09e64809b757c47f942bea"
ETHEREUM_SUSDE = "0x9d39a5de30e57443bff2a8307a4256c8797a3497"
POLYGON_PUSD = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb"
ARBITRUM_PUSD_SQUATTER = "0xc8fb643d18f1e53698cfda5c8fdf0cdc03c1dbec"
ETHEREUM_MIM = "0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3"


def _ref(chain: str, address: str, symbol: str) -> TokenRef:
    return TokenRef(chain=chain, address=address, decimals=6, symbol=symbol)


def test_known_registry_identity_has_usd_peg() -> None:
    assert is_pegged(_ref("arbitrum", ARBITRUM_USDC, "NOT-USDC")) == Decimal("1")


def test_normalized_identity_helper_has_usd_peg() -> None:
    assert peg_for_identity("arbitrum", ARBITRUM_USDC.upper()) == Decimal("1")


def test_symbol_cannot_grant_peg_to_unknown_address() -> None:
    assert is_pegged(_ref("arbitrum", "0x" + "12" * 20, "USDC")) is None


def test_same_symbol_on_different_contracts_isolated() -> None:
    assert is_pegged(_ref("polygon", POLYGON_PUSD, "PUSD")) == Decimal("1")
    assert is_pegged(_ref("arbitrum", ARBITRUM_PUSD_SQUATTER, "PUSD")) is None


def test_yield_bearing_dollar_tokens_are_not_one_dollar_pegs() -> None:
    assert is_pegged(_ref("ethereum", ETHEREUM_SDAI, "SDAI")) is None
    assert is_pegged(_ref("ethereum", ETHEREUM_SUSDE, "SUSDE")) is None


def test_soft_peg_is_registered_by_exact_identity() -> None:
    assert is_pegged(_ref("ethereum", ETHEREUM_MIM, "MIM")) == Decimal("1")


def test_resolver_propagates_registry_peg_metadata(tmp_path: Path) -> None:
    resolver = create_token_resolver(cache_file=str(tmp_path / "tokens.json"))
    resolved = resolver.resolve(ARBITRUM_USDC, "arbitrum", skip_gateway=True)

    assert resolved.peg_class is PegClass.USD
    assert ResolvedToken.from_dict(resolved.to_dict()).peg_class is PegClass.USD
