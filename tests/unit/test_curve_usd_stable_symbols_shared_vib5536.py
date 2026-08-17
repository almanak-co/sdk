"""ALM-3190: Curve accounting and NAV share the address-keyed peg registry."""

from decimal import Decimal

from almanak.framework.data.tokens.models import TokenRef
from almanak.framework.data.tokens.pegs import PEG_REGISTRY, is_pegged


def _ref(address: str, symbol: str) -> TokenRef:
    return TokenRef(chain="ethereum", address=address, decimals=18, symbol=symbol)


def test_curve_consumers_share_the_central_identity_api() -> None:
    from almanak.framework.accounting.category_handlers import lp_handler
    from almanak.framework.valuation import curve_lp_position_reader

    assert lp_handler.is_pegged is is_pegged
    assert curve_lp_position_reader.is_pegged is is_pegged


def test_registry_is_address_keyed_and_includes_soft_peg_identity() -> None:
    mim = _ref("0x99D8a9C45b2ecA8864373A26D1459e3Dff1e17F3", "MIM")
    squatter = _ref("0x1111111111111111111111111111111111111111", "MIM")

    assert mim.identity_key in PEG_REGISTRY
    assert is_pegged(mim) == Decimal("1")
    assert is_pegged(squatter) is None


def test_yield_bearing_dollar_tokens_are_not_par_pegged() -> None:
    sdai = _ref("0x83F20F44975D03b1b09e64809B757c47f942BEeA", "SDAI")
    susde = _ref("0x9D39A5DE30e57443BfF2A8307A4256c8797A3497", "SUSDE")

    assert is_pegged(sdai) is None
    assert is_pegged(susde) is None
