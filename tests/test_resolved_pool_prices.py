from fractions import Fraction
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome.math import quote
from almanak.connectors.curve import stable_math
from almanak.connectors.resolved_pool_prices import aerodrome_price, curve_prices

USDC = SimpleNamespace(identifier="eip155:8453/erc20:0x01", decimals=6)
WETH = SimpleNamespace(identifier="eip155:8453/erc20:0x02", decimals=18)
DAI = SimpleNamespace(identifier="eip155:8453/erc20:0x03", decimals=18)


def value(entry):
    return Fraction(int(entry["numerator"]), int(entry["denominator"]))


def aerodrome(stable, reserve0, reserve1, currency0=USDC, currency1=WETH):
    return SimpleNamespace(
        source=SimpleNamespace(stable=stable),
        currency0=currency0,
        currency1=currency1,
        reserve0=reserve0,
        reserve1=reserve1,
    )


def test_a_volatile_aerodrome_price_is_the_whole_unit_reserve_ratio():
    entry = aerodrome_price(aerodrome(False, 3_000 * 10**6, 10**18))
    assert (entry["base"], entry["quote"], entry["kind"]) == (
        USDC.identifier,
        WETH.identifier,
        "spot",
    )
    assert value(entry) == Fraction(1, 3_000)


@pytest.mark.parametrize("reserves", [(10**24, 10**24), (2 * 10**24, 10**24), (10**24, 3 * 10**24)])
def test_a_stable_aerodrome_price_matches_a_tiny_fee_free_swap(reserves):
    pool = aerodrome(True, *reserves, currency0=DAI, currency1=WETH)
    price = value(aerodrome_price(pool))
    amount = 10**12
    output = quote(True, 10**18, 10**18, *reserves, amount, 0, True)
    assert abs(Fraction(output, amount) - price) < Fraction(1, 10**6)
    assert (price == 1) == (reserves[0] == reserves[1])


def test_an_empty_aerodrome_reserve_has_no_price():
    assert aerodrome_price(aerodrome(False, 0, 10**18)) is None


def stable(balances, currencies, amplification=200_000, rates=None):
    state = stable_math.StableState(
        balances=balances,
        rates=rates or tuple(10 ** (36 - coin.decimals) for coin in currencies),
        amplification=amplification,
        fee=0,
        offpeg_multiplier=10**10,
        total_supply=sum(balances),
    )
    return SimpleNamespace(
        source=SimpleNamespace(family="stableswap_ng"),
        currencies=currencies,
        math_state=lambda: state,
    ), state


def test_a_balanced_stableswap_pool_prices_each_coin_at_par():
    observation, _ = stable((10**24, 10**12, 10**24), (DAI, USDC, DAI))
    entries = curve_prices(observation)
    assert [(e["base"], e["quote"], e["kind"]) for e in entries] == [
        (USDC.identifier, DAI.identifier, "spot"),
        (DAI.identifier, DAI.identifier, "spot"),
    ]
    assert [value(e) for e in entries] == [1, 1]


@pytest.mark.parametrize("amplification", [1_000, 200_000])
def test_an_imbalanced_stableswap_price_matches_a_tiny_fee_free_swap(amplification):
    observation, state = stable((3 * 10**24, 10**12), (DAI, USDC), amplification)
    (entry,) = curve_prices(observation)
    price = value(entry)
    amount = 10**6
    output = stable_math.swap(state, 1, 0, amount).output
    traded = Fraction(output, 10**18) / Fraction(amount, 10**6)
    assert price > 1
    assert abs(traded - price) < Fraction(1, 10**6)


def test_a_stableswap_price_converts_an_oracle_rate_into_whole_coins():
    # Equal normalized balances are at par per normalized unit; coin 1's rate doubles
    # each whole coin's normalized size, so one whole coin 1 is worth two of coin 0.
    observation, state = stable((2 * 10**24, 10**24), (DAI, DAI), rates=(10**18, 2 * 10**18))
    assert state.normalized[0] == state.normalized[1]
    (entry,) = curve_prices(observation)
    assert value(entry) == 2
    output = stable_math.swap(state, 1, 0, 10**12).output
    assert abs(Fraction(output, 10**12) - 2) < Fraction(1, 10**6)


def test_a_stableswap_pool_with_an_empty_balance_has_no_price():
    observation, _ = stable((10**24, 0), (DAI, USDC))
    assert curve_prices(observation) is None


def crypto(family, currencies, last, oracle):
    return SimpleNamespace(
        source=SimpleNamespace(family=family),
        currencies=currencies,
        last_prices=last,
        price_oracle=oracle,
    )


def test_a_twocrypto_pool_reports_its_last_spot_and_oracle_prices():
    entries = curve_prices(crypto("twocrypto_ng", (USDC, WETH), 2_500 * 10**18, 2_400 * 10**18))
    assert [(e["base"], e["quote"], e["kind"], value(e)) for e in entries] == [
        (WETH.identifier, USDC.identifier, "last_spot", 2_500),
        (WETH.identifier, USDC.identifier, "oracle", 2_400),
    ]


def test_a_tricrypto_pool_prices_each_later_coin_in_coin_zero():
    entries = curve_prices(
        crypto(
            "tricrypto_ng", (USDC, WETH, DAI), (2 * 10**18, 3 * 10**18), (4 * 10**18, 5 * 10**18)
        )
    )
    assert [(e["base"], e["kind"], value(e)) for e in entries] == [
        (WETH.identifier, "last_spot", 2),
        (WETH.identifier, "oracle", 4),
        (DAI.identifier, "last_spot", 3),
        (DAI.identifier, "oracle", 5),
    ]


def test_aerodrome_reserves_are_whole_units_of_each_currency():
    from types import SimpleNamespace

    from almanak.connectors.resolved_pool_prices import aerodrome_reserves

    weth = SimpleNamespace(identifier="eip155:8453/erc20:0x42", decimals=18)
    usdc = SimpleNamespace(identifier="eip155:8453/erc20:0x83", decimals=6)
    observation = SimpleNamespace(
        currency0=weth, currency1=usdc, reserve0=202 * 10**18, reserve1=473_000 * 10**6
    )
    assert aerodrome_reserves(observation) == [
        {
            "asset": "eip155:8453/erc20:0x42",
            "amount": str(202 * 10**18),
            "decimals": 18,
            "quantity": "202",
        },
        {
            "asset": "eip155:8453/erc20:0x83",
            "amount": str(473_000 * 10**6),
            "decimals": 6,
            "quantity": "473000",
        },
    ]
    # Every digit survives, beyond any decimal context's precision.
    large = SimpleNamespace(
        currency0=weth, currency1=usdc, reserve0=123456789012345678901234567890123, reserve1=5
    )
    assert [value["quantity"] for value in aerodrome_reserves(large)] == [
        "123456789012345.678901234567890123",
        "0.000005",
    ]


def test_exact_decimal_keeps_every_digit_trimmed_or_at_full_scale():
    from almanak.report_values import exact_decimal

    assert exact_decimal(1_500_000, 6) == "1.5"
    assert exact_decimal(1_500_000, 6, trim=False) == "1.500000"
    assert exact_decimal(0, 6, trim=False) == "0.000000"
    assert exact_decimal(-5, 2) == "-0.05"
    assert exact_decimal(42, 0) == exact_decimal(42, 0, trim=False) == "42"


def test_exact_decimal_scales_up_for_non_positive_decimals():
    from almanak.report_values import exact_decimal

    assert exact_decimal(5, -3) == "5000"
    assert exact_decimal(-5, -3, trim=False) == "-5000"


def test_aerodrome_reserves_render_as_report_quantities():
    from types import SimpleNamespace

    from almanak.connectors.resolved_pool_prices import aerodrome_reserves
    from almanak.report_values import quantity

    observation = SimpleNamespace(
        currency0=WETH, currency1=USDC, reserve0=2025 * 10**17, reserve1=473_000_120_000
    )
    metadata = {
        WETH.identifier: {"decimals": 18, "symbol": "WETH"},
        USDC.identifier: {"decimals": 6, "symbol": "USDC"},
    }
    assert [quantity(value, metadata) for value in aerodrome_reserves(observation)] == [
        (WETH.identifier, "202.5 WETH"),
        (USDC.identifier, "473000.12 USDC"),
    ]
