"""Decoded pristine-authority regressions for the actual Robinhood V3 references."""

from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v3 import abi
from tests.exchange_scope import assert_scope_only
from tests.strategy_fork import robinhood_joint_authority as authority
from tests.test_bnb_v3_pristine_authority import (
    decode_approval,
    decode_collect,
    decode_exit,
    decode_increase,
    decode_mint,
    decode_multicall,
    decode_swap,
    selector,
)
from tests.test_robinhood_joint_authority import ROOT, prepared_config

CANDIDATES = ROOT / "tests/fixtures/author/candidates"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


def prepared(name):
    return prepared_config(CANDIDATES / name / "config.yaml")


def v3(config):
    return (
        config.component(ConnectorId("uniswap_v3"))
        if getattr(config, "connector", None) is None
        else config
    )


def groups(config):
    return {group.name: group for group in authority.v3_groups(v3(config))}


def payload(call):
    return bytes.fromhex(call.data[2:])


def test_actual_references_derive_scope_only_v3_grants():
    # Blueprint 27: no Roles allowance, and no grant depends on the allocation's capital.
    for name in (
        "uniswap_v3_robinhood_trading",
        "uniswap_v3_robinhood_lp",
        "robinhood_morpho_borrow_and_trade_v3",
    ):
        config = v3(prepared(name))
        permissions = connector_for(config).permissions
        profile = config.accounts.binding(config.allocations[0].account).profile
        grants = permissions.derive(profile, permissions.codec.declare(config))
        assert_scope_only(grants)
        allocation = config.allocations[0]
        resized = allocation.capital.model_copy(update={"amount": allocation.capital.amount * 7})
        other = config.model_copy(
            update={"allocations": (allocation.model_copy(update={"capital": resized}),)}
        )
        assert permissions.derive(profile, permissions.codec.declare(other)) == grants


def test_actual_token_order_and_both_swap_directions_are_nonvacuous():
    for name, allocation_name in (
        ("uniswap_v3_robinhood_trading", "position"),
        ("robinhood_morpho_borrow_and_trade_v3", "trade"),
    ):
        config = v3(prepared(name))
        target = config.target(allocation_name)
        profile = config.accounts.binding(target.account).profile
        selected = groups(config)
        assert tuple(asset.reference for asset in target.assets) == (WETH, USDG)
        assert target.funding_currency.reference == USDG
        swap = selected["uniswap_v3_swap"]
        controls = []
        for direction, call in enumerate(swap.controls):
            deadline, nested = decode_swap(payload(call))
            baseline = (
                target.assets[direction].reference,
                target.assets[1 - direction].reference,
                500,
                target.fund.address,
                1,
                1,
                0,
            )
            assert (deadline, nested) == (2**64, (baseline,))
            controls.append(baseline)
        assert controls[0][:2] == (WETH, USDG)
        assert controls[1][:2] == (USDG, WETH)
        attacks = dict(swap.attacks)
        for direction, baseline in enumerate(controls):
            changes = {
                "wrong_input": (0, target.assets[1 - direction].reference),
                "wrong_output": (1, target.assets[direction].reference),
                "wrong_fee": (2, 501),
                "wrong_recipient": (3, profile.submitter.address),
                "zero_input": (4, 0),
                "price_limit": (6, 1),
            }
            for label, (index, replacement) in changes.items():
                _, nested = decode_swap(payload(attacks[f"v3_swap{direction}_{label}"]))
                changed = list(baseline)
                changed[index] = replacement
                assert nested == (tuple(changed),) and tuple(changed) != baseline
            _, nested = decode_swap(payload(attacks[f"v3_swap{direction}_extra_nested_call"]))
            assert nested == (baseline, baseline)
        approvals = selected["uniswap_v3_router_approval"]
        assert_any_amount_approvals(approvals, target, target.contracts.router, profile, "router")


def test_lp_mint_checks_both_robinhood_token_fields_and_optional_deposits():
    config = v3(prepared("uniswap_v3_robinhood_lp"))
    target = config.target("position")
    profile = config.accounts.binding("fund").profile
    selected = groups(config)
    mint = selected["uniswap_v3_mint"]
    baseline = (
        WETH,
        USDG,
        500,
        -198620,
        -197620,
        1,
        1,
        0,
        0,
        target.fund.address,
        2**64,
    )
    large = 2**128 - 1
    # Ticks and deposit amounts are gateway checks (blueprint 27 section 4.3).
    assert tuple(decode_mint(payload(call)) for call in mint.controls) == (
        baseline,
        (*baseline[:5], 0, 1, *baseline[7:]),
        (*baseline[:5], 1, 0, *baseline[7:]),
        (*baseline[:3], -198610, -197610, *baseline[5:]),
        (*baseline[:5], large, large, *baseline[7:]),
    )
    attacks = dict(mint.attacks)
    assert not {"v3_mint_wrong_lower", "v3_mint_wrong_upper"} & set(attacks)
    changes = {
        "wrong_token0": (0, USDG),
        "wrong_token1": (1, WETH),
        "wrong_fee": (2, 501),
        "wrong_recipient": (9, profile.submitter.address),
    }
    for label, (index, replacement) in changes.items():
        actual = decode_mint(payload(attacks[f"v3_mint_{label}"]))
        expected = list(baseline)
        expected[index] = replacement
        assert actual == tuple(expected) and actual != baseline
    assert decode_mint(payload(attacks["v3_mint_wrong_token0"]))[:2] == (USDG, USDG)
    assert decode_mint(payload(attacks["v3_mint_wrong_token1"]))[:2] == (WETH, WETH)


def test_lp_broad_nft_controls_match_generated_roles_scope():
    config = v3(prepared("uniswap_v3_robinhood_lp"))
    target = config.target("position")
    profile = config.accounts.binding("fund").profile
    selected = groups(config)
    token0, token1, token2 = 2**200, 2**200 + 1, 2**200 + 2
    increase = selected["uniswap_v3_increase"]
    assert tuple(decode_increase(payload(call)) for call in increase.controls) == (
        (token0, 1, 1, 0, 0, 2**64),
        (token0, 0, 1, 0, 0, 2**64),
        (token0, 1, 0, 0, 0, 2**64),
        (token0, 2**128 - 1, 2**128 - 1, 0, 0, 2**64),
    )
    increase_attacks = dict(increase.attacks)
    assert set(increase_attacks) == {"v3_increase_zero_nft"}
    assert decode_increase(payload(increase_attacks["v3_increase_zero_nft"]))[0] == 0
    collect = selected["uniswap_v3_collect"]
    assert decode_collect(payload(collect.controls[0])) == (
        token0,
        target.fund.address,
        abi.MAX128,
        abi.MAX128,
    )
    collect_attacks = dict(collect.attacks)
    assert decode_collect(payload(collect_attacks["v3_collect_zero_nft"]))[0] == 0
    assert decode_collect(payload(collect_attacks["v3_collect_wrong_recipient"]))[1] == (
        profile.submitter.address
    )
    assert decode_collect(payload(collect_attacks["v3_collect_nonmax_amount0"]))[2] == (
        abi.MAX128 - 1
    )
    assert decode_collect(payload(collect_attacks["v3_collect_nonmax_amount1"]))[3] == (
        abi.MAX128 - 1
    )
    exits = tuple(decode_exit(payload(call)) for call in selected["uniswap_v3_exit"].controls)
    (
        partial,
        close,
        uncorrelated_partial,
        uncorrelated_close,
        zero_liquidity,
        zero_nft_partial,
        zero_nft_close,
    ) = exits
    assert (partial[0][0], partial[1][0], partial[2]) == (token0, token0, None)
    assert (close[0][0], close[1][0], close[2]) == (token0, token0, token0)
    assert (uncorrelated_partial[0][0], uncorrelated_partial[1][0], uncorrelated_partial[2]) == (
        token0,
        token1,
        None,
    )
    assert (uncorrelated_close[0][0], uncorrelated_close[1][0], uncorrelated_close[2]) == (
        token0,
        token1,
        token2,
    )
    assert zero_liquidity[0][1] == 0
    assert (zero_nft_partial[0][0], zero_nft_partial[1][0], zero_nft_partial[2]) == (
        0,
        0,
        None,
    )
    assert (zero_nft_close[0][0], zero_nft_close[1][0], zero_nft_close[2]) == (0, 0, 0)
    attacks = dict(selected["uniswap_v3_exit"].attacks)
    assert "v3_exit_zero_nft" not in attacks
    wrong_recipient = decode_exit(payload(attacks["v3_exit_wrong_recipient"]))
    assert wrong_recipient[1][1] == profile.submitter.address
    wrong_order = decode_multicall(payload(attacks["v3_exit_wrong_order"]))
    assert tuple(call[:4] for call in wrong_order) == (
        selector(f"collect({abi.COLLECT_PARAMS})"),
        selector(f"decreaseLiquidity({abi.DECREASE_PARAMS})"),
        selector("burn(uint256)"),
    )
    extra = decode_multicall(payload(attacks["v3_exit_extra_call"]))
    assert len(extra) == 4 and extra[-1][:4] == selector("burn(uint256)")


def assert_any_amount_approvals(group, target, spender, profile, name):
    """Each asset approves the exact spender for one unit and for the largest amount."""
    expected = tuple(
        (asset.reference, (spender, amount))
        for asset in target.assets
        for amount in (1, 2**256 - 1)
    )
    assert tuple((c.to, decode_approval(payload(c))) for c in group.controls) == expected
    malformed = dict(group.attacks)
    assert set(malformed) == {f"v3_{name}_approval{index}_wrong_spender" for index in (0, 1)}
    for index, asset in enumerate(target.assets):
        call = malformed[f"v3_{name}_approval{index}_wrong_spender"]
        assert call.to == asset.reference
        assert decode_approval(payload(call)) == (profile.submitter.address, 1)


def test_lp_position_approvals_bind_both_assets_to_the_exact_spender():
    config = v3(prepared("uniswap_v3_robinhood_lp"))
    target = config.target("position")
    profile = config.accounts.binding("fund").profile
    approval = groups(config)["uniswap_v3_position_approval"]
    assert_any_amount_approvals(
        approval, target, target.contracts.position_manager, profile, "position"
    )
