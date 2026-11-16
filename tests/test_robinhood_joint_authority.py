"""Offline failure-semantics checks for the Robinhood installed-authority fixture."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.caip import AccountReference, ChainId
from almanak.configuration.unresolved import read_inputs
from almanak.connectors.composition import connector_for
from almanak.gateway.accounts import ContractCall
from almanak.models import Account, AccountBinding, Chain, SafeRoles, StrategyAccounts
from scripts.legacy_configuration.author import read_author
from tests.fork.test_robinhood_joint_authority import exact_authority_state
from tests.strategy_fork import robinhood_joint_authority as authority

ROOT = Path(__file__).parents[1]
INSTANCE = "0x" + "00" * 32


def profile():
    chain = Chain(chain_id=ChainId("eip155:4663"), environment="anvil:test")

    def account(number):
        return Account(chain=chain, address=AccountReference(f"0x{number:040x}"))

    return SafeRoles(
        kind="safe_roles",
        fund=account(1),
        operator=account(2),
        roles_module=account(3).address,
        role_key="0x" + "11" * 32,
    )


def rpc_error(status, info=b"\0" * 32):
    data = authority.CONDITION_VIOLATION + encode(("uint8", "bytes32"), (status, info)).hex()
    payload = {"code": 3, "message": "execution reverted", "data": data}
    return RuntimeError(authority.RPC_ERROR_PREFIX + repr(payload))


class ProbeNode:
    def __init__(self, outcomes):
        self.outcomes = iter(outcomes)
        self.calls = []

    def _validate_origin(self):
        return None

    def rpc(self, method, params):
        self.calls.append((method, params))
        outcome = next(self.outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return "0x" + encode(("bool", "bytes"), outcome).hex()


class BlockNode:
    def rpc(self, method, params):
        assert (method, params) == ("eth_getBlockByNumber", ["latest", False])
        return {"timestamp": "0x64"}


def prepared_config(path):
    fork = read_author(path).fork_config()
    chain = Chain(chain_id=fork.origin.chain.chain_id, environment="anvil:" + INSTANCE)
    bindings = []
    for index, fund in enumerate(fork.funds):

        def account(number):
            return Account(chain=chain, address=AccountReference(f"0x{number:040x}"))

        bindings.append(
            AccountBinding(
                name=fund.account,
                profile=SafeRoles(
                    kind="safe_roles",
                    fund=account(10 + index * 3),
                    operator=account(11 + index * 3),
                    roles_module=account(12 + index * 3).address,
                    role_key="0x" + f"{index + 1:064x}",
                ),
            )
        )
    return connector_for(fork).fork.prepare(fork, tuple(bindings), chain, INSTANCE, fork.origin)


def test_condition_violation_decodes_exact_status_and_info():
    info = bytes.fromhex("ab" * 32)
    data, status, name, decoded = authority.condition_violation(rpc_error(2, info))
    assert data.startswith(authority.CONDITION_VIOLATION)
    assert (status, name, decoded) == (2, "TargetAddressNotAllowed", "0x" + info.hex())


@pytest.mark.parametrize(
    "error",
    (
        RuntimeError("transport failed with 0xd0a9bf58 in its text"),
        RuntimeError(
            authority.RPC_ERROR_PREFIX
            + repr({"code": 3, "message": "execution reverted", "data": "0xd0a9bf58"})
        ),
        rpc_error(0),
    ),
)
def test_condition_violation_refuses_transport_substrings_and_malformed_payloads(error):
    with pytest.raises(AssertionError):
        authority.condition_violation(error)


def test_group_runs_positive_control_before_structured_denial(monkeypatch):
    safe = profile()
    call = ContractCall(fund=safe.fund, to=safe.fund.address, value=0, data="0x12345678")
    group = authority.ProbeGroup("swap", (call,), (("wrong_recipient", call),))
    monkeypatch.setattr(authority, "probe_groups", lambda node, config: (group,))
    config = SimpleNamespace(
        accounts=StrategyAccounts(bindings=(AccountBinding(name="fund", profile=safe),))
    )
    node = ProbeNode(((False, b"downstream"), rpc_error(7)))
    result = authority.run(node, config)
    template = result["templates"][0]
    assert template["controls"][0]["outcome"] == "authorized_downstream_refusal"
    assert template["rejected"][0]["status_name"] == "ParameterNotAllowed"
    assert [call[0] for call in node.calls] == ["eth_call", "eth_call"]


@pytest.mark.parametrize(
    ("path", "templates"),
    (
        (
            ROOT / "tests/fixtures/author/legacy/morpho_blue_ethereum_lending/config.yaml",
            {
                "morpho_earn_supply_assets",
                "morpho_earn_withdraw_assets",
                "morpho_earn_withdraw_shares",
                "morpho_collateral_supplyCollateral_assets",
                "morpho_collateral_withdrawCollateral_assets",
                "morpho_loan_borrow_assets",
                "morpho_loan_repay_assets",
                "morpho_loan_repay_shares",
            },
        ),
        (
            ROOT / "tests/fixtures/author/candidates/uniswap_v4_robinhood_lp/config.yaml",
            {
                "uniswap_v4_swap",
                "uniswap_v4_erc20_approval",
                "uniswap_v4_permit2_approval",
                "uniswap_v4_mint",
                "uniswap_v4_collect",
                "uniswap_v4_close",
            },
        ),
        (
            ROOT
            / "tests/fixtures/author/candidates/robinhood_morpho_borrow_and_trade_v4/config.yaml",
            {
                "morpho_collateral_supplyCollateral_assets",
                "morpho_collateral_withdrawCollateral_assets",
                "morpho_loan_borrow_assets",
                "morpho_loan_repay_assets",
                "morpho_loan_repay_shares",
                "uniswap_v4_swap",
                "uniswap_v4_erc20_approval",
                "uniswap_v4_permit2_approval",
            },
        ),
        (
            ROOT / "tests/fixtures/author/legacy/curve_robinhood_stable_lp/config.yaml",
            {"curve_swap", "curve_approval"},
        ),
        (
            ROOT / "tests/fixtures/author/candidates/uniswap_v3_robinhood_trading/config.yaml",
            {"uniswap_v3_swap", "uniswap_v3_router_approval"},
        ),
        (
            ROOT / "tests/fixtures/author/candidates/uniswap_v3_robinhood_lp/config.yaml",
            {
                "uniswap_v3_swap",
                "uniswap_v3_router_approval",
                "uniswap_v3_position_approval",
                "uniswap_v3_mint",
                "uniswap_v3_increase",
                "uniswap_v3_collect",
                "uniswap_v3_exit",
            },
        ),
        (
            ROOT
            / "tests/fixtures/author/candidates/robinhood_morpho_borrow_and_trade_v3/config.yaml",
            {
                "morpho_collateral_supplyCollateral_assets",
                "morpho_collateral_withdrawCollateral_assets",
                "morpho_loan_borrow_assets",
                "morpho_loan_repay_assets",
                "morpho_loan_repay_shares",
                "uniswap_v3_swap",
                "uniswap_v3_router_approval",
            },
        ),
    ),
)
def test_actual_resolved_configs_construct_every_expected_template_group(path, templates):
    groups = authority.probe_groups(BlockNode(), prepared_config(path))
    names = {group.name for group in groups}
    assert templates <= names
    assert all(group.controls or group.name.startswith("roles_outer_boundary_") for group in groups)
    assert all(group.attacks for group in groups)


def test_direct_v4_lp_and_trading_author_inputs_have_exact_shared_swap_semantics():
    lp = read_inputs(ROOT / "strategies/uniswap_v4_robinhood_lp/config.yaml").inputs
    trading = read_inputs(ROOT / "strategies/uniswap_v4_robinhood_trading/config.yaml").inputs
    assert lp.markets == trading.markets
    assert lp.accounts == trading.accounts
    lp_allocation = lp.strategy.allocations["position"].model_dump()
    trading_allocation = trading.strategy.allocations["position"].model_dump()
    for key in ("kind", "range", "collect_seconds"):
        lp_allocation.pop(key)
        trading_allocation.pop(key)
    assert lp_allocation == trading_allocation


def test_arbitrary_outer_boundaries_require_the_source_defined_statuses():
    safe = profile()
    call = ContractCall(fund=safe.fund, to=safe.fund.address, value=0, data="0x12345678")
    node = ProbeNode((rpc_error(2), rpc_error(3)))
    target = authority.denied(node, safe, "target", call, expected_status=2)
    function = authority.denied(node, safe, "function", call, expected_status=3)
    assert (target["status_name"], function["status_name"]) == (
        "TargetAddressNotAllowed",
        "FunctionNotAllowed",
    )


def test_authority_verification_is_exact_and_partition_complete():
    partition = SimpleNamespace(account="fund")
    manifest = SimpleNamespace(digest="expected", partitions=(partition,))
    verification = {"manifest": "expected", "partitions": [{"account": "fund"}]}
    assert exact_authority_state(manifest, verification, ("fund",)) == [{"account": "fund"}]
    for invalid in (
        {**verification, "manifest": "wrong"},
        {**verification, "partitions": []},
        {**verification, "partitions": verification["partitions"] * 2},
        # Scope-only grants (blueprint 27): a verification naming quotas is not current.
        {**verification, "partitions": [{"account": "fund", "quotas": {}}]},
    ):
        with pytest.raises(AssertionError):
            exact_authority_state(manifest, invalid, ("fund",))
    with pytest.raises(AssertionError):
        exact_authority_state(manifest, verification, ("other",))
