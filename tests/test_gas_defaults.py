"""Author gas policy defaults: only the payers, the chain and the amounts are required."""

import json
from argparse import Namespace
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanak.check_warnings import BPS, gas_references, measured_gas, offline_warnings
from almanak.checks import gas_defaults
from almanak.configuration.gas import (
    DEFAULT_GAS_LIMIT_BUFFER_BPS,
    DEFAULT_MAX_GAS_LIMIT,
    DEFAULTED_FIELDS,
    GasInput,
)
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError

# Chain: (USDC, a Uniswap V3 USDC pool).
CHAINS = {
    "eip155:8453": (
        "eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "0xd0b53d9277642d899df5c87a3966a349a798f224",
    ),
    "eip155:42161": (
        "eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "0xc6962004f452be9203591991d15f6b388e09e8d0",
    ),
}
FULL = {
    "source": "network_rpc_fee_history",
    "base_fee_basis": "next_block",
    "reward_percentile": 50,
    "base_fee_multiplier_bps": 20000,
    "max_gas_limit": 1000000,
    "max_gas_price_wei": 150000000,
    "gas_limit_buffer_bps": 2500,
}

CONFIG = """
strategy:
  decision_interval: 1h
  allocations:
    position:
      market: pool
      account: fund
      capital: {{asset: {usdc}, quantity: {{base_units: 200000000}}}}
      entry_swap_bps: 10000
      hold_seconds: 1d
      slippage_bps: 100
      deadline_seconds: 300s
      minimum_native_balance: {{base_units: 0}}
      range: {{lower: -200000, upper: -190000}}
  gas:
    base:
      accounts: [fund]
      chain: {chain}
{extra}      max_transaction_fee: '{fee}'
      exit_reserve: '0.0008'
      total_budget: '0.00145'
markets:
  pool:
    connector: uniswap_v3
    chain: {chain}
    pool: '{pool}'
accounts:
  fund:
    chain: {chain}
"""


def document(tmp_path: Path, *, chain="eip155:8453", fee="0.00016", extra=""):
    path = tmp_path / "config.yaml"
    path.write_text(
        CONFIG.format(
            usdc=CHAINS[chain][0], pool=CHAINS[chain][1], chain=chain, fee=fee, extra=extra
        )
    )
    return read_inputs(path)


def test_a_policy_with_only_payers_chain_and_amounts_gets_consistent_ceilings(tmp_path):
    config = document(tmp_path)
    policy = config.inputs.strategy.gas["base"]
    assert (policy.source, policy.reward_percentile, policy.gas_limit_buffer_bps) == (
        "network_rpc_fee_history",
        50,
        2500,
    )
    assert policy.base_fee_multiplier_bps == 20000
    assert policy.max_gas_limit == DEFAULT_MAX_GAS_LIMIT
    # 0.00016 ETH over 1.2M gas: the highest price the fee ceiling admits.
    assert policy.max_gas_price_wei == 160_000_000_000_000 // 1_200_000
    resolved = policy.resolve("base")
    assert resolved.max_gas_limit * resolved.max_gas_price_wei <= (
        resolved.max_transaction_fee.amount
    )
    # Defaulted ceilings never contradict the amounts: no gas finding at all.
    codes = {w["code"] for w in offline_warnings(config.inputs)}
    assert not codes & {"gas_limit", "gas_fee_ceiling", "gas_budget", "gas_exit_reserve"}


def test_arbitrum_defaults_to_a_lower_base_fee_multiplier(tmp_path):
    policy = document(tmp_path, chain="eip155:42161").inputs.strategy.gas["base"]
    assert policy.base_fee_multiplier_bps == 12500


def test_stated_fields_are_kept_and_the_price_follows_a_stated_limit(tmp_path):
    extra = (
        "      max_gas_limit: 1000000\n"
        "      base_fee_multiplier_bps: 15000\n"
        "      reward_percentile: 25\n"
    )
    config = document(tmp_path, extra=extra)
    policy = config.inputs.strategy.gas["base"]
    assert (policy.max_gas_limit, policy.base_fee_multiplier_bps) == (1_000_000, 15000)
    assert policy.reward_percentile == 25
    assert policy.max_gas_price_wei == 160_000_000
    reported = {entry["field"]: entry["value"] for entry in gas_defaults(config)["gas_defaults"]}
    assert reported == {
        "strategy.gas.base.source": "network_rpc_fee_history",
        "strategy.gas.base.base_fee_basis": "next_block",
        "strategy.gas.base.max_gas_price_wei": 160_000_000,
        "strategy.gas.base.gas_limit_buffer_bps": 2500,
    }


def test_a_fully_stated_policy_reports_no_defaults(tmp_path):
    assert set(DEFAULTED_FIELDS) == set(FULL)
    extra = "".join(f"      {field}: {value}\n" for field, value in FULL.items())
    config = document(tmp_path, extra=extra)
    assert config.inputs.strategy.gas["base"].max_gas_price_wei == 150_000_000
    assert gas_defaults(config) == {}


def test_a_fee_too_small_to_derive_a_price_names_the_fee(tmp_path):
    with pytest.raises(ConfigurationError) as refused:
        document(tmp_path, fee="0.000000000001")
    assert "raise max_transaction_fee" in str(refused.value)


def test_the_default_gas_limit_covers_every_measured_action_but_gmx_orders():
    """A reference that outgrows the default must change it deliberately, not silently
    turn every defaulted policy into a gas_limit finding. GMX orders and native-coin pool
    mints (a variant that bounds only liquidity on a native pool) state their limit instead."""
    for reference in gas_references():
        if (
            reference["profile"] != "safe_roles"
            or reference["connector"] == "gmx_v2"
            or (reference.get("variant") == "native" and reference["action"] == "mint")
        ):
            continue
        required = -(-measured_gas(reference) * (BPS + DEFAULT_GAS_LIMIT_BUFFER_BPS) // BPS)
        assert required <= DEFAULT_MAX_GAS_LIMIT, reference


def policy(**fields):
    """A gas policy validated alone, as the loader validates it: in JSON mode."""
    body = {"accounts": ["fund"], "max_transaction_fee": "0.00016", "exit_reserve": "0"}
    return GasInput.model_validate_json(json.dumps(body | fields))


@pytest.mark.parametrize(
    ("chain", "basis", "multiplier"),
    [
        ("eip155:1", "next_block", 20000),
        ("eip155:43114", "observed_block", 20000),
        ("eip155:42161", "next_block", 12500),
        ("eip155:4663", "next_block", 12500),
    ],
)
def test_bidding_defaults_follow_the_chain(chain, basis, multiplier):
    gas = policy(chain=chain)
    assert (gas.base_fee_basis, gas.base_fee_multiplier_bps) == (basis, multiplier)
    assert gas.max_gas_price_wei == 160_000_000_000_000 // DEFAULT_MAX_GAS_LIMIT


@pytest.mark.parametrize(
    ("fields", "reason"),
    [
        ({"chain": "eip155:999999"}, "gas chain eip155:999999"),
        (
            {"chain": "eip155:1", "max_transaction_fee": "0." + "0" * 18 + "1"},
            "max_transaction_fee",
        ),
        ({"chain": "eip155:1", "max_gas_price_wei": 0}, "max_gas_price_wei must be positive"),
    ],
)
def test_a_price_that_cannot_be_derived_is_a_located_validation_error(fields, reason):
    with pytest.raises(ValidationError) as refused:
        policy(**fields)
    assert reason in str(refused.value)


def test_an_unknown_gas_chain_is_a_configuration_error_not_a_crash(tmp_path):
    path = tmp_path / "config.yaml"
    text = CONFIG.format(
        usdc=CHAINS["eip155:8453"][0],
        pool=CHAINS["eip155:8453"][1],
        chain="eip155:8453",
        fee="0.00016",
        extra="",
    )
    path.write_text(text.replace("      chain: eip155:8453\n", "      chain: eip155:999999\n", 1))
    with pytest.raises(ConfigurationError) as refused:
        read_inputs(path)
    assert refused.value.field.startswith("strategy.gas.base")


def test_a_price_above_256_bits_is_refused_when_parsed():
    with pytest.raises(ValidationError):
        policy(chain="eip155:1", max_gas_price_wei=2**256)


def test_check_reports_the_defaults_on_success_and_on_a_failed_stage(tmp_path, monkeypatch):
    from almanak import checks

    config = document(tmp_path)
    monkeypatch.setattr(checks, "author_inputs", lambda args, mode=None: (config, None))
    monkeypatch.setattr(checks, "staged_check", lambda *args: {"valid": True})
    args = Namespace(config=str(tmp_path / "config.yaml"))
    fields = {entry["field"] for entry in checks.check(args)["gas_defaults"]}
    assert fields == {f"strategy.gas.base.{field}" for field in DEFAULTED_FIELDS}

    def refuse(*args):
        raise checks.CheckFailure(ValueError("refused"), [{"stage": "resolution"}], "x")

    monkeypatch.setattr(checks, "staged_check", refuse)
    with pytest.raises(checks.CheckFailure) as failed:
        checks.check(args)
    assert len(failed.value.detail["gas_defaults"]) == len(DEFAULTED_FIELDS)
