"""Seal-time contract for the Uniswap V4 SWAP mainnet cell.

V4 identifies a pool by the keccak of its PoolKey inside one PoolManager, not
by a deployed address, and the connector derives tick spacing from a fee map
welded over from V3. The guards below are what turn that inference into a
proven binding, so each one is pinned in both directions: it accepts the
observation a healthy run produces, and refuses every single-field drift.
"""

from __future__ import annotations

from typing import Any

import pytest
from eth_utils import keccak

from almanak.connectors.uniswap_v4.receipt_parser import SWAP_EVENT_TOPIC as V4_SWAP_EVENT_TOPIC
from qa_lab.mainnet_intent_envelope import (
    MainnetEnvelopeError,
    _required_guard_ids,
    _validate_uniswap_v4_guard,
    _validate_uniswap_v4_phase_action,
)
from qa_lab.mainnet_intent_recipe import UNISWAP_V4_ROBINHOOD_SWAP_EOA as RECIPE
from qa_lab.mainnet_intent_recipe import RECIPES

CHAIN_ID = 4663
WALLET = "0x" + "11" * 20
TRANSFER_TOPIC = "0x" + keccak(text="Transfer(address,address,uint256)").hex()
# Imported rather than re-derived: a hand-typed signature that drifts from the
# connector's would make every case below pass against the wrong topic.
V4_SWAP_TOPIC = V4_SWAP_EVENT_TOPIC


def _identity(**overrides: Any) -> dict[str, Any]:
    observation = {
        "chain_id": CHAIN_ID,
        "block_number": 1,
        "block_hash": "0x" + "ab" * 32,
        "pool_manager": RECIPE.resource_address,
        "state_view": RECIPE.state_view_address,
        "pool_id": RECIPE.pool_id,
        "derived_pool_id": RECIPE.pool_id,
        "currency0": RECIPE.currency0,
        "currency1": RECIPE.currency1,
        "fee_tier": RECIPE.fee_tier,
        "tick_spacing": RECIPE.tick_spacing,
        "hooks": RECIPE.hooks,
        "sqrt_price_x96": 79228162514264337593543950336,
        "tick": 0,
        "pool_manager_code_sha256": "cd" * 32,
        "state_view_code_sha256": "ef" * 32,
    }
    observation.update(overrides)
    return observation


def _min_out(**overrides: Any) -> dict[str, Any]:
    metadata = {
        "pool_id": RECIPE.pool_id,
        "pool_manager": RECIPE.resource_address,
        "amount_out_minimum": "123456",
        "quote_source": "onchain_quoter",
    }
    metadata.update(overrides.pop("compile_metadata", {}))
    observation = {
        "managed_fork": False,
        "max_slippage": RECIPE.max_slippage,
        "compile_metadata": metadata,
    }
    observation.update(overrides)
    return observation


def _word(value: str) -> str:
    return "0x" + value.lower().removeprefix("0x").rjust(64, "0")


def _cleanup_receipt(**overrides: Any) -> dict[str, Any]:
    amount_out = 500000
    logs = [
        {
            "address": RECIPE.resource_address,
            "topics": [V4_SWAP_TOPIC, RECIPE.pool_id, _word(WALLET)],
            "data": "0x" + "00" * 32,
        },
        {
            "address": RECIPE.asset_address,
            "topics": [TRANSFER_TOPIC, _word(RECIPE.resource_address), _word(WALLET)],
            "data": "0x" + f"{amount_out:064x}",
        },
    ]
    receipt: dict[str, Any] = {
        "transactionHash": "0x" + "cd" * 32,
        "blockNumber": 2,
        "logs": overrides.pop("logs", logs),
    }
    receipt.update(overrides)
    return {"raw_receipt": receipt}


class _StubSwap:
    amount_in = 1_000_000_000_000_000
    amount_out = 500000


class _StubParsed:
    swap_result = _StubSwap()


@pytest.fixture(autouse=True)
def _stub_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the log-shape contract without re-encoding a full V4 Swap payload.

    The parser has its own dedicated tests; what is under test here is that the
    envelope binds the receipt to the approved pool id and wallet.
    """
    import qa_lab.mainnet_intent_envelope as envelope

    class _Parser:
        def __init__(self, *, chain: str) -> None:
            self.chain = chain

        def parse_receipt(self, receipt: dict[str, Any]) -> _StubParsed:
            return _StubParsed()

    monkeypatch.setattr(envelope, "UniswapV4ReceiptParser", _Parser)


def test_the_v4_cell_demands_both_production_guards() -> None:
    assert _required_guard_ids(RECIPE) == {
        "uniswap_v4_exact_pool_identity",
        "uniswap_v4_production_quote_and_min_out",
    }


def test_recipe_names_the_singleton_and_a_derivable_pool_id() -> None:
    from almanak.connectors._strategy_base.v4_pool_abi import compute_v4_pool_id
    from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4

    assert RECIPE.cell_id == "intent.uniswap_v4.robinhood.SWAP.mainnet.eoa"
    assert RECIPE.exec_path == "eoa"
    assert RECIPE.semantic_profile == "v4_swap.v1"
    assert RECIPE.resource_address == UNISWAP_V4["robinhood"]["pool_manager"]
    assert RECIPE.state_view_address == UNISWAP_V4["robinhood"]["state_view"]
    assert RECIPE.pool_id == compute_v4_pool_id(
        RECIPE.currency0, RECIPE.currency1, RECIPE.fee_tier, RECIPE.tick_spacing, RECIPE.hooks
    )
    assert {RECIPE.currency0.lower(), RECIPE.currency1.lower()} == {
        RECIPE.asset_address.lower(),
        RECIPE.output_asset_address.lower(),
    }
    assert RECIPE.target == (f"V4_SWAP:USDG:WETH:{RECIPE.target_amount}:{RECIPE.fee_tier}:{RECIPE.pool_id}",)
    assert RECIPE.cleanup == (
        f"V4_SWAP_BACK:WETH:USDG:MEASURED:{RECIPE.fee_tier}:{RECIPE.pool_id}",
        "SWEEP_TO_MASTER",
    )
    assert RECIPE.terminal == ("TOKEN_BALANCE_ZERO:WETH", "NO_RESIDUAL_ALLOWANCES", "POOL_WALLET_RELEASED")
    assert RECIPES[RECIPE.cell_id] is RECIPE


def test_pool_identity_guard_accepts_a_healthy_observation() -> None:
    _validate_uniswap_v4_guard(
        guard_id="uniswap_v4_exact_pool_identity",
        observation=_identity(),
        recipe=RECIPE,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"chain_id": 1},
        {"pool_id": "0x" + "00" * 32},
        # The whole point of the guard: a key built from a different tick
        # spacing hashes to another pool, and only the re-derivation catches it.
        {"derived_pool_id": "0x" + "22" * 32},
        {"tick_spacing": 10},
        {"fee_tier": 500},
        {"hooks": "0x" + "33" * 20},
        {"pool_manager": "0x" + "44" * 20},
        {"state_view": "0x" + "55" * 20},
        {"currency1": "0x" + "66" * 20},
        {"pool_manager_code_sha256": "00" * 32},
        {"state_view_code_sha256": "00" * 32},
        # An uninitialised pool reads zero here.
        {"sqrt_price_x96": 0},
    ],
)
def test_pool_identity_guard_refuses_each_drift(override: dict[str, Any]) -> None:
    with pytest.raises(MainnetEnvelopeError):
        _validate_uniswap_v4_guard(
            guard_id="uniswap_v4_exact_pool_identity",
            observation=_identity(**override),
            recipe=RECIPE,
            chain_id=CHAIN_ID,
        )


def test_min_out_guard_accepts_an_executable_quote() -> None:
    _validate_uniswap_v4_guard(
        guard_id="uniswap_v4_production_quote_and_min_out",
        observation=_min_out(),
        recipe=RECIPE,
        chain_id=CHAIN_ID,
    )


@pytest.mark.parametrize(
    "override",
    [
        {"managed_fork": True},
        {"max_slippage": "0.5"},
        {"compile_metadata": {"pool_id": "0x" + "77" * 32}},
        {"compile_metadata": {"pool_manager": "0x" + "88" * 20}},
        {"compile_metadata": {"amount_out_minimum": "0"}},
        # A minimum derived from an offline estimate is not protection.
        {"compile_metadata": {"quote_source": "offline_estimate"}},
    ],
)
def test_min_out_guard_refuses_unbound_protection(override: dict[str, Any]) -> None:
    with pytest.raises(MainnetEnvelopeError):
        _validate_uniswap_v4_guard(
            guard_id="uniswap_v4_production_quote_and_min_out",
            observation=_min_out(**override),
            recipe=RECIPE,
            chain_id=CHAIN_ID,
        )


def test_unknown_v4_guard_id_is_refused_rather_than_ignored() -> None:
    with pytest.raises(MainnetEnvelopeError, match="No validator for mandatory production guard"):
        _validate_uniswap_v4_guard(
            guard_id="uniswap_v4_something_new",
            observation={},
            recipe=RECIPE,
            chain_id=CHAIN_ID,
        )


def test_cleanup_receipt_binds_to_the_approved_pool_and_wallet() -> None:
    _validate_uniswap_v4_phase_action(
        payload=_cleanup_receipt(),
        action_spec=RECIPE.cleanup[0],
        recipe=RECIPE,
        wallet=WALLET,
    )


def test_cleanup_obligation_must_be_the_reviewed_inverse() -> None:
    for spec in (
        "SWAP_BACK:WETH:USDG:MEASURED:3000:" + RECIPE.pool_id,
        "V4_SWAP_BACK:USDG:WETH:MEASURED:3000:" + RECIPE.pool_id,
        "V4_SWAP_BACK:WETH:USDG:MEASURED:500:" + RECIPE.pool_id,
        "V4_SWAP_BACK:WETH:USDG:MEASURED:3000:0x" + "99" * 32,
    ):
        with pytest.raises(MainnetEnvelopeError):
            _validate_uniswap_v4_phase_action(
                payload=_cleanup_receipt(), action_spec=spec, recipe=RECIPE, wallet=WALLET
            )


def test_cleanup_settling_in_another_pool_is_refused() -> None:
    """The PoolManager emits for every pool, so the emitter alone proves nothing."""
    logs = [
        {
            "address": RECIPE.resource_address,
            "topics": [V4_SWAP_TOPIC, "0x" + "aa" * 32, _word(WALLET)],
            "data": "0x" + "00" * 32,
        },
        {
            "address": RECIPE.asset_address,
            "topics": [TRANSFER_TOPIC, _word(RECIPE.resource_address), _word(WALLET)],
            "data": "0x" + f"{500000:064x}",
        },
    ]
    with pytest.raises(MainnetEnvelopeError, match="one Swap event in the approved pool"):
        _validate_uniswap_v4_phase_action(
            payload=_cleanup_receipt(logs=logs),
            action_spec=RECIPE.cleanup[0],
            recipe=RECIPE,
            wallet=WALLET,
        )


def test_a_dust_credit_does_not_satisfy_the_measured_output() -> None:
    """Existence is not the contract; the amount is.

    A receipt crediting the wallet one wei of the funding asset would satisfy a
    non-empty check while the Swap reports a far larger output -- the difference
    between a cleanup that returned the trade and one that returned nothing.
    """
    logs = [
        {
            "address": RECIPE.resource_address,
            "topics": [V4_SWAP_TOPIC, RECIPE.pool_id, _word(WALLET)],
            "data": "0x" + "00" * 32,
        },
        {
            "address": RECIPE.asset_address,
            "topics": [TRANSFER_TOPIC, _word(RECIPE.resource_address), _word(WALLET)],
            "data": "0x" + f"{1:064x}",
        },
    ]
    with pytest.raises(MainnetEnvelopeError, match="one wallet-bound transfer"):
        _validate_uniswap_v4_phase_action(
            payload=_cleanup_receipt(logs=logs),
            action_spec=RECIPE.cleanup[0],
            recipe=RECIPE,
            wallet=WALLET,
        )


def test_cleanup_that_pays_another_account_is_refused() -> None:
    stranger = "0x" + "77" * 20
    logs = [
        {
            "address": RECIPE.resource_address,
            "topics": [V4_SWAP_TOPIC, RECIPE.pool_id, _word(WALLET)],
            "data": "0x" + "00" * 32,
        },
        {
            "address": RECIPE.asset_address,
            "topics": [TRANSFER_TOPIC, _word(RECIPE.resource_address), _word(stranger)],
            "data": "0x" + f"{500000:064x}",
        },
    ]
    with pytest.raises(MainnetEnvelopeError, match="one wallet-bound transfer"):
        _validate_uniswap_v4_phase_action(
            payload=_cleanup_receipt(logs=logs),
            action_spec=RECIPE.cleanup[0],
            recipe=RECIPE,
            wallet=WALLET,
        )
