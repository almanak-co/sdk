"""The Slipstream generation registry and the cross-generation key resolver.

Every lane resolves a generation from the pool: an address-form pool through
its ``factory()``, a symbolic key by asking every reviewed factory. These tests
pin the registry shape and the resolver's three outcomes (unique, absent,
ambiguous) plus its refusal to trust a partial scan.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base.pool_validation_base import PoolValidationReason
from almanak.connectors.aerodrome import pool_validation
from almanak.connectors.aerodrome.addresses import (
    AERODROME,
    SLIPSTREAM_LP_DEPLOYMENTS,
    SlipstreamDeployment,
    slipstream_deployment_for_factory,
    slipstream_deployment_for_position_manager,
    slipstream_deployment_for_router,
    slipstream_lp_deployments,
    slipstream_position_manager_kind,
)

WETH = "0x4200000000000000000000000000000000000006"
USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
ZERO_WORD = bytes(32)


def _word(address: str) -> bytes:
    return bytes.fromhex(address[2:].rjust(64, "0"))


def _deployments() -> dict[str, SlipstreamDeployment]:
    return {deployment.generation: deployment for deployment in slipstream_lp_deployments("base")}


class TestRegistryShape:
    def test_every_base_generation_is_complete(self) -> None:
        for deployment in slipstream_lp_deployments("base"):
            assert deployment.factory and deployment.position_manager and deployment.generation
            assert deployment.swap_router and deployment.quoter, deployment

    def test_no_singleton_kinds_remain_in_the_flat_table(self) -> None:
        assert not {"cl_factory", "cl_router", "cl_quoter", "cl_nft", "cl_nft_current"} & set(AERODROME["base"])

    def test_flat_table_publishes_one_kind_per_reviewed_manager(self) -> None:
        for chain, deployments in SLIPSTREAM_LP_DEPLOYMENTS.items():
            for deployment in deployments:
                assert AERODROME[chain][slipstream_position_manager_kind(deployment)] == deployment.position_manager

    def test_lookups_are_case_insensitive_and_exact(self) -> None:
        for deployment in slipstream_lp_deployments("base"):
            assert deployment.swap_router is not None
            assert slipstream_deployment_for_factory("base", deployment.factory.upper()) == deployment
            assert slipstream_deployment_for_position_manager("base", deployment.position_manager.lower()) == deployment
            assert slipstream_deployment_for_router("base", deployment.swap_router.lower()) == deployment
        assert slipstream_deployment_for_factory("base", "0x" + "1" * 40) is None
        assert slipstream_deployment_for_router("base", "") is None
        assert slipstream_lp_deployments("optimism") == ()


class TestResolveSlipstreamPoolKey:
    def _resolve(self, answers: dict[str, bytes | None]):
        def fake_eth_call(rpc_url, to, calldata, **kwargs):
            return answers[to.lower()]

        with patch.object(pool_validation, "eth_call", side_effect=fake_eth_call):
            return pool_validation.resolve_slipstream_pool_key("base", WETH, USDC, 50, "http://rpc")

    def test_unique_owner_confirms_and_names_the_factory(self) -> None:
        gens = _deployments()
        pool = "0x" + "ab" * 20
        resolution = self._resolve(
            {gens["current"].factory.lower(): _word(pool), gens["legacy"].factory.lower(): ZERO_WORD}
        )
        assert resolution.unique is not None
        assert resolution.unique.deployment == gens["current"]
        result = resolution.validation_result()
        assert (result.exists, result.reason, result.pool_address, result.factory) == (
            True,
            PoolValidationReason.CONFIRMED,
            pool,
            gens["current"].factory,
        )

    def test_two_owners_are_ambiguous_and_fail_closed(self) -> None:
        gens = _deployments()
        resolution = self._resolve(
            {
                gens["current"].factory.lower(): _word("0x" + "aa" * 20),
                gens["legacy"].factory.lower(): _word("0x" + "bb" * 20),
            }
        )
        assert resolution.unique is None
        result = resolution.validation_result()
        assert (result.exists, result.reason) == (False, PoolValidationReason.AMBIGUOUS)
        error = result.error or ""
        assert "Name the pool address" in error
        assert "current" in error and "legacy" in error

    def test_no_owner_is_not_found(self) -> None:
        gens = _deployments()
        resolution = self._resolve({d.factory.lower(): ZERO_WORD for d in gens.values()})
        result = resolution.validation_result()
        assert (result.exists, result.reason) == (False, PoolValidationReason.NOT_FOUND)

    def test_partial_scan_cannot_prove_uniqueness(self) -> None:
        gens = _deployments()
        resolution = self._resolve(
            {gens["current"].factory.lower(): _word("0x" + "aa" * 20), gens["legacy"].factory.lower(): None}
        )
        assert resolution.matches and resolution.unreachable
        assert resolution.unique is None
        result = resolution.validation_result()
        assert (result.exists, result.reason) == (None, PoolValidationReason.RPC_FAILED)

    def test_no_transport_marks_every_generation_unreachable(self) -> None:
        resolution = pool_validation.resolve_slipstream_pool_key("base", WETH, USDC, 50, None)
        assert set(resolution.unreachable) == set(slipstream_lp_deployments("base"))

    def test_validator_without_deployment_uses_the_resolver(self) -> None:
        gens = _deployments()
        pool = "0x" + "cd" * 20

        def fake_eth_call(rpc_url, to, calldata, **kwargs):
            return _word(pool) if to.lower() == gens["legacy"].factory.lower() else ZERO_WORD

        with patch.object(pool_validation, "eth_call", side_effect=fake_eth_call):
            result = pool_validation.validate_aerodrome_cl_pool("base", WETH, USDC, 100, "http://rpc")
        assert result.exists is True
        assert result.factory == gens["legacy"].factory

    def test_validator_with_deployment_reads_only_that_factory(self) -> None:
        gens = _deployments()
        calls: list[str] = []

        def fake_eth_call(rpc_url, to, calldata, **kwargs):
            calls.append(to.lower())
            return _word("0x" + "ef" * 20)

        with patch.object(pool_validation, "eth_call", side_effect=fake_eth_call):
            result = pool_validation.validate_aerodrome_cl_pool(
                "base", WETH, USDC, 100, "http://rpc", deployment=gens["legacy"]
            )
        assert calls == [gens["legacy"].factory.lower()]
        assert result.factory == gens["legacy"].factory

    def test_validator_rejects_an_unreviewed_deployment(self) -> None:
        rogue = SlipstreamDeployment(factory="0x" + "1" * 40, position_manager="0x" + "2" * 40, generation="rogue")
        with pytest.raises(ValueError, match="unreviewed"):
            pool_validation.validate_aerodrome_cl_pool("base", WETH, USDC, 100, "http://rpc", deployment=rogue)
