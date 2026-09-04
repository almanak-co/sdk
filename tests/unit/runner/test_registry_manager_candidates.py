"""Registry NFT-manager authority when a venue has several reviewed managers.

Aerodrome Slipstream on Base has two reviewed factory generations, each
minting under its own NonfungiblePositionManager. The registry can therefore
name a single manager only for single-generation venues; for Slipstream the
manager must come from the receipt, and an LP_OPEN row whose receipt names
none is refused rather than hashed under a default generation.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments
from almanak.framework.migration.backfill import (
    _nft_manager_candidates_for_protocol_chain,
    _nft_manager_for_protocol_chain,
    physical_identity_hash_univ3,
)
from almanak.framework.runner.strategy_runner import StrategyRunner

_build_lp_open_registry_row = StrategyRunner._build_lp_open_registry_row


def _manager_for_generation(name: str) -> str:
    return next(
        deployment.position_manager for deployment in slipstream_lp_deployments("base") if deployment.generation == name
    )


class TestNftManagerCandidates:
    def test_slipstream_base_lists_every_reviewed_manager(self):
        candidates = _nft_manager_candidates_for_protocol_chain("aerodrome_slipstream", "base")
        expected = tuple(deployment.position_manager.lower() for deployment in slipstream_lp_deployments("base"))
        assert len(expected) == 2
        assert candidates == expected

    def test_slipstream_base_has_no_single_manager(self):
        assert _nft_manager_for_protocol_chain("aerodrome_slipstream", "base") is None

    def test_single_generation_venue_names_its_manager(self):
        from almanak.connectors.uniswap_v3.receipt_parser import POSITION_MANAGER_ADDRESSES

        candidates = _nft_manager_candidates_for_protocol_chain("uniswap_v3", "arbitrum")
        assert len(candidates) == 1
        assert candidates[0].lower() == POSITION_MANAGER_ADDRESSES["arbitrum"].lower()
        assert _nft_manager_for_protocol_chain("uniswap_v3", "arbitrum") == candidates[0]

    def test_unknown_chain_has_no_candidates(self):
        assert _nft_manager_candidates_for_protocol_chain("aerodrome_slipstream", "ethereum") == ()
        assert _nft_manager_for_protocol_chain("aerodrome_slipstream", "ethereum") is None


def _make_runner(*, chain: str = "base") -> SimpleNamespace:
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain=chain)
    runner._extract_block_number_from_result = MagicMock(return_value=100)

    def _stub_build_registry_row(**kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)

    runner._build_registry_row = _stub_build_registry_row
    return runner


def _make_parser(*, open_payload: Any) -> SimpleNamespace:
    parser = SimpleNamespace()
    parser.extract_registry_payload_open = MagicMock(return_value=open_payload)
    return parser


class TestBuildLpOpenRegistryRowManagerAuthority:
    POOL = "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
    TOKEN_ID = 4242

    def _open_payload(self, **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "token_id": str(self.TOKEN_ID),
            "pool_address": self.POOL,
            "tick_lower": -100,
            "tick_upper": 100,
        }
        payload.update(overrides)
        return payload

    def _build(self, *, nft_manager: str, payload: dict[str, Any] | None):
        return _build_lp_open_registry_row(
            _make_runner(),
            strategy=SimpleNamespace(deployment_id="dep:1", chain="base"),
            intent=SimpleNamespace(registry_handle=None),
            result=MagicMock(),
            entry=SimpleNamespace(tx_hash="0x" + "ab" * 32),
            chain="base",
            nft_manager=nft_manager,
            receipt={},
            parser=_make_parser(open_payload=payload),
            fee_tier=100,
        )

    def test_refuses_row_when_neither_registry_nor_receipt_names_manager(self):
        assert self._build(nft_manager="", payload=self._open_payload()) is None

    def test_hashes_with_receipt_manager_when_registry_is_silent(self):
        legacy_manager = _manager_for_generation("legacy")
        out = self._build(nft_manager="", payload=self._open_payload(nft_manager_addr=legacy_manager.upper()))
        assert out is not None
        row, payload, token_id = out
        assert token_id == self.TOKEN_ID
        assert row.physical_identity_hash == physical_identity_hash_univ3(
            chain="base", nft_manager_addr=legacy_manager.lower(), token_id=self.TOKEN_ID
        )
        assert row.physical_identity_hash != physical_identity_hash_univ3(
            chain="base",
            nft_manager_addr=_manager_for_generation("current"),
            token_id=self.TOKEN_ID,
        )

    def test_receipt_manager_overrides_registry_manager(self):
        """The receipt is the on-chain emitter: it wins over the registry's
        single-candidate manager when both are present."""
        out = self._build(
            nft_manager="0x" + "11" * 20,
            payload=self._open_payload(nft_manager_addr="0x" + "22" * 20),
        )
        assert out is not None
        row, _payload, _token_id = out
        assert row.physical_identity_hash == physical_identity_hash_univ3(
            chain="base", nft_manager_addr="0x" + "22" * 20, token_id=self.TOKEN_ID
        )

    def test_registry_manager_used_when_receipt_is_silent(self):
        out = self._build(nft_manager="0x" + "11" * 20, payload=self._open_payload())
        assert out is not None
        row, _payload, _token_id = out
        assert row.physical_identity_hash == physical_identity_hash_univ3(
            chain="base", nft_manager_addr="0x" + "11" * 20, token_id=self.TOKEN_ID
        )
        assert row.status == "open"
        assert row.opened_at_block == 100

    @pytest.mark.parametrize("payload", [None, {"pool_address": "0x" + "ab" * 20}])
    def test_unusable_payload_is_refused_before_manager_check(self, payload):
        assert self._build(nft_manager="0x" + "11" * 20, payload=payload) is None


def test_velodrome_slipstream_offers_no_aerodrome_manager_on_any_chain() -> None:
    """The Aerodrome Base managers are Aerodrome's; a slug with no reviewed generation has no candidates."""
    from almanak.framework.migration.backfill import (
        _nft_manager_candidates_for_protocol_chain,
        _nft_manager_for_protocol_chain,
    )

    for chain in ("base", "optimism"):
        assert _nft_manager_candidates_for_protocol_chain("velodrome_slipstream", chain) == ()
        assert _nft_manager_for_protocol_chain("velodrome_slipstream", chain) is None
    assert len(_nft_manager_candidates_for_protocol_chain("aerodrome_slipstream", "base")) == 2
    assert _nft_manager_candidates_for_protocol_chain("aerodrome_slipstream", "optimism") == ()
