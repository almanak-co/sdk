"""Chain-neutral adapter contracts and explicitly supported network identities."""

import re
from dataclasses import dataclass
from typing import Literal, Protocol

from pydantic import model_validator

from almanak.caip import AssetType, ChainId, Identifier
from almanak.models import AssetAmount, Chain, Model, UInt


class ChainReference(Identifier):
    """Opaque namespace-specific reference; the adapter validates its semantics."""

    __slots__ = ()
    pattern = r"[-._:%a-zA-Z0-9]{1,256}"


class CheckpointReference(Model):
    chain: Chain
    reference: ChainReference
    timestamp: UInt
    height: UInt | None = None

    @model_validator(mode="after")
    def family(self):
        validate_reference(self.chain, self.reference)
        if self.chain.chain_id.namespace == "eip155" and self.height is None:
            raise ValueError("EVM checkpoint requires its observed block height")
        return self


class TransactionReference(Model):
    chain: Chain
    reference: ChainReference

    @model_validator(mode="after")
    def family(self):
        validate_reference(self.chain, self.reference)
        return self


def validate_reference(chain, reference):
    if (
        chain.chain_id.namespace == "eip155"
        and re.fullmatch(r"0x[0-9a-f]{64}", str(reference)) is None
    ):
        raise ValueError("EVM references require a canonical 32-byte hash")


class ChainAdapter(Protocol):
    """Privileged dispatch remains behind the deployment's authenticated gateway."""

    def guard(self) -> None: ...

    def observe(self, state=None): ...

    def snapshot(self, name, checkpoint, reference=None): ...

    def verify_snapshot(self, observation): ...

    def checkpoint(self) -> CheckpointReference: ...

    def transaction_reference(self, pending) -> TransactionReference: ...

    def unsigned_plan(self, pending): ...

    def authorize_submission(self, state) -> None: ...

    def advance_family(self, state, action, identity, index): ...

    def submit(self, state): ...

    def prepare(self, state, intent, before, attempt_id): ...

    def sign(self, state): ...

    def verify_signed_admission(self, state, pending) -> bool: ...

    def prepare_submission(self, state) -> None: ...

    def broadcast(self, pending, permissions=None) -> bool: ...

    def require_releasable(self, pending) -> None: ...

    def superseded(self, pending): ...

    def receipt(self, pending): ...

    def reconcile(self, pending, receipt): ...

    def execution_cost(self, pending, receipt) -> AssetAmount: ...

    def audit_history(self, state) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class EvmNetwork:
    chain: ChainId
    native_asset: AssetType
    wrapped_native: AssetType
    genesis: str
    family: Literal["ethereum", "optimism"] = "ethereum"
    wrapping_emits_transfer: bool = False
    legacy_price_is_ceiling: bool = False
    # BSC fee history omits usable tips; the node gas-price suggestion is
    # retained on the quote as its floor instead of a pinned observation.
    fee_history_reports_tips: bool = True
    # The chain seals blocks at one fixed interval (OP Stack; Optimism since Bedrock).
    # Readers take the interval from pinned headers and confirm what they derive from it.
    fixed_block_interval: bool = False
    # Large blocks share the block sequence with small blocks but keep their own base
    # fee (HyperEVM). Transactions within the small-block gas limit land in small blocks,
    # so fee quotes read the small-block market (`production_fees.fee_history_block`).
    separate_large_block_fees: bool = False
    # Provider pools for this chain run some EIP-1898 block-hash reads at their head
    # instead of the pinned block, while block-number reads are served at the block.
    # Each block is final when produced (`finalized` is the head), so a number names one
    # block and remote readers pin by number (`EthereumReader._pin`).
    pins_reads_by_number: bool = False
    # Remote readers prove the execution context of every block they read at, its number,
    # timestamp and parent hash, with one probe call, and refuse a provider that runs the
    # read anywhere else instead of returning another block's state.
    verifies_read_context: bool = False
    # Blocks behind the head within which remote providers run pinned reads elsewhere.
    # Observation readers (`almanak ax`, `almanak markets`) pin `finalized` and `latest`
    # this many blocks behind the tag's block; the context proof still checks that block.
    observation_depth: int = 0
    # Seconds a block must be behind the head before a signed step is released because
    # a transaction in that block took its nonce (`gateway.consumed_nonce`). It is the
    # chain's documented time to finality, stated beside each network with its source,
    # and holds whatever an endpoint serves as `finalized`. None releases nothing.
    finality_margin_seconds: int | None = None


# Finality margins (`EvmNetwork.finality_margin_seconds`), each from the chain's own
# documentation as read on 2026-11-20. A figure is the longest the chain documents for
# its normal case, or a documented lower bound where part of the delay has no documented
# figure; the endpoint's `finalized` tag must hold as well, so the margin only delays.
#
# Ethereum: only an epoch's first block is justified and then finalized, so a block is
# final two to three 32-slot epochs of 12-second slots after it is proposed
# (ethereum.org proof-of-stake and Gasper pages; consensus-specs phase0). Three epochs.
ETHEREUM_FINALITY_SECONDS = 3 * 32 * 12
# Arbitrum One and Robinhood Chain are Nitro rollups on Ethereum: a block is final once
# its batch is posted and that Ethereum block is final (docs.arbitrum.io finality and
# reorgs reference; docs.robinhood.com/chain transaction finality). Neither documents
# a batch-posting time, so the margin is Ethereum's alone, a lower bound.
#
# OP Mainnet: "typically reaches the finalized state within about 15 to 30 minutes of
# submission" (docs.optimism.io transaction finality). Thirty minutes.
OP_MAINNET_FINALITY_SECONDS = 30 * 60
# Base: batch inclusion on Ethereum in about 2 minutes, then finality of that batch in
# about 20 minutes (docs.base.org transaction finality). Their sum.
BASE_FINALITY_SECONDS = 22 * 60
# BNB Smart Chain: fast finality (BEP-126) finalizes within two blocks. When its votes
# are insufficient the chain falls back to probabilistic finality, documented as 120
# blocks for a two-thirds validator majority, about 180 seconds at 1.5-second blocks
# (docs.bnbchain.org JSON-RPC reference). The fallback, in the documented seconds:
# blocks have only become shorter, so 180 seconds spans at least those 120 blocks.
BNB_FINALITY_SECONDS = 180
# Avalanche C-Chain: a block is final when consensus accepts it, in about a second, and
# is never reorganized; `finalized` is the last settled block, a constant 5 seconds
# behind execution (build.avax.network exchange integration and C-Chain RPC pages;
# ACP-194). The settlement delay.
AVALANCHE_FINALITY_SECONDS = 5
# Polygon PoS and HyperEVM state none, so nothing is released there. Polygon documents
# milestone finality in 2 to 5 seconds but no bound when milestones stall and no reorg
# depth without them (docs.polygon.technology finality; PIP-11). Hyperliquid documents
# no finality time and no meaning for the block tags, and `finalized` is the head.

ETHEREUM = EvmNetwork(
    ChainId("eip155:1"),
    AssetType("eip155:1/slip44:60"),
    AssetType("eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
    "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
    finality_margin_seconds=ETHEREUM_FINALITY_SECONDS,
)
AVALANCHE = EvmNetwork(
    ChainId("eip155:43114"),
    AssetType("eip155:43114/slip44:9005"),
    AssetType("eip155:43114/erc20:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"),
    "0x31ced5b9beb7f8782b014660da0cb18cc409f121f408186886e1ca3e8eeca96b",
    finality_margin_seconds=AVALANCHE_FINALITY_SECONDS,
)
BASE = EvmNetwork(
    ChainId("eip155:8453"),
    AssetType("eip155:8453/slip44:60"),
    AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006"),
    "0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd",
    "optimism",
    fixed_block_interval=True,
    finality_margin_seconds=BASE_FINALITY_SECONDS,
)
ARBITRUM = EvmNetwork(
    ChainId("eip155:42161"),
    AssetType("eip155:42161/slip44:60"),
    AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1"),
    "0x7ee576b35482195fc49205cec9af72ce14f003b9ae69f6ba0faef4514be8b442",
    wrapping_emits_transfer=True,
    legacy_price_is_ceiling=True,
    finality_margin_seconds=ETHEREUM_FINALITY_SECONDS,
)
BNB = EvmNetwork(
    ChainId("eip155:56"),
    AssetType("eip155:56/slip44:714"),
    AssetType("eip155:56/erc20:0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"),
    "0x0d21840abff46b96c84b2ac9e10e4f5cdaeb5693cb665db62a2f3b02d2d57b5b",
    fee_history_reports_tips=False,
    finality_margin_seconds=BNB_FINALITY_SECONDS,
)
OPTIMISM = EvmNetwork(
    ChainId("eip155:10"),
    AssetType("eip155:10/slip44:60"),
    AssetType("eip155:10/erc20:0x4200000000000000000000000000000000000006"),
    "0x7ca38a1916c42007829c55e69d3e9a73265554b586a499015373241b8a3fa48b",
    "optimism",
    fixed_block_interval=True,
    finality_margin_seconds=OP_MAINNET_FINALITY_SECONDS,
)


POLYGON = EvmNetwork(
    ChainId("eip155:137"),
    AssetType("eip155:137/slip44:966"),
    AssetType("eip155:137/erc20:0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
    "0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b",
)


ROBINHOOD = EvmNetwork(
    ChainId("eip155:4663"),
    AssetType("eip155:4663/slip44:60"),
    AssetType("eip155:4663/erc20:0x0bd7d308f8e1639fab988df18a8011f41eacad73"),
    "0xaad15f3d702aaea00caf3e9bb56395efe9127bc3b31b24921abf1eee3409305c",
    wrapping_emits_transfer=True,
    legacy_price_is_ceiling=True,
    finality_margin_seconds=ETHEREUM_FINALITY_SECONDS,
)


# HyperEVM pays HYPE (SLIP-44 2457) for gas. WHYPE is a WETH9 clone, and legacy
# receipts settle at exactly the bid price. A 30M-gas large block follows every 60
# one-second 3M-gas small blocks, at the large-block base fee. Measured 2026-10-20/21:
# Alchemy runs reads pinned within about 128 blocks (hash) or 192 blocks (number) of its
# head at head + 1, and several dRPC upstreams ignore hash pins at any depth while every
# one served number pins exactly. Readers therefore pin by number and verify the context.
# `finalized` is the head, so an observation at a tag pins 256 blocks (about four minutes)
# behind it; on 2026-11-04 Alchemy ran pins within 128 blocks at head + 1 and served 132.
HYPEREVM = EvmNetwork(
    ChainId("eip155:999"),
    AssetType("eip155:999/slip44:2457"),
    AssetType("eip155:999/erc20:0x5555555555555555555555555555555555555555"),
    "0xd8fcc13b6a195b88b7b2da3722ff6cad767b13a8c1e9ffb1c73aa9d216d895f0",
    separate_large_block_fees=True,
    pins_reads_by_number=True,
    verifies_read_context=True,
    observation_depth=256,
)


def evm_network(chain: ChainId) -> EvmNetwork:
    for network in (
        ETHEREUM,
        AVALANCHE,
        BASE,
        ARBITRUM,
        BNB,
        OPTIMISM,
        POLYGON,
        ROBINHOOD,
        HYPEREVM,
    ):
        if network.chain == chain:
            return network
    raise ValueError(f"no verified EVM network definition for {chain}")


def configured_chains(config) -> tuple[Chain, ...]:
    return tuple(dict.fromkeys(b.profile.fund.chain for b in config.accounts.bindings))
