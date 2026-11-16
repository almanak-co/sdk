"""Verified Aave price dependencies, including caller-dependent SVR visibility."""

import json
from pathlib import Path

from eth_abi import encode
from eth_utils import keccak

from almanak.backtest.economic_data import OraclePrice
from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.models import Address, Model, UInt, address

ARTIFACT = Path(__file__).with_name("artifacts") / "ethereum-oracles.json"


def artifact_for(chain: ChainId):
    files = {
        ChainId("eip155:1"): ARTIFACT,
        ChainId("eip155:43114"): ARTIFACT.with_name("avalanche-oracles.json"),
    }
    if not isinstance(chain, ChainId) or chain not in files:
        raise ValueError("oracle reconstruction requires a verified CAIP-2 chain binding")
    return files[chain]


def contracts_for(chain: ChainId):
    artifact = json.loads(artifact_for(chain).read_text())
    if ChainId(artifact["chain_id"]) != chain:
        raise ValueError("oracle artifact identifies another chain")
    return tuple(artifact["contracts"])


class Transmission(Model):
    round_id: UInt
    answer: int
    observed_at: UInt
    recorded_at: UInt


class DualState(Model):
    latest: UInt
    secondary: UInt
    is_latest_secondary: bool
    cutoff: UInt
    max_sync_iterations: UInt
    secondary_proxy: Address
    rounds: tuple[Transmission, ...]

    def round(self, identity):
        for row in self.rounds:
            if row.round_id == identity:
                return row
        raise ValueError("required oracle transmission is missing")

    def selected(self, caller, time):
        if caller == self.secondary_proxy:
            if self.round(self.secondary).recorded_at + self.cutoff >= time:
                return self.round(self.secondary)
            for identity in range(self.latest, max(0, self.latest - self.max_sync_iterations), -1):
                row = self.round(identity)
                if row.recorded_at + self.cutoff < time:
                    return row
            return self.round(self.secondary)
        current = self.round(self.latest)
        if (
            self.latest == self.secondary
            and self.is_latest_secondary
            and current.recorded_at == time
        ):
            return self.round(self.latest - 1)
        return current

    def deadlines(self, now):
        return tuple(
            sorted(
                {
                    edge
                    for row in self.rounds
                    for edge in (row.recorded_at + 1, row.recorded_at + self.cutoff + 1)
                    if edge > now
                }
            )
        )


class OracleReader:
    def __init__(self, reader):
        self.reader = reader
        self.contracts = {row["address"]: row for row in contracts_for(reader.chain.chain_id)}
        self.dependencies: set[str] = set()
        self.deadlines: set[int] = set()

    def verified(self, target, block):
        row = self.contracts.get(target)
        if row is None:
            raise DataError(
                "unsupported_oracle", "oracle dependency has no verified reconstruction"
            )
        raw = self.reader._rpc(
            "eth_getCode", [target, {"blockHash": block.hash, "requireCanonical": True}]
        )
        actual = "0x" + keccak(bytes.fromhex(raw[2:])).hex()
        if row["code_hash"] != actual:
            raise DataError(
                "unsupported_oracle", "oracle dependency code differs from its verified revision"
            )
        self.dependencies.add(target)
        return row

    def storage(self, target, slot, block):
        raw = self.reader._rpc(
            "eth_getStorageAt",
            [target, hex(slot), {"blockHash": block.hash, "requireCanonical": True}],
        )
        return int(raw, 16)

    def transmission(self, target, identity, block):
        # Verified DualAggregator 1.0.0 layout: transmissions are packed into slot 17.
        slot = int.from_bytes(keccak(encode(("uint32", "uint256"), (identity, 17))))
        raw = self.storage(target, slot, block)
        answer = raw & ((1 << 192) - 1)
        if answer >= 1 << 191:
            answer -= 1 << 192
        return Transmission(
            round_id=identity,
            answer=answer,
            observed_at=(raw >> 192) & ((1 << 32) - 1),
            recorded_at=raw >> 224,
        )

    def dual(self, target, artifact, block):
        hot = self.storage(target, 13, block)
        latest, secondary = (hot >> 48) & ((1 << 32) - 1), (hot >> 80) & ((1 << 32) - 1)
        identifiers = {secondary, max(0, latest - 1)}
        identifiers.update(range(latest, max(0, latest - artifact["max_sync_iterations"]), -1))
        return DualState(
            latest=latest,
            secondary=secondary,
            is_latest_secondary=bool((hot >> 240) & 255),
            cutoff=self.storage(target, 18, block) & ((1 << 32) - 1),
            max_sync_iterations=artifact["max_sync_iterations"],
            secondary_proxy=artifact["secondary_proxy"],
            rounds=tuple(
                self.transmission(target, identity, block) for identity in sorted(identifiers)
            ),
        )

    def answer(self, target, caller, block, visited=()):
        if target in visited or len(visited) >= 8:
            raise ValueError("oracle dependency cycle or unsupported depth")
        artifact = self.verified(target, block)
        chain = (*visited, target)
        if artifact["kind"] == "proxy":
            (aggregator,) = self.reader.call(target, "aggregator()", block, outputs=("address",))
            return self.answer(address(aggregator), target, block, chain)
        if artifact["kind"] == "cap":
            (source,) = self.reader.call(
                target, "ASSET_TO_USD_AGGREGATOR()", block, outputs=("address",)
            )
            (cap,) = self.reader.call(target, "getPriceCap()", block, outputs=("int256",))
            return min(cap, self.answer(address(source), target, block, chain))
        if artifact["kind"] == "dual":
            state = self.dual(target, artifact, block)
            self.deadlines.update(state.deadlines(block.timestamp))
            return state.selected(caller, block.timestamp).answer
        if artifact["kind"] == "ocr2":
            # Verified OCR2 exposes the last stored transmission without SVR's
            # caller-dependent visibility or timestamp-dependent release rules.
            return self.reader.call(target, "latestAnswer()", block, outputs=("int256",))[0]
        raise DataError("unsupported_oracle", "price source has an unsupported adapter role")

    def price(self, reserve, block):
        self.dependencies.clear()
        self.deadlines.clear()
        (oracle,) = self.reader.call(reserve.spoke, "ORACLE()", block, outputs=("address",))
        oracle = address(oracle)
        if self.verified(oracle, block)["kind"] != "oracle":
            raise DataError("unsupported_oracle", "Spoke oracle has an unsupported implementation")
        (decimals,) = self.reader.call(oracle, "decimals()", block, outputs=("uint8",))
        (spoke,) = self.reader.call(oracle, "spoke()", block, outputs=("address",))
        if decimals != 8 or address(spoke) != reserve.spoke:
            raise ValueError("oracle units or Spoke binding differ from the declared source")
        (feed,) = self.reader.call(
            oracle,
            "getReserveSource(uint256)",
            block,
            inputs=("uint256",),
            args=(reserve.reserve_id,),
            outputs=("address",),
        )
        feed = address(feed)
        reconstructed = self.answer(feed, oracle, block)
        (actual,) = self.reader.call(
            oracle,
            "getReservePrice(uint256)",
            block,
            inputs=("uint256",),
            args=(reserve.reserve_id,),
        )
        if actual != reconstructed or reconstructed <= 0:
            raise DataError(
                "oracle_reconciliation", "reconstructed price differs from the pinned Spoke oracle"
            )
        return OraclePrice(
            source=reserve,
            oracle=oracle,
            feed=feed,
            price=reconstructed,
            dependencies=tuple(sorted(self.dependencies)),
        )
