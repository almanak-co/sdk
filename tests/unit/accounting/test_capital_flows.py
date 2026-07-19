"""Unit tests for the capital-flow transfer provenance reader (VIB-5866 leg B).

All RPC is served by an in-process fake provider — no sockets, no gateway.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.accounting.capital_flows import (
    CHUNK_BLOCKS,
    MAX_BACKLOG_BLOCKS,
    MAX_BLOCKS_PER_CYCLE,
    MIN_CHUNK_BLOCKS,
    TRANSFER_SIG,
    ZERO_ADDRESS,
    ChainScanResult,
    CounterpartyKind,
    FlowClassification,
    ScanStatus,
    TokenInfo,
    TransferDirection,
    clear_provenance_caches,
    pad_address_topic,
    scan_chain_transfers,
)

WALLET = "0x1111111111111111111111111111111111111111"
EOA = "0x2222222222222222222222222222222222222222"
CONTRACT = "0x3333333333333333333333333333333333333333"
SPENDER = "0x4444444444444444444444444444444444444444"
USDC = "0x" + "aa" * 20
WETH = "0x" + "bb" * 20

UNIVERSE = {
    USDC: TokenInfo(symbol="USDC", decimals=6),
    WETH: TokenInfo(symbol="WETH", decimals=18),
}


def _log(
    *,
    token: str = USDC,
    sender: str,
    recipient: str,
    amount: int = 1_000_000,
    tx_hash: str = "0xdead01",
    block: int = 100,
    log_index: int = 0,
    extra_topic: str | None = None,
) -> dict:
    topics = [TRANSFER_SIG, pad_address_topic(sender), pad_address_topic(recipient)]
    if extra_topic is not None:
        topics.append(extra_topic)
    return {
        "address": token,
        "topics": topics,
        "data": hex(amount),
        "transactionHash": tx_hash,
        "blockNumber": block,
        "logIndex": log_index,
    }


class FakeEth:
    """Minimal eth namespace honouring the filter shape the reader emits."""

    def __init__(
        self,
        logs: list[dict],
        code: dict[str, str] | None = None,
        txs: dict[str, dict] | None = None,
        *,
        max_span: int | None = None,
        fail_get_code: bool = False,
        fail_get_tx: bool = False,
    ) -> None:
        self.logs = logs
        self.code = {k.lower(): v for k, v in (code or {}).items()}
        self.txs = {k.lower(): v for k, v in (txs or {}).items()}
        self.max_span = max_span
        self.fail_get_code = fail_get_code
        self.fail_get_tx = fail_get_tx
        self.log_calls: list[tuple[int, int]] = []
        self.code_calls: list[str] = []
        self.tx_calls: list[str] = []

    def get_logs(self, params: dict) -> list[dict]:
        span = params["toBlock"] - params["fromBlock"] + 1
        self.log_calls.append((params["fromBlock"], params["toBlock"]))
        if self.max_span is not None and span > self.max_span:
            raise ValueError(f"query returned more than 10000 results / range {span} too wide")

        addresses = {a.lower() for a in params["address"]}
        sig, topic_from, topic_to = params["topics"]
        out = []
        for log in self.logs:
            if log["address"].lower() not in addresses:
                continue
            if not (params["fromBlock"] <= log["blockNumber"] <= params["toBlock"]):
                continue
            if log["topics"][0].lower() != sig.lower():
                continue
            if topic_from is not None and log["topics"][1].lower() != topic_from.lower():
                continue
            if topic_to is not None and log["topics"][2].lower() != topic_to.lower():
                continue
            out.append(log)
        return out

    def get_code(self, address: str) -> str:
        self.code_calls.append(address.lower())
        if self.fail_get_code:
            raise ConnectionError("provider down")
        return self.code.get(address.lower(), "0x")

    def get_transaction(self, tx_hash: str) -> dict:
        self.tx_calls.append(tx_hash.lower())
        if self.fail_get_tx:
            raise ConnectionError("provider down")
        if tx_hash.lower() not in self.txs:
            raise ValueError("not found")
        return self.txs[tx_hash.lower()]


class FakeWeb3:
    def __init__(self, eth: FakeEth) -> None:
        self.eth = eth


@pytest.fixture(autouse=True)
def _clear_caches():
    clear_provenance_caches()
    yield
    clear_provenance_caches()


def _scan(
    logs: list[dict],
    *,
    code: dict[str, str] | None = None,
    txs: dict[str, dict] | None = None,
    ledger: tuple[str, ...] = (),
    universe: dict | None = None,
    from_block: int = 0,
    head: int = 1_000,
    max_span: int | None = None,
    fail_get_code: bool = False,
    fail_get_tx: bool = False,
) -> ChainScanResult:
    eth = FakeEth(
        logs,
        code,
        txs,
        max_span=max_span,
        fail_get_code=fail_get_code,
        fail_get_tx=fail_get_tx,
    )
    return scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=from_block,
        head_block=head,
        token_universe=universe if universe is not None else UNIVERSE,
        ledger_tx_hashes=ledger,
    )


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


def test_ledger_tx_is_strategy_tx_and_skips_code_lookup():
    eth = FakeEth(
        [_log(sender=CONTRACT, recipient=WALLET, tx_hash="0xABC1")],
        {CONTRACT: "0x6080"},
    )
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
        ledger_tx_hashes=("0xabc1",),
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.STRATEGY_TX
    assert obs.counterparty_kind is CounterpartyKind.UNKNOWN
    # The ledger short-circuit must actually skip the code probe, not
    # fetch-and-discard it.
    assert eth.code_calls == []


def test_eoa_inflow_is_deposit():
    result = _scan([_log(sender=EOA, recipient=WALLET, amount=5_000_000)])
    (obs,) = result.observations
    assert obs.classification is FlowClassification.DEPOSIT
    assert obs.direction is TransferDirection.IN
    assert obs.counterparty == EOA.lower()
    assert obs.counterparty_kind is CounterpartyKind.EOA
    assert obs.amount == Decimal("5")
    assert obs.symbol == "USDC"
    assert obs.measurable is True


def test_wallet_sent_eoa_outflow_is_withdrawal():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xfeed")],
        txs={"0xfeed": {"from": WALLET}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.WITHDRAWAL
    assert obs.direction is TransferDirection.OUT


def test_eip7702_delegated_counterparty_is_eoa_deposit():
    result = _scan(
        [_log(sender=EOA, recipient=WALLET)],
        code={EOA: "0xef0100" + "11" * 20},
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.EOA
    assert obs.classification is FlowClassification.DEPOSIT


def test_contract_inflow_is_unclassified_not_deposit():
    result = _scan(
        [_log(sender=CONTRACT, recipient=WALLET)],
        code={CONTRACT: "0x6080604052"},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN
    assert obs.counterparty_kind is CounterpartyKind.CONTRACT


def test_transfer_from_pull_is_unclassified_out_never_withdrawal():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xpull")],
        txs={"0xpull": {"from": SPENDER}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_tx_fetch_failure_is_unclassified_out():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xgone")],
        fail_get_tx=True,
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_code_fetch_failure_is_unknown_and_unclassified():
    result = _scan([_log(sender=EOA, recipient=WALLET)], fail_get_code=True)
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.UNKNOWN
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN


def test_mint_from_zero_address_is_mint_burn_never_deposit():
    eth = FakeEth([_log(sender=ZERO_ADDRESS, recipient=WALLET)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.MINT_BURN
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN
    # address(0) must never be probed with eth_getCode — it has no code and
    # would masquerade as an EOA.
    assert eth.code_calls == []


def test_burn_to_zero_address_is_mint_burn_never_withdrawal():
    eth = FakeEth([_log(sender=WALLET, recipient=ZERO_ADDRESS, tx_hash="0xburn")])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.MINT_BURN
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT
    assert eth.code_calls == []
    assert eth.tx_calls == []


# --------------------------------------------------------------------------
# Scan mechanics
# --------------------------------------------------------------------------


def test_self_transfer_is_dropped():
    result = _scan([_log(sender=WALLET, recipient=WALLET)])
    assert result.observations == ()
    assert result.status is ScanStatus.OK


def test_transfer_seen_by_both_topic_scans_is_deduped():
    log = _log(sender=EOA, recipient=WALLET, tx_hash="0xdupe", log_index=3)

    class EchoEth(FakeEth):
        """Returns the same log from both the inbound and outbound filter."""

        def get_logs(self, params):
            self.log_calls.append((params["fromBlock"], params["toBlock"]))
            return [log]

    eth = EchoEth([log])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe=UNIVERSE,
    )
    assert len(eth.log_calls) == 2
    assert len(result.observations) == 1


def test_erc721_four_topic_transfer_is_ignored():
    result = _scan(
        [
            _log(
                sender=EOA,
                recipient=WALLET,
                extra_topic="0x" + "00" * 31 + "07",
            )
        ]
    )
    assert result.observations == ()


def test_same_symbol_on_two_chains_keyed_apart():
    logs = [_log(sender=EOA, recipient=WALLET, tx_hash="0xa1")]
    arb = _scan(logs)
    eth = FakeEth(logs)
    base = scan_chain_transfers(
        FakeWeb3(eth),
        chain="base",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (a,) = arb.observations
    (b,) = base.observations
    assert a.symbol == b.symbol == "USDC"
    assert a.key != b.key
    assert (a.chain, a.token_address) == ("arbitrum", USDC.lower())
    assert (b.chain, b.token_address) == ("base", USDC.lower())


def test_missing_decimals_marks_observation_unmeasurable():
    result = _scan(
        [_log(token=WETH, sender=EOA, recipient=WALLET, amount=10**18)],
        universe={WETH: TokenInfo(symbol="WETH", decimals=None)},
    )
    (obs,) = result.observations
    assert obs.measurable is False
    assert obs.amount is None
    assert obs.raw_amount == 10**18
    assert result.has_unmeasurable is True


def test_measurable_scan_has_no_unmeasurable_flag():
    result = _scan([_log(sender=EOA, recipient=WALLET)])
    assert result.has_unmeasurable is False


def test_hex_case_normalization_of_hashes_and_addresses():
    result = _scan(
        [_log(token=USDC.upper(), sender=EOA.upper(), recipient=WALLET.upper(), tx_hash="0xDEADBEEF")],
        ledger=("0xDeAdBeEf",),
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.STRATEGY_TX
    assert obs.tx_hash == "0xdeadbeef"
    assert obs.token_address == USDC.lower()
    assert obs.counterparty == EOA.lower()


def test_block_budget_truncation_within_backlog_cap():
    result = _scan([], from_block=1_000, head=1_000 + MAX_BLOCKS_PER_CYCLE + 25_000)
    assert result.status is ScanStatus.OK
    assert result.to_block == 1_000 + MAX_BLOCKS_PER_CYCLE


def test_backlog_beyond_cap_is_range_unmeasurable():
    result = _scan([], from_block=0, head=MAX_BACKLOG_BLOCKS + 1)
    assert result.status is ScanStatus.RANGE_UNMEASURABLE
    assert result.observations == ()
    assert result.error is not None


def test_chunk_halving_then_success():
    eth = FakeEth([], max_span=CHUNK_BLOCKS // 2)
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=CHUNK_BLOCKS,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.OK
    assert result.to_block == CHUNK_BLOCKS
    # First attempt used the full chunk and failed; the retry halved it, then
    # two half-chunks x two directions completed the range — no extra calls.
    assert eth.log_calls[0] == (1, CHUNK_BLOCKS)
    assert eth.log_calls[1] == (1, CHUNK_BLOCKS // 2)
    assert len(eth.log_calls) == 5


def test_chunk_halving_hits_floor_and_returns_transient_failure():
    eth = FakeEth([], max_span=MIN_CHUNK_BLOCKS - 1)
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=500,
        head_block=20_000,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.TRANSIENT_FAILURE
    assert result.error is not None
    # Nothing was scanned, so the caller's cursor must not advance.
    assert result.to_block == 500


def test_transient_failure_keeps_earlier_chunk_observations():
    # Chunk 1 succeeds at the halved size, then the provider dies outright.
    class FlakyEth(FakeEth):
        def get_logs(self, params):
            if params["fromBlock"] > CHUNK_BLOCKS:
                raise ValueError("range too wide")
            return super().get_logs(params)

    eth = FlakyEth([_log(sender=EOA, recipient=WALLET, block=42)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=20_000,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.TRANSIENT_FAILURE
    assert result.to_block == CHUNK_BLOCKS
    assert [o.classification for o in result.observations] == [FlowClassification.DEPOSIT]


def test_empty_universe_short_circuits():
    eth = FakeEth([_log(sender=EOA, recipient=WALLET)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe={},
    )
    assert result.observations == ()
    assert eth.log_calls == []


def test_code_lookup_is_cached_across_transfers():
    logs = [
        _log(sender=EOA, recipient=WALLET, tx_hash="0xa1", log_index=0),
        _log(sender=EOA, recipient=WALLET, tx_hash="0xa2", log_index=1),
    ]
    eth = FakeEth(logs)
    scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe=UNIVERSE,
    )
    assert eth.code_calls == [EOA.lower()]


def test_observations_are_block_ordered():
    logs = [
        _log(sender=EOA, recipient=WALLET, tx_hash="0xb2", block=200, log_index=1),
        _log(sender=EOA, recipient=WALLET, tx_hash="0xb1", block=100, log_index=0),
    ]
    result = _scan(logs)
    assert [o.block_number for o in result.observations] == [100, 200]


def test_rpc_address_params_are_checksummed_like_web3_middleware():
    """web3.py's validation middleware rejects lowercase address params.

    Found live in the VIB-5866 real-fork proof run: a lowercased ``address``
    filter makes every ``eth_getLogs`` call fail before reaching the node,
    which a case-insensitive fake provider can never catch. This fake enforces
    EIP-55 exactly like the middleware.
    """
    from web3 import Web3

    token = "0x00000000000000000000000000000000000000ab"
    eoa = "0x00000000000000000000000000000000000000cd"

    class ChecksumEnforcingEth(FakeEth):
        def get_logs(self, params: dict) -> list[dict]:
            for entry in params["address"]:
                if entry != Web3.to_checksum_address(entry):
                    raise ValueError("middleware: address param is not EIP-55 checksummed")
            return super().get_logs(params)

        def get_code(self, address: str) -> str:
            if address != Web3.to_checksum_address(address):
                raise ValueError("middleware: address param is not EIP-55 checksummed")
            return super().get_code(address)

    eth = ChecksumEnforcingEth([_log(sender=eoa, recipient=WALLET, token=token)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        # Mixed-case caller input must still reach the wire checksummed.
        token_universe={token.upper().replace("0X", "0x"): TokenInfo(symbol="TOK", decimals=6)},
    )
    assert result.status is ScanStatus.OK
    (obs,) = result.observations
    assert obs.classification is FlowClassification.DEPOSIT
    assert eth.code_calls, "code lookup must have gone through the checksum path"
