"""Bounded pool-state reads through the read-only archival gateway."""

from eth_abi import encode
from eth_utils import keccak

from almanak.backtest.dataset import RawEvidence
from almanak.backtest.history import ReadRequest
from almanak.backtest.types import UnixTime
from almanak.connectors.uniswap_v4.economic_data import PoolSample, Record, Tick, Word, key
from almanak.connectors.uniswap_v4.history import contract_identities, tick_gross
from almanak.connectors.uniswap_v4.hook_terms import read_terms
from almanak.connectors.uniswap_v4.pool import pool_id, read_pool
from almanak.connectors.uniswap_v4.reader import fee_growth


def requests(target, block, signature, argument_type, values):
    identifier = bytes.fromhex(pool_id(target.pool)[2:])
    return tuple(
        ReadRequest(
            chain=block.chain,
            method="eth_call",
            params=(
                {
                    "to": target.pool.state_view,
                    "data": "0x"
                    + (
                        keccak(text=signature)[:4]
                        + encode(("bytes32", argument_type), (identifier, value))
                    ).hex(),
                },
                {"blockHash": block.hash, "requireCanonical": True},
            ),
        )
        for value in values
    )


def tick_state(reader, target, block, policy):
    bound = policy.for_target(target)
    spacing = target.pool.tick_spacing
    indices = range((bound.lower // spacing) >> 8, ((bound.upper // spacing) >> 8) + 1)
    if len(indices) > policy.max_bitmap_words:
        raise ValueError("tick coverage exceeds the explicit bitmap collection limit")
    # Only the historical backtest reader warms a cache; a live fork reader has
    # no prefetch, and every actual read below goes through reader.call.
    if hasattr(reader, "prefetch"):
        reader.prefetch(requests(target, block, "getTickBitmap(bytes32,int16)", "int16", indices))
    identifier = bytes.fromhex(pool_id(target.pool)[2:])
    words = tuple(
        Word(
            index=index,
            bitmap=reader.call(
                target.pool.state_view,
                "getTickBitmap(bytes32,int16)",
                block,
                inputs=("bytes32", "int16"),
                args=(identifier, index),
                outputs=("uint256",),
            )[0],
        )
        for index in indices
    )
    ticks = tuple(
        ((w.index << 8) + bit) * spacing
        for w in words
        for bit in range(256)
        if w.bitmap & (1 << bit)
    )
    if len(ticks) > policy.max_initialized_ticks:
        raise ValueError("pool exceeds the explicit initialized-tick collection limit")
    if hasattr(reader, "prefetch"):
        reader.prefetch(requests(target, block, "getTickLiquidity(bytes32,int24)", "int24", ticks))
    states = []
    for tick in ticks:
        gross, net = reader.call(
            target.pool.state_view,
            "getTickLiquidity(bytes32,int24)",
            block,
            inputs=("bytes32", "int24"),
            args=(identifier, tick),
            outputs=("uint128", "int128"),
        )
        states.append(Tick(index=tick, gross=gross, net=net))
    return words, tuple(states)


def tick_growth(reader, target, block, ticks):
    """Global growth and each initialized tick's outside growth at one pinned block."""
    identifier = bytes.fromhex(pool_id(target.pool)[2:])
    indices = tuple(tick.index for tick in ticks)
    if hasattr(reader, "prefetch"):
        reader.prefetch(
            requests(target, block, "getTickFeeGrowthOutside(bytes32,int24)", "int24", indices)
        )
    outside = tuple(
        tuple(
            reader.call(
                target.pool.state_view,
                "getTickFeeGrowthOutside(bytes32,int24)",
                block,
                inputs=("bytes32", "int24"),
                args=(identifier, index),
                outputs=("uint256", "uint256"),
            )
        )
        for index in indices
    )
    total = reader.call(
        target.pool.state_view,
        "getFeeGrowthGlobals(bytes32)",
        block,
        inputs=("bytes32",),
        args=(identifier,),
        outputs=("uint256", "uint256"),
    )
    return {"global_growth": tuple(total), "outside": outside}


def evidence(reader):
    return tuple(RawEvidence(body=reader.cache[k].model_dump_json()) for k in sorted(reader.used))


def sample(history, target, at, policy, baseline):
    reader = history.reader
    anchor = history.floor(UnixTime(at))
    reader.used.clear()
    block = history.pin(anchor)
    identities = contract_identities(reader, target, block)
    if identities != baseline.contracts:
        raise ValueError("prepared sample has a changed implementation")
    pool = read_pool(reader, target.pool, block)
    bound = policy.for_target(target)
    words, ticks = tick_state(reader, target, block, policy)
    proof = RawEvidence(body=baseline.model_dump_json())
    payload = PoolSample(
        baseline_digest=proof.digest,
        pool=pool,
        lower=bound.lower,
        upper=bound.upper,
        words=words,
        ticks=ticks,
        fee_growth=fee_growth(reader, target, block) if target.range else None,
        boundary_gross=tick_gross(reader, target, block) if target.range else None,
        **(tick_growth(reader, target, block, ticks) if target.band else {}),
        hook=read_terms(reader, target.pool, block),
    )
    raw = (*evidence(reader), proof)
    record = Record(
        key=key(target),
        unit="uniswap_v4_integer_pool",
        payload=payload,
        reference=block,
        source_time=UnixTime(block.timestamp),
        available_at=UnixTime(at),
        retrieved_at=max(reader.cache[k].retrieved_at for k in reader.used),
        evidence=tuple(e.digest for e in raw),
    )
    return record, raw
