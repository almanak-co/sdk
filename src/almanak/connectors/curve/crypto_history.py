"""Retain Crypto ramps, fee controls and factory receiver changes between samples."""

from dataclasses import dataclass

from almanak.connectors.curve.crypto_observation import CryptoRamp
from almanak.connectors.curve.receipts import topic
from almanak.connectors.curve.tricrypto_observation import TriCryptoRamp
from almanak.gateway.evm_logs import data

RAMP = topic("RampAgamma(uint256,uint256,uint256,uint256,uint256,uint256)")
STOP = topic("StopRampA(uint256,uint256,uint256)")
PARAMETERS = topic("NewParameters(uint256,uint256,uint256,uint256,uint256,uint256)")
TOKEN_PARAMETERS = topic("NewParameters(uint256,uint256,uint256,uint256,uint256,uint256,uint256)")
RECEIVER = topic("UpdateFeeReceiver(address,address)")
VIEWS = topic("UpdateViewsImplementation(address,address)")
POOL_TOPICS = RAMP, STOP, PARAMETERS
FACTORY_TOPICS = RECEIVER, VIEWS


@dataclass(frozen=True)
class Fees:
    packed_fees: int
    packed_rebalancing: int
    xcp_ma_time: int | None = None


def pool_topics(source):
    if source.implementation_revision == "tricrypto-ng-2024-01-04":
        return RAMP, STOP, TOKEN_PARAMETERS
    return POOL_TOPICS


def observed_fees(observation):
    tokens = getattr(observation, "token_accounting", None)
    return Fees(
        observation.packed_fees,
        observation.packed_rebalancing,
        tokens.xcp_ma_time if tokens else None,
    )


@dataclass(frozen=True)
class Receiver:
    previous: str
    current: str


def pack(values):
    return (values[0] << 128) | (values[1] << 64) | values[2]


def fee_parameters(raw, *, token_revision=False):
    values = data(raw, ("uint256",) * (7 if token_revision else 6))
    mid, out, gamma, profit, step, duration = values[:6]
    xcp_duration = values[6] if token_revision else None
    if not 0 <= mid <= out <= 10**10 or out < 5 * 10**5 or not 0 < gamma <= 10**18:
        raise ValueError("Crypto NG fee event exceeds the verified contract bounds")
    if max(profit, step) > 10**18 or not 86 < duration < 872542:
        raise ValueError("Crypto NG rebalancing event exceeds the verified contract bounds")
    if xcp_duration is not None and not 86 < xcp_duration < 872542:
        raise ValueError("TriCrypto XCP oracle duration exceeds the verified contract bounds")
    return Fees(pack((mid, out, gamma)), pack((profit, step, duration)), xcp_duration)


def ramp_event(raw, timestamp, stopped, *, family="twocrypto_ng"):
    if stopped:
        initial_a, initial_g, start = data(raw, ("uint256",) * 3)
        future_a, future_g, end = initial_a, initial_g, start
    else:
        initial_a, future_a, initial_g, future_g, start, end = data(raw, ("uint256",) * 6)
    if start != timestamp:
        raise ValueError("Crypto NG ramp event differs from its canonical effective time")
    ramp_type = TriCryptoRamp if family == "tricrypto_ng" else CryptoRamp
    return ramp_type(
        initial_amplification=initial_a,
        future_amplification=future_a,
        initial_gamma=initial_g,
        future_gamma=future_g,
        initial_time=start,
        future_time=end,
    )


def parameter_event(event, source):
    if len(event.topics) != 1:
        raise ValueError("Crypto NG parameter event has an unexpected indexed layout")
    signature, raw = event.topics[0], {"data": event.data}
    expected = source.factory if signature in FACTORY_TOPICS else source.pool
    if event.address != expected:
        raise ValueError("Crypto NG parameter event identifies another contract")
    if signature in (RAMP, STOP):
        return ramp_event(raw, event.block.timestamp, signature == STOP, family=source.family)
    if signature in (PARAMETERS, TOKEN_PARAMETERS):
        if signature not in pool_topics(source):
            raise ValueError("Crypto parameter ABI differs from the configured revision")
        return fee_parameters(raw, token_revision=signature == TOKEN_PARAMETERS)
    if signature == RECEIVER:
        return Receiver(*data(raw, ("address", "address")))
    if signature == VIEWS:
        raise ValueError("Crypto NG prepared window contains an unsupported views revision change")
    raise ValueError("Crypto NG parameter path contains an unsupported event")


def validate_parameters(previous, current, events):
    ramp = previous.ramp
    fees = observed_fees(previous)
    receiver = previous.accounting.receiver
    for event in events:
        result = parameter_event(event, previous.source)
        if isinstance(result, (CryptoRamp, TriCryptoRamp)):
            if (result.initial_amplification, result.initial_gamma) != ramp.at(
                event.block.timestamp
            ):
                raise ValueError("Crypto NG ramp breaks the retained parameter path")
            ramp = result
        elif isinstance(result, Receiver):
            if result.previous != receiver:
                raise ValueError("Crypto NG fee receiver breaks the retained parameter path")
            receiver = result.current
        else:
            fees = result
    if (ramp, fees, receiver) != (
        current.ramp,
        observed_fees(current),
        current.accounting.receiver,
    ):
        raise ValueError("Crypto NG parameters changed without complete retained event evidence")
