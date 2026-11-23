"""The venue quote is parsed strictly and refused whenever it cannot be signed safely."""

import json

import httpx
import pytest
from pydantic import ValidationError

from almanak.caip import AssetType
from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import SPOKE_POOLS, refuse_native_output
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS, Account, AccountBinding, SafeRoles, StrategyAccounts
from tests.across_support import (
    ACCOUNTS,
    AMOUNT,
    ARBITRUM,
    BASE,
    BASE_USDC,
    BASE_WETH,
    CONFIG,
    INTENT,
    NOW,
    PIN,
    POOL,
    RECIPIENT,
    SENDER,
    TARGET,
    USDC,
    allocation,
    body,
    eoa,
    parameters,
    quote,
    route,
)

WETH = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        (
            {"destination_chain": ARBITRUM.chain_id, "destination_asset": USDC},
            "two different chains",
        ),
        ({"spoke_pool": "0x" + "33" * 20}, "another SpokePool"),
        ({"asset": WETH}, "wrapped native"),
        ({"destination_asset": USDC}, "chain"),
        ({"quote": "coingecko"}, "across_api"),
        ({"max_fee_bps": 10_001}, "10000"),
    ],
)
def test_a_route_refuses_what_this_scope_cannot_bridge(changes, cause):
    with pytest.raises(ValidationError, match=cause):
        route(**changes)


def test_a_wrapped_native_output_needs_a_contract_recipient():
    wrapped = route(destination_asset=BASE_WETH)
    refuse_native_output(wrapped, "safe_roles")
    refuse_native_output(route(), "eoa")
    with pytest.raises(ValueError, match="native asset"):
        refuse_native_output(wrapped, "eoa")
    with pytest.raises(ValidationError, match="native asset"):
        parameters(allocation(route=wrapped))


def test_the_bridge_action_names_both_funds_and_the_sent_amount():
    assert INTENT.kind == "bridge" and INTENT.amount.amount == AMOUNT
    assert (TARGET.fund.address, TARGET.destination_fund.address) == (SENDER, RECIPIENT)
    assert (TARGET.asset, TARGET.destination_asset) == (USDC, BASE_USDC)
    assert CONFIG.target("move") == TARGET


def safe(name, chain, address):
    fund = Account(chain=chain, address=address)
    profile = SafeRoles(
        kind="safe_roles",
        fund=fund,
        operator=Account(chain=chain, address="0x" + "44" * 20),
        roles_module="0x" + "55" * 20,
        role_key="0x" + "66" * 32,
    )
    return AccountBinding(name=name, profile=profile)


@pytest.mark.parametrize(
    ("build", "cause"),
    [
        (lambda: parameters(allocation(destination_account="elsewhere")), "unbound account"),
        (lambda: parameters(allocation(destination_account="arb")), "two different named"),
        (
            lambda: parameters(
                accounts=StrategyAccounts(
                    bindings=(*ACCOUNTS.bindings, eoa("idle", BASE, "0x" + "77" * 20))
                )
            ),
            "must send or receive",
        ),
        (
            lambda: parameters(
                accounts=StrategyAccounts(
                    bindings=(safe("arb", ARBITRUM, SENDER), ACCOUNTS.bindings[1])
                )
            ),
            "EOA account only",
        ),
        (
            lambda: parameters(
                accounts=StrategyAccounts(
                    bindings=(ACCOUNTS.bindings[0], eoa("base", ARBITRUM, RECIPIENT))
                )
            ),
            "different fund chain",
        ),
        (lambda: parameters(allocation(), allocation()), "unique"),
    ],
)
def test_parameters_refuse_accounts_a_bridge_cannot_use(build, cause):
    with pytest.raises(ValidationError, match=cause):
        build()


def test_the_request_names_the_exact_route_amount_and_recipient():
    assert venue.request(TARGET, AMOUNT) == {
        "inputToken": USDC.reference,
        "outputToken": BASE_USDC.reference,
        "originChainId": "42161",
        "destinationChainId": "8453",
        "amount": str(AMOUNT),
        "recipient": RECIPIENT,
    }


def test_a_quote_keeps_the_venue_answer_as_received():
    parsed = quote()
    assert (parsed.output_amount, parsed.timestamp, parsed.fill_deadline) == (
        999_897_038,
        NOW - 69,
        NOW - 69 + 7200,
    )
    assert (parsed.exclusivity_parameter, parsed.spoke_pool) == (3, PIN.address)
    assert json.loads(parsed.response) == body() and parsed.source == "across_api"
    assert (parsed.input_token, parsed.output_token) == (USDC.reference, BASE_USDC.reference)
    assert (parsed.input_decimals, parsed.output_decimals) == (6, 6)
    assert parsed.destination_spoke_pool == SPOKE_POOLS[BASE.chain_id].address
    venue.admit(parsed, TARGET, AMOUNT, POOL, NOW)


@pytest.mark.parametrize(
    "changes",
    [
        {"outputAmount": "1.5"},
        {"outputAmount": None},
        {"outputAmount": "-1"},
        {"outputAmount": " 999 "},
        {"outputAmount": "999_897_038"},
        {"outputAmount": "+1"},
        {"outputAmount": "\u0663"},
        {"outputAmount": "9" * 79},
        {
            "inputToken": {
                "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                "decimals": "6" * 5000,
            }
        },
        {"outputAmount": -1},
        {"outputAmount": 1.0},
        {"inputToken": "0xaf88d065e77c8cc2239327c5edb3a432268e5831"},
        {"outputToken": {"address": 5, "decimals": 6}},
        {"outputToken": {"address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"}},
        {"extra": float("nan")},
        {"extra": float("inf")},
        {"extra": "x" * venue.MAX_ANSWER_BYTES},
        {"timestamp": True},
        {"fillDeadline": str(2**32)},
        {"isAmountTooLow": "false"},
        {"exclusiveRelayer": "0x12"},
    ],
)
def test_a_malformed_quote_is_refused(changes):
    with pytest.raises(DataError) as error:
        quote(**changes)
    assert error.value.kind == "malformed"


def test_a_quote_missing_a_field_names_it():
    incomplete = body()
    del incomplete["fillDeadline"]
    with pytest.raises(DataError, match="lacks fillDeadline"):
        venue.parse(incomplete, AMOUNT)


@pytest.mark.parametrize(
    ("changes", "kind"),
    [
        ({"outputAmount": str(AMOUNT - 1_000_001)}, "fee_above_ceiling"),
        ({"outputAmount": str(AMOUNT + 1)}, "quote_mismatch"),
        ({"outputAmount": "0"}, "amount_too_low"),
        ({"isAmountTooLow": True}, "amount_too_low"),
        ({"spokePoolAddress": "0x" + "33" * 20}, "quote_mismatch"),
        ({"destinationSpokePoolAddress": "0x" + "33" * 20}, "quote_mismatch"),
        ({"inputToken": {"address": "0x" + "44" * 20, "decimals": 6}}, "quote_mismatch"),
        ({"outputToken": {"address": "0x" + "44" * 20, "decimals": 6}}, "quote_mismatch"),
        ({"inputToken": {"address": USDC.reference, "decimals": 18}}, "quote_mismatch"),
        ({"outputToken": {"address": BASE_USDC.reference, "decimals": 18}}, "decimals_differ"),
        ({"timestamp": str(NOW + 1)}, "quote_time"),
        ({"timestamp": str(NOW - 3601)}, "quote_time"),
        ({"fillDeadline": str(NOW + 21601)}, "fill_deadline"),
        ({"fillDeadline": str(NOW + 599)}, "fill_deadline"),
        ({"exclusivityDeadline": 7200}, "exclusivity"),
        ({"exclusivityDeadline": NOW + 100_000_000}, "exclusivity"),
        ({"exclusiveRelayer": ZERO_ADDRESS}, "exclusivity"),
    ],
)
def test_a_quote_that_cannot_be_signed_is_refused(changes, kind):
    with pytest.raises(DataError) as error:
        venue.admit(quote(**changes), TARGET, AMOUNT, POOL, NOW)
    assert error.value.kind == kind


def test_the_fee_ceiling_rounds_down_and_admits_its_exact_bound():
    assert venue.fee_ceiling(AMOUNT, 10) == 1_000_000
    assert venue.fee_ceiling(999, 10) == 0
    venue.admit(quote(outputAmount=str(AMOUNT - 1_000_000)), TARGET, AMOUNT, POOL, NOW)
    venue.admit(
        quote(exclusivityDeadline=0, exclusiveRelayer=ZERO_ADDRESS), TARGET, AMOUNT, POOL, NOW
    )


def test_a_quote_of_another_amount_is_refused():
    with pytest.raises(DataError, match="another source or amount"):
        venue.admit(quote(), TARGET, AMOUNT + 1, POOL, NOW)


def client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_asks_once_and_sends_the_key_only_as_a_header():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=body())

    fetched = venue.fetch(TARGET, AMOUNT, api_key="secret", client=client(handler))
    assert fetched == quote() and len(seen) == 1
    assert seen[0].url.params["amount"] == str(AMOUNT)
    assert seen[0].headers["authorization"] == "Bearer secret"
    assert "secret" not in str(seen[0].url) and "secret" not in fetched.model_dump_json()
    venue.fetch(TARGET, AMOUNT, client=client(handler))
    assert "authorization" not in seen[1].headers


@pytest.mark.parametrize(
    ("response", "kind"),
    [
        (httpx.Response(429), "rate_limited"),
        (httpx.Response(400, json={"message": "no route"}), "quote_refused"),
        (httpx.Response(500), "quote_refused"),
        (httpx.Response(200, text="not json"), "malformed"),
        (httpx.Response(302, headers={"location": "https://example.invalid"}), "quote_refused"),
        (httpx.Response(204), "quote_refused"),
        (httpx.Response(200, text='{"outputAmount": NaN}'), "malformed"),
        (httpx.Response(200, text='{"pad": "' + "x" * venue.MAX_ANSWER_BYTES + '"}'), "malformed"),
        (httpx.Response(200, json=[]), "malformed"),
    ],
)
def test_a_refusal_or_throttle_is_reported_as_what_it_is(response, kind):
    with pytest.raises(DataError) as error:
        venue.fetch(TARGET, AMOUNT, client=client(lambda request: response))
    assert error.value.kind == kind


def test_an_unanswered_request_reports_no_request_detail():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(DataError) as error:
        venue.fetch(TARGET, AMOUNT, api_key="secret", client=client(handler))
    assert error.value.kind == "quote_unavailable"
    assert "secret" not in str(error.value) and RECIPIENT not in str(error.value)


def test_the_retained_answer_survives_a_round_trip_byte_for_byte():
    parsed = quote(extra={"b": 1.5, "a": [1, "two"]})
    again = venue.Quote.model_validate_json(parsed.model_dump_json())
    assert again == parsed and again.response == parsed.response
    assert parsed.response == json.dumps(
        json.loads(parsed.response), sort_keys=True, separators=(",", ":")
    )
    assert len(parsed.response.encode()) <= venue.MAX_ANSWER_BYTES
