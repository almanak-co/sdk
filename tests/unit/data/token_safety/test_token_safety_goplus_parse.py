import pytest

from almanak.framework.data.token_safety.client import TokenSafetyClient

MINT_ADDRESS = "Mint111111111111111111111111111111111111111"


def _parse(data: dict, mint_address: str = MINT_ADDRESS):
    client = object.__new__(TokenSafetyClient)
    return client._parse_goplus_response(data, mint_address)


def _response(token_data: dict, mint_address: str = MINT_ADDRESS) -> dict:
    return {"code": 1, "result": {mint_address: token_data}}


def test_parse_goplus_response_returns_none_for_error_code() -> None:
    result = _parse({"code": 0, "message": "bad request", "result": {MINT_ADDRESS: {}}})

    assert result is None


def test_parse_goplus_response_string_success_code_is_error() -> None:
    result = _parse({"code": "1", "result": {MINT_ADDRESS: {"mintable": "1"}}})

    assert result is None


def test_parse_goplus_response_missing_result_returns_none() -> None:
    result = _parse({"code": 1})

    assert result is None


def test_parse_goplus_response_returns_none_when_token_missing() -> None:
    result = _parse({"code": 1, "result": {"OtherMint": {"mintable": "1"}}})

    assert result is None


def test_parse_goplus_response_falsey_token_payload_returns_none() -> None:
    result = _parse({"code": 1, "result": {MINT_ADDRESS: {}, MINT_ADDRESS.lower(): {}}})

    assert result is None


def test_parse_goplus_response_matches_mint_case_insensitively() -> None:
    token_data = {"mintable": {"status": "1"}}

    result = _parse({"code": 1, "result": {MINT_ADDRESS.lower(): token_data}})

    assert result is not None
    assert result.mintable is True
    assert result.raw_response is token_data


def test_parse_goplus_response_maps_authority_status_fields() -> None:
    result = _parse(
        _response(
            {
                "mintable": {"status": "1"},
                "freezable": "1",
                "closable": {"status": "0"},
                "balance_mutable_authority": "1",
                "transfer_fee_upgradable": {"status": "1"},
                "transfer_hook_upgradable": "1",
                "metadata_mutable": {"status": "1"},
            }
        )
    )

    assert result is not None
    assert result.mintable is True
    assert result.freezable is True
    assert result.closable is False
    assert result.balance_mutable is True
    assert result.transfer_fee_upgradable is True
    assert result.transfer_hook_upgradable is True
    assert result.metadata_mutable is True


@pytest.mark.parametrize(
    ("transfer_fee", "expected"),
    [
        ({}, False),
        ({"fee_rate": "0", "current_fee_rate": "0"}, False),
        ({"fee_rate": "0.0"}, True),
        ({"fee_rate": "5"}, True),
        ({"fee_rate": 0, "current_fee_rate": "7"}, True),
        (["unexpected"], False),
    ],
)
def test_parse_goplus_response_detects_nonzero_transfer_fee(transfer_fee, expected) -> None:
    result = _parse(_response({"transfer_fee": transfer_fee}))

    assert result is not None
    assert result.has_transfer_fee is expected


@pytest.mark.parametrize(
    ("transfer_hook", "expected"),
    [
        ([], False),
        ([{"program": "hook"}], True),
        ("hook-program", True),
        ("", False),
    ],
)
def test_parse_goplus_response_detects_transfer_hook_shapes(transfer_hook, expected) -> None:
    result = _parse(_response({"transfer_hook": transfer_hook}))

    assert result is not None
    assert result.transfer_hook is expected


def test_parse_goplus_response_parses_state_trusted_and_holder_stats() -> None:
    result = _parse(
        _response(
            {
                "non_transferable": "1",
                "default_account_state": "2",
                "trusted_token": "1",
                "holder_count": "123",
                "holders": [{"percent": "42.5"}],
            }
        )
    )

    assert result is not None
    assert result.non_transferable is True
    assert result.default_account_state_frozen is True
    assert result.trusted_token is True
    assert result.holder_count == 123
    assert result.top_holder_pct == 42.5


@pytest.mark.parametrize("holders", [[{"percent": "not-a-number"}], [], "not-a-list"])
def test_parse_goplus_response_invalid_holder_percent_falls_back_to_zero(holders) -> None:
    result = _parse(_response({"holders": holders}))

    assert result is not None
    assert result.top_holder_pct == 0.0


@pytest.mark.parametrize(
    "token_data",
    [
        {"default_account_state": "not-an-int"},
        {"trusted_token": "not-an-int"},
        {"holder_count": "not-an-int"},
    ],
)
def test_parse_goplus_response_preserves_loud_integer_coercion(token_data) -> None:
    with pytest.raises(ValueError):
        _parse(_response(token_data))


@pytest.mark.parametrize(
    "data",
    [
        {"code": 1, "result": None},
        {"code": 1, "result": {MINT_ADDRESS: "truthy-token-payload"}},
        {"code": 1, "result": {MINT_ADDRESS: {"holders": ["not-a-dict"]}}},
    ],
)
def test_parse_goplus_response_preserves_loud_malformed_shapes(data) -> None:
    with pytest.raises(AttributeError):
        _parse(data)
