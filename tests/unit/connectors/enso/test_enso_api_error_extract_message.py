"""Branch coverage for EnsoAPIError._extract_api_message.

The extractor runs in EnsoAPIError.__init__ against ``error_data`` and must
handle every error shape the Enso API has emitted: ``{"error": "..."}``
(string), ``{"error": {"message": "..."}}`` (nested), top-level ``message``,
legacy ``errorMessage``, plus the defensive Nones for missing / non-dict /
unrecognised payloads. ``__str__`` consumes the result, so the formatting
fallbacks are locked in alongside. Pure construction — no network.
"""

from almanak.connectors.enso.exceptions import EnsoAPIError


def _error(error_data, status_code=400, endpoint="/api/v1/shortcuts/route"):
    return EnsoAPIError(
        message="API request failed",
        status_code=status_code,
        endpoint=endpoint,
        error_data=error_data,
    )


class TestExtractApiMessage:
    def test_none_error_data_returns_none(self):
        assert _error(None).api_error_message is None

    def test_non_dict_error_data_returns_none(self):
        assert _error(["not", "a", "dict"]).api_error_message is None

    def test_empty_dict_returns_none(self):
        # Falsy dict short-circuits on the ``not self.error_data`` guard.
        assert _error({}).api_error_message is None

    def test_error_key_with_string_value(self):
        err = _error({"error": "Insufficient liquidity"})
        assert err.api_error_message == "Insufficient liquidity"

    def test_error_key_with_nested_message_dict(self):
        err = _error({"error": {"message": "Token not supported", "code": 42}})
        assert err.api_error_message == "Token not supported"

    def test_error_key_with_dict_missing_message_returns_none(self):
        assert _error({"error": {"code": 42}}).api_error_message is None

    def test_error_key_with_non_str_non_dict_returns_none(self):
        # e.g. a list payload under "error" matches neither isinstance arm.
        assert _error({"error": ["boom"]}).api_error_message is None

    def test_top_level_message_key(self):
        err = _error({"message": "Rate limited"})
        assert err.api_error_message == "Rate limited"

    def test_legacy_error_message_key(self):
        err = _error({"errorMessage": "Route not found"})
        assert err.api_error_message == "Route not found"

    def test_error_key_takes_precedence_over_message(self):
        err = _error({"error": "primary", "message": "secondary"})
        assert err.api_error_message == "primary"

    def test_message_takes_precedence_over_error_message(self):
        err = _error({"message": "primary", "errorMessage": "secondary"})
        assert err.api_error_message == "primary"

    def test_unrecognised_keys_return_none(self):
        assert _error({"detail": "something else"}).api_error_message is None


class TestStrFormatting:
    def test_str_includes_api_message_when_extracted(self):
        text = str(_error({"error": "Insufficient liquidity"}))
        assert "API Error (400): API request failed" in text
        assert "API Message: Insufficient liquidity" in text
        assert "Endpoint: /api/v1/shortcuts/route" in text
        assert "Error Type: VALIDATION_ERROR" in text
        # Details line is suppressed when an API message was extracted.
        assert "Details:" not in text

    def test_str_falls_back_to_details_when_no_message_extracted(self):
        text = str(_error({"detail": "raw payload"}))
        assert "Details: {'detail': 'raw payload'}" in text
        assert "API Message:" not in text
