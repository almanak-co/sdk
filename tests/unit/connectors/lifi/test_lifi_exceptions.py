"""Unit tests for LiFi Exceptions."""

from almanak.framework.connectors.lifi.exceptions import (
    LiFiAPIError,
    LiFiConfigError,
    LiFiError,
    LiFiRouteNotFoundError,
    LiFiTransferFailedError,
    LiFiValidationError,
)


class TestLiFiAPIError:
    """Test API error classification and formatting."""

    def test_rate_limit_classification(self):
        err = LiFiAPIError("Rate limited", status_code=429, endpoint="/quote")
        assert err.error_type == "RATE_LIMIT"
        assert err.status_code == 429

    def test_server_error_classification(self):
        err = LiFiAPIError("Internal error", status_code=500)
        assert err.error_type == "SERVER_ERROR"

    def test_validation_error_classification(self):
        err = LiFiAPIError("Bad request", status_code=400)
        assert err.error_type == "VALIDATION_ERROR"

    def test_auth_error_classification(self):
        err = LiFiAPIError("Unauthorized", status_code=401)
        assert err.error_type == "AUTHENTICATION_ERROR"

    def test_not_found_classification(self):
        err = LiFiAPIError("Not found", status_code=404)
        assert err.error_type == "NOT_FOUND"

    def test_generic_client_error(self):
        err = LiFiAPIError("Forbidden", status_code=403)
        assert err.error_type == "CLIENT_ERROR"

    def test_unknown_error(self):
        err = LiFiAPIError("Connection error", status_code=0)
        assert err.error_type == "UNKNOWN_ERROR"

    def test_str_includes_all_info(self):
        err = LiFiAPIError(
            "Rate limited",
            status_code=429,
            endpoint="/quote",
            error_data={"message": "Too many requests"},
        )
        msg = str(err)
        assert "429" in msg
        assert "Rate limited" in msg
        assert "/quote" in msg
        assert "RATE_LIMIT" in msg
        assert "Too many requests" in msg

    def test_str_minimal(self):
        err = LiFiAPIError("Error", status_code=500)
        msg = str(err)
        assert "500" in msg
        assert "Error" in msg

    def test_inherits_from_lifi_error(self):
        err = LiFiAPIError("test", status_code=500)
        assert isinstance(err, LiFiError)
        assert isinstance(err, Exception)


class TestLiFiConfigError:
    """Test configuration error formatting."""

    def test_with_parameter(self):
        err = LiFiConfigError("Invalid value", parameter="chain_id")
        assert "chain_id" in str(err)
        assert "Invalid value" in str(err)

    def test_without_parameter(self):
        err = LiFiConfigError("Generic config error")
        assert "Generic config error" in str(err)

    def test_inherits_from_lifi_error(self):
        assert isinstance(LiFiConfigError("test"), LiFiError)


class TestLiFiValidationError:
    """Test validation error formatting."""

    def test_with_field_and_value(self):
        err = LiFiValidationError("Invalid amount", field="from_amount", value="-1")
        msg = str(err)
        assert "from_amount" in msg
        assert "-1" in msg

    def test_with_field_only(self):
        err = LiFiValidationError("Required field missing", field="to_chain")
        assert "to_chain" in str(err)

    def test_without_details(self):
        err = LiFiValidationError("Something wrong")
        assert "Something wrong" in str(err)

    def test_inherits_from_lifi_error(self):
        assert isinstance(LiFiValidationError("test"), LiFiError)


class TestLiFiRouteNotFoundError:
    """Test route not found error."""

    def test_message(self):
        err = LiFiRouteNotFoundError("No route: USDC->WETH on chain 42161")
        assert "No route" in str(err)

    def test_inherits_from_lifi_error(self):
        assert isinstance(LiFiRouteNotFoundError("test"), LiFiError)


class TestLiFiTransferFailedError:
    """Test transfer failed error formatting."""

    def test_with_all_details(self):
        err = LiFiTransferFailedError(
            "Bridge failed",
            tx_hash="0xabc123",
            status="FAILED",
            substatus="REFUNDED",
        )
        msg = str(err)
        assert "Bridge failed" in msg
        assert "0xabc123" in msg
        assert "FAILED" in msg
        assert "REFUNDED" in msg

    def test_minimal(self):
        err = LiFiTransferFailedError("Transfer failed")
        assert "Transfer failed" in str(err)

    def test_inherits_from_lifi_error(self):
        assert isinstance(LiFiTransferFailedError("test"), LiFiError)
