"""Infrastructure failures shared by simulation and teardown retry policy."""

from almanak.framework.intents.error_keywords import categorize_error

TRANSIENT_RPC_KEYWORDS = (
    "fork error",
    "transport",
    "dns error",
    "failed to lookup address",
    "host unreachable",
    "connection reset",
    "broken pipe",
    "eof",
)


def simulation_failure_kind(error: Exception | str) -> str:
    message = str(error).lower()
    if isinstance(error, TimeoutError | ConnectionError) or any(word in message for word in TRANSIENT_RPC_KEYWORDS):
        return "transient"
    if categorize_error(message) in {"TIMEOUT", "NETWORK_ERROR", "RATE_LIMIT"}:
        return "transient"
    return "unavailable"
