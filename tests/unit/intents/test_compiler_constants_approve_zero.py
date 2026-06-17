"""Tests for approve-zero token constant builders."""

from almanak.framework.intents import compiler_constants as cc


def test_build_approve_zero_first_tokens_uses_token_metadata_and_skips_misses(monkeypatch) -> None:
    class FakeToken:
        symbol = "USDC"
        approve_zero_first_chains = ("arbitrum", "missing")

        def get_address(self, chain: str) -> str | None:
            if chain == "arbitrum":
                return "0xAABB000000000000000000000000000000000000"
            return None

    class NoopToken:
        symbol = "WETH"
        approve_zero_first_chains: tuple[str, ...] = ()

        def get_address(self, chain: str) -> str | None:
            raise AssertionError(f"unexpected address lookup for {chain}")

    monkeypatch.setattr(cc, "DEFAULT_TOKENS", [FakeToken(), NoopToken()])

    tokens = cc._build_approve_zero_first_tokens()

    assert tokens == {"0xaabb000000000000000000000000000000000000"}
