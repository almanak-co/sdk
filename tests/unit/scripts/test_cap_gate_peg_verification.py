"""Fail-closed controls for the cap gate's stablecoin pricing.

The cap gate is a fund-safety predicate, so a price it did not measure is a
wallet total it did not establish. These controls pin the four ways that
guarantee can be lost: a ticker-keyed peg, a synthetic quote graded as an
observation, an off-peg quote spent against a dollar-denominated cap, and a
cached number whose witness is gone.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from qa_lab import chains as C

ROBINHOOD_USDG = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"


def _response(price: str, source: str) -> dict:
    return {"status": "success", "data": {"token": "USDG", "price_usd": price, "source": source}}


def _cache(tmp_path: Path) -> Path:
    return tmp_path / "prices.json"


def _provenance(cache: Path) -> dict:
    path = cache.with_name("price-provenance.json")
    return json.loads(path.read_text()) if path.exists() else {}


class TestRegistryPeg:
    def test_peg_is_resolved_by_address_not_ticker(self):
        assert C.registry_peg("USDG", "robinhood") == Decimal("1")
        assert C.registry_peg("USDE", "robinhood") == Decimal("1")

    def test_native_and_unpegged_symbols_get_no_peg(self):
        assert C.registry_peg("ETH", "robinhood") is None
        assert C.registry_peg("WETH", "robinhood") is None

    def test_a_ticker_absent_from_the_token_table_gets_no_peg(self):
        # A ticker-keyed peg grants $1 on every chain, including chains where
        # the harness holds no such contract at all.
        assert C.registry_peg("DAI", "robinhood") is None
        assert C.registry_peg("USDBC", "arbitrum") is None
        assert C.registry_peg("USDC", "robinhood") is None

    def test_a_ticker_that_names_a_different_contract_is_refused(self, monkeypatch):
        # Balances would be read at USDe's contract while the quote is requested
        # for the ticker USDG: both are pegged, so only an identity check catches it.
        crossed = dict(C.TOKENS["robinhood"])
        crossed["USDG"] = C.TOKENS["robinhood"]["USDE"]
        monkeypatch.setitem(C.TOKENS, "robinhood", crossed)
        with pytest.raises(SystemExit, match="another asset's quote"):
            C.registry_peg("USDG", "robinhood")


class TestPegVerification:
    def test_a_synthetic_peg_is_graded_an_assumption_and_refused(self, tmp_path):
        cache = _cache(tmp_path)
        with pytest.raises(SystemExit, match="not a market observation"):
            C._verify_peg(
                Decimal("1"), Decimal("1"), "USDG", "robinhood", _response("1.0", "stablecoin_peg"), cache, strict=True
            )
        row = _provenance(cache)["USDG"]
        assert row["status"] == "ASSUMPTION"
        assert row["gate_decision"] == "REFUSED_UNVERIFIED_PEG"

    def test_the_all_sources_failed_peg_is_refused_too(self, tmp_path):
        with pytest.raises(SystemExit):
            C._verify_peg(
                Decimal("1"),
                Decimal("1"),
                "USDG",
                "robinhood",
                _response("1.0", "stablecoin_fallback"),
                _cache(tmp_path),
                strict=True,
            )

    def test_an_unnamed_source_is_refused(self, tmp_path):
        with pytest.raises(SystemExit):
            C._verify_peg(
                Decimal("1"), Decimal("1"), "USDG", "robinhood", _response("1.0", ""), _cache(tmp_path), strict=True
            )

    def test_an_in_band_measurement_is_accepted_and_kept_as_measured(self, tmp_path):
        cache = _cache(tmp_path)
        witness = C._verify_peg(
            Decimal("1.0005"), Decimal("1"), "USDG", "robinhood", _response("1.0005", "aggregated"), cache, strict=True
        )
        assert witness["status"] == "OBSERVED_API_RESPONSE"
        assert witness["gate_decision"] == "ACCEPTED"
        # Rounding an in-band print back to the peg is the under-valuation the
        # measurement exists to rule out.
        assert witness["price_usd"] == "1.0005"
        assert Decimal(witness["peg_verification"]["deviation_bps"]) == Decimal("5")

    def test_an_out_of_band_measurement_refuses_the_gate(self, tmp_path):
        cache = _cache(tmp_path)
        with pytest.raises(SystemExit, match="Refusing the cap gate"):
            C._verify_peg(
                Decimal("0.93"), Decimal("1"), "USDG", "robinhood", _response("0.93", "aggregated"), cache, strict=True
            )
        row = _provenance(cache)["USDG"]
        assert row["status"] == "OBSERVED_API_RESPONSE"
        assert row["gate_decision"] == "REJECTED_PEG_DEVIATION"

    def test_a_risk_reducing_path_records_instead_of_refusing(self, tmp_path):
        cache = _cache(tmp_path)
        witness = C._verify_peg(
            Decimal("0.93"), Decimal("1"), "USDG", "robinhood", _response("0.93", "aggregated"), cache, strict=False
        )
        assert witness["gate_decision"] == "REJECTED_PEG_DEVIATION"
        synthetic = C._verify_peg(
            Decimal("1"), Decimal("1"), "USDG", "robinhood", _response("1.0", "stablecoin_peg"), cache, strict=False
        )
        assert synthetic["status"] == "ASSUMPTION"


class TestAxPrice:
    def test_the_quote_subprocess_demands_gateway_side_verification(self, tmp_path, monkeypatch):
        captured: dict = {}

        class _Completed:
            stdout = json.dumps(_response("1.0005", "aggregated"))

        def _fake_run(argv, **kwargs):
            captured["argv"] = argv
            captured["env"] = kwargs.get("env") or {}
            return _Completed()

        monkeypatch.setattr(C.subprocess, "run", _fake_run)
        cache = _cache(tmp_path)
        assert C.ax_price("USDG", "robinhood", cache) == Decimal("1.0005")
        assert captured["env"]["ALMANAK_GATEWAY_STABLECOIN_VERIFY"] == "true"
        assert _provenance(cache)["USDG"]["gate_decision"] == "ACCEPTED"

    def test_a_cached_number_without_a_witness_is_re_measured(self, tmp_path, monkeypatch):
        cache = _cache(tmp_path)
        cache.write_text(json.dumps({"USDG": "1"}))
        calls: list[list[str]] = []

        class _Completed:
            stdout = json.dumps(_response("1.0005", "aggregated"))

        def _fake_run(argv, **kwargs):
            calls.append(argv)
            return _Completed()

        monkeypatch.setattr(C.subprocess, "run", _fake_run)
        assert C.ax_price("USDG", "robinhood", cache) == Decimal("1.0005")
        assert calls, "a number with no provenance row must be a cache miss, not a quote"

    def test_a_witness_a_risk_reducing_pass_recorded_over_a_refusal_is_not_reused(self, tmp_path, monkeypatch):
        """The sweep and the cap gate share one cache but not one standard.

        ``strict_peg=False`` exists so a recovery path can value a wallet it is
        rescuing rather than strand it. That pass writes its number and witness
        into the same batch cache the gate reads, so without a grade check the
        gate inherits a price the sweep only tolerated.
        """
        cache = _cache(tmp_path)
        cache.write_text(json.dumps({"USDG": "1"}))
        cache.with_name("price-provenance.json").write_text(
            json.dumps({"USDG": {"status": "ASSUMPTION", "gate_decision": "REFUSED_UNVERIFIED_PEG"}})
        )
        calls: list[list[str]] = []

        class _Completed:
            stdout = json.dumps(_response("1.0005", "aggregated"))

        def _fake_run(argv, **kwargs):
            calls.append(argv)
            return _Completed()

        monkeypatch.setattr(C.subprocess, "run", _fake_run)
        assert C.ax_price("USDG", "robinhood", cache) == Decimal("1.0005")
        assert calls, "a refused witness must not satisfy a cap gate from the cache"
        # The same row still serves the path that recorded it.
        assert C.ax_price("USDG", "robinhood", cache, strict_peg=False) == Decimal("1.0005")

    def test_a_grade_the_predicate_has_never_been_taught_is_not_admitted(self):
        """Admissibility is allowlisted so a new grade cannot inherit it.

        This predicate is the only thing standing between a degraded number and
        a wallet valuation. Denylisting the grades known to be bad admits by
        omission: whoever adds the next status has to remember this function
        exists, and the failure if they do not is silent and on a money path.
        """
        assert C._witness_admissible({"status": "OBSERVED_API_RESPONSE"}, strict=True)
        for degraded in ({}, {"gate_decision": "ACCEPTED"}, {"status": "A_GRADE_ADDED_LATER"}):
            assert not C._witness_admissible(degraded, strict=True)
            # The recovery path values a wallet it is rescuing; it never gates.
            assert C._witness_admissible(degraded, strict=False)

    def test_a_cached_number_with_a_witness_is_reused(self, tmp_path, monkeypatch):
        cache = _cache(tmp_path)
        cache.write_text(json.dumps({"USDG": "1.0004"}))
        cache.with_name("price-provenance.json").write_text(json.dumps({"USDG": {"status": "OBSERVED_API_RESPONSE"}}))

        def _fail(*args, **kwargs):
            raise AssertionError("a witnessed cache entry must not be re-measured")

        monkeypatch.setattr(C.subprocess, "run", _fail)
        assert C.ax_price("USDG", "robinhood", cache) == Decimal("1.0004")
