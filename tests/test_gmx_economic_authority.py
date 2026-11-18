"""Range authorization admits the ReferralStorage that perpetual fee reads consult."""

from almanak.backtest.evm import EvmHistory
from almanak.backtest.gateway import GatewayConfig
from almanak.connectors.gmx_v2 import economic_authority
from almanak.connectors.gmx_v2.economic_authority import authorize_ranges
from tests import gmx_perpetual_support as support
from tests import gmx_synthetic_chain as chain


def offline(monkeypatch, history, boundaries):
    class Gateway:
        endpoint = "http://unused.invalid"

        def __init__(self, directory, env_file):
            assert env_file is None

        def __enter__(self):
            return self

        def __exit__(self, *exception):
            return None

    monkeypatch.setattr(economic_authority, "GatewayProcess", Gateway)
    monkeypatch.setattr(
        economic_authority,
        "open_histories",
        lambda stack, directory, settings, endpoint: dict(history),
    )
    monkeypatch.setattr(
        economic_authority, "resolve_boundaries", lambda histories, config, settings: boundaries
    )
    monkeypatch.setattr(economic_authority, "provider_scopes", lambda *args: ())


def written(directory):
    return GatewayConfig.model_validate_json((directory / "history.json").read_text())


def test_authorize_ranges_admits_the_discovered_referral_storage_for_positions(
    tmp_path, monkeypatch
):
    reader, target, at = chain.perpetual_chain("arbitrum")
    config = support.parameters("arbitrum")
    settings = support.settings(config)
    history = {target.fund.chain: EvmHistory(reader)}
    offline(monkeypatch, history, {target.fund.chain: (at, at)})
    without = tmp_path / "without"
    without.mkdir()
    assert authorize_ranges(without, config, settings, None) == {target.fund.chain: (at, at)}
    baseline = written(without)
    assert chain.REFERRAL_STORAGE not in baseline.contracts[0].contracts
    chain.register_referral(reader, target, rebate=1, share=1, minimum=0)
    reader.cache.clear()
    with_referral = tmp_path / "with"
    with_referral.mkdir()
    authorize_ranges(with_referral, config, settings, None)
    expanded = written(with_referral)
    assert chain.REFERRAL_STORAGE in expanded.contracts[0].contracts
    assert set(expanded.contracts[0].contracts) == set(baseline.contracts[0].contracts) | {
        chain.REFERRAL_STORAGE
    }
    assert expanded.contracts[0].callers == baseline.contracts[0].callers
    assert expanded.contracts[0].logs == baseline.contracts[0].logs
