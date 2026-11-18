"""The composed strategy's declared series, acquired once beside its native datasets.

Components prepare and replay their own markets; the strategy decides once for the
whole composition, so its series form one prepared dataset bound by the descriptor.
"""

import secrets
from contextlib import ExitStack

from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.dataset import file_digest
from almanak.backtest.evm import EvmHistory
from almanak.backtest.history import HistoricalReader
from almanak.backtest.prepared import DatasetWriter, Preparation, load_dataset
from almanak.backtest.strategy_prices import (
    StrategySeries,
    require_history_chains,
    strategy_bindings,
    strategy_requirements,
)
from almanak.gateway.auth import read_token

DIRECTORY = "prices"
REVISION = "composed-strategy-prices-v1"


def preparation(policy_digest, settings):
    from almanak.connectors.composed.economic_book import MODEL

    return Preparation(
        model=MODEL,
        acquisition_revision=REVISION,
        policy_digest=policy_digest,
        requirements=strategy_requirements(settings),
    )


def validate_chains(settings):
    require_history_chains(settings)


def prepare_prices(directory, policy_digest, settings, env_file):
    """The digest of the prepared series dataset, or None when the strategy declares none."""
    from almanak.connectors.dex_economic_prepare import collect_strategy_prices, gateway_config

    if not strategy_bindings(settings):
        return None
    target = directory / DIRECTORY
    if not (target / "dataset.json").exists():
        target.mkdir(exist_ok=True)
        path = target / "gateway-auth.token"
        if not path.exists():
            with path.open("x") as output:
                path.chmod(0o600)
                output.write(secrets.token_hex(32))
        write_json(target / "history.json", gateway_config(settings, ()))
        with ExitStack() as stack:
            gateway = stack.enter_context(GatewayProcess(target, env_file))
            token = read_token(target / "gateway-auth.token")
            histories = {}
            for binding in settings.history:
                reader = HistoricalReader(binding.chain, gateway.endpoint, token)
                stack.callback(reader.__exit__)
                histories[binding.chain] = EvmHistory(reader)
            writer = DatasetWriter(target, preparation(policy_digest, settings))
            stack.callback(writer.close)
            collect_strategy_prices(writer, histories, settings)
            writer.publish()
    return file_digest(target / "dataset.json")


class Prices:
    """The declared series of one composed dataset, sliced per decision."""

    def __init__(self, directory, policy_digest, settings, digest):
        self.reader = None
        self.series = None
        if (digest is None) != (not strategy_bindings(settings)):
            raise ValueError("composed dataset differs from the strategy's declared series")
        if digest is None:
            return
        manifest, self.reader = load_dataset(directory / DIRECTORY, digest)
        try:
            if manifest.preparation != preparation(policy_digest, settings):
                raise ValueError("prepared strategy series differ from the declared series")
            self.series = StrategySeries(self.reader, manifest, settings)
        except BaseException:
            self.close()
            raise

    def at(self, time):
        return () if self.series is None else self.series.at(time)

    def close(self):
        if self.reader is not None:
            self.reader.close()
            self.reader = None
