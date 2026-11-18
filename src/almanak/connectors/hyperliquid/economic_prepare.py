"""Prepare a HyperCore dataset through the historical gateway's `/info` history (D4).

The shared preparation binding provisions the directory, resolves nothing on HyperEVM
(HyperCore history needs no contract reads) and publishes the immutable dataset; this
binding gives the gateway its HyperCore scopes and reads candles and funding through it.
"""

import time
from contextlib import ExitStack

from almanak.backtest import progress
from almanak.backtest.cli import GatewayProcess, write_json
from almanak.backtest.gateway import GatewayConfig
from almanak.backtest.history import HistoricalReader
from almanak.backtest.hypercore_history import HistoryClient
from almanak.backtest.prepared import DatasetWriter, Preparation
from almanak.connectors.dex_economic_dataset import targets
from almanak.connectors.dex_economic_prepare import PreparationBinding
from almanak.connectors.hyperliquid import economic_collection, economic_data
from almanak.connectors.hyperliquid.economic_book import HyperCoreBook
from almanak.connectors.hyperliquid.economic_parameters import parameters
from almanak.gateway.auth import read_token


def validate_policy(config, settings):
    """Refuse funds that hold no allocation or allocations no fund holds."""
    funds = {fund.name for fund in settings.funds}
    if {allocation.account for allocation in config.allocations} != funds:
        raise ValueError("HyperCore backtests fund every allocation and nothing else")


def gateway_config(config, settings) -> GatewayConfig:
    return GatewayConfig(
        providers=settings.history,
        contracts=(),
        hypercore=economic_collection.scopes(config, settings),
    )


def require_closed(settings, now=None):
    """The venue's candles exist only once closed, so the window must lie in the past."""
    latest = (time.time() if now is None else now) - settings.data.candle_seconds
    if settings.schedule.deadline > latest:
        raise ValueError(
            "a HyperCore backtest must end, cleanup included, before the latest closed candle"
        )


class HyperCorePreparation(PreparationBinding):
    def authorize_ranges(self, directory, config, settings, env_file):
        write_json(directory / "history.json", gateway_config(config, settings))
        return None

    def acquire(self, directory, config, settings, env_file, boundaries):
        """Read every requirement through the gateway and publish the dataset."""
        require_closed(settings)
        requirements = tuple(
            requirement
            for target in targets(config)
            for requirement in economic_data.requirements(target, settings)
        )
        preparation = Preparation(
            model=economic_data.MODEL,
            acquisition_revision=economic_data.REVISION,
            policy_digest=self.book_type.identity(config, settings),
            requirements=requirements,
        )
        with ExitStack() as stack:
            gateway = stack.enter_context(GatewayProcess(directory, env_file))
            token = read_token(directory / "gateway-auth.token")
            writer = DatasetWriter(directory, preparation)
            stack.callback(writer.close)
            with progress.phase(
                "preparation.hypercore", "Reading HyperCore candles and funding", 1, "phases"
            ):
                for target in targets(config):
                    reader = HistoricalReader(target.fund.chain, gateway.endpoint, token)
                    stack.callback(reader.__exit__)
                    economic_collection.collect(writer, HistoryClient(reader), target, settings)
            with progress.phase(
                "preparation.finalization", "Publishing HyperCore prepared inputs", 1, "phases"
            ):
                writer.publish()


binding = HyperCorePreparation(
    data=economic_data,
    collection=economic_collection,
    book_type=HyperCoreBook,
    parameters=parameters,
    validate_policy=validate_policy,
)
validate_sources = binding.validate_sources
dataset_directory = binding.dataset_directory
prepare = binding.prepare
