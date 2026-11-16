"""Dedicated historical HTTP gateway process, with no signing or submission API."""

import argparse
import json
import signal
from pathlib import Path

from pydantic import Field, RootModel

from almanak.backtest.config import HistoryBinding
from almanak.backtest.history import Capture, LogScope, ReadRequest
from almanak.backtest.history_authority import CallerScope, TransactionSourceScope
from almanak.backtest.history_batch import BatchedHistoricalGateway, ReadBatch
from almanak.backtest.history_logs import LogScope as RangeLogScope
from almanak.backtest.hypercore_history import HistoryScope, HyperCoreCollector
from almanak.backtest.pool_prices import SwapWindow
from almanak.backtest.pool_reads import PoolGateway, PoolRequest, PricePoolScope
from almanak.backtest.pricing import VendorBinding
from almanak.backtest.vendor import VendorCollector, VendorRequest
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.hypercore_info import InfoRequest
from almanak.gateway.providers import provider_endpoint
from almanak.gateway.service import http_server
from almanak.models import Address, Chain, Model


class ContractBinding(Model):
    chain: Chain
    contracts: tuple[Address, ...]
    logs: tuple[LogScope | RangeLogScope, ...] = ()
    callers: tuple[CallerScope, ...] = ()
    transaction_sources: tuple[TransactionSourceScope, ...] = ()


class GatewayConfig(Model):
    providers: tuple[HistoryBinding, ...]
    contracts: tuple[ContractBinding, ...]
    vendors: tuple[VendorBinding, ...] = ()
    vendor_requests: tuple[VendorRequest, ...] = ()
    # Pools the strategy declared as price series sources; omitted when there are none.
    price_pools: tuple[PricePoolScope, ...] = Field(default=(), exclude_if=lambda value: not value)
    # HyperCore `/info` history a HyperCore preparation declared (D4); omitted when none.
    hypercore: tuple[HistoryScope, ...] = Field(default=(), exclude_if=lambda value: not value)


class HistoricalRequest(
    RootModel[ReadRequest | ReadBatch | VendorRequest | PoolRequest | SwapWindow | InfoRequest]
):
    pass


class AcquisitionGateway(BatchedHistoricalGateway):
    def __init__(self, *args, vendor_config, env_file, **kwargs):
        super().__init__(*args, **kwargs)
        self.vendor = VendorCollector(
            self.capture, vendor_config.vendor_requests, vendor_config.vendors, env_file
        )
        self.pools = PoolGateway(self, vendor_config.price_pools)
        self.hypercore = HyperCoreCollector(self.capture, vendor_config.hypercore)

    def dispatch(self, method, request):
        request = request.root if isinstance(request, HistoricalRequest) else request
        if isinstance(request, PoolRequest | SwapWindow):
            if (method, type(request)) not in (
                ("price_pool", PoolRequest),
                ("swap_read", SwapWindow),
            ):
                raise ValueError("unsupported price pool operation")
            with self.request_locks[int(request.key, 16) % len(self.request_locks)]:
                if isinstance(request, PoolRequest):
                    return self.pools.pool(request)
                return self.pools.swaps(request)
        if isinstance(request, InfoRequest):
            if method != "hypercore_read":
                raise ValueError("unsupported HyperCore history operation")
            return self.hypercore.read(request)
        if isinstance(request, VendorRequest):
            if method != "vendor_read":
                raise ValueError("unsupported vendor operation")
            with self.request_locks[int(request.key, 16) % len(self.request_locks)]:
                return self.vendor.read(request)
        return super().dispatch(method, request)

    def close(self):
        self.vendor.close()
        self.hypercore.close()
        super().close()


def archive_readers(config, env_file):
    if env_file is None:
        return {}
    return {
        binding.chain: EthereumReader(binding.chain, provider_endpoint(binding.provider, env_file))
        for binding in config.providers
    }


def backend_for(directory, config, env_file):
    return AcquisitionGateway(
        Capture(directory / "observations.sqlite"),
        archive_readers(config, env_file),
        {binding.chain: binding.contracts for binding in config.contracts},
        {binding.chain: binding.logs for binding in config.contracts},
        caller_scopes={binding.chain: binding.callers for binding in config.contracts},
        transaction_scopes={
            binding.chain: binding.transaction_sources for binding in config.contracts
        },
        limits={b.chain: b.collection for b in config.providers},
        provider_keys={
            b.chain: b.provider.model_dump_json(exclude={"network"}) for b in config.providers
        },
        vendor_config=config,
        env_file=env_file,
    )


def serve(directory: Path, env_file: Path | None):
    config = GatewayConfig.model_validate_json((directory / "history.json").read_text())
    backend = backend_for(directory, config, env_file)
    server = http_server(
        backend,
        directory / "gateway-auth.token",
        0,
        request_type=HistoricalRequest,
        fields={"read", "read_many", "vendor_read", "price_pool", "swap_read", "hypercore_read"},
    )
    signal.signal(signal.SIGTERM, terminate_gateway)
    print(json.dumps({"event": "historical_gateway_ready", "port": server.server_port}), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        backend.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    serve(args.run, args.env_file)


def terminate_gateway(*_):
    raise SystemExit(0)


if __name__ == "__main__":
    main()
