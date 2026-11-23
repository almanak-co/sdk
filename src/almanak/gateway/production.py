"""Mainnet transport identity and encrypted signers for shared EVM execution."""

from pathlib import Path
from urllib.parse import urlsplit

from almanak.chains import ETHEREUM, evm_network
from almanak.gateway.credentials import load_production_signers
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import EvmGateway
from almanak.gateway.request_scope import once

ETHEREUM_GENESIS = ETHEREUM.genesis


class ProductionGateway(EvmGateway):
    def __init__(
        self,
        config,
        endpoint: str,
        key_file: Path | None = None,
        password_file: Path | None = None,
        *,
        chain=None,
        password=None,
        signer_file: Path | None = None,
    ):
        if config.production is None:
            raise ValueError("production gateway requires explicit production configuration")
        validate_signer_files(key_file, signer_file)
        validate_endpoint(endpoint)
        super().__init__(config, endpoint, {}, chain=chain)
        try:
            self.signers = production_signers(
                config, key_file, password_file, password, signer_file
            )
        except BaseException:
            self.close()
            raise

    @property
    def venue_transports(self) -> dict[str, object]:
        # Production asks each venue what its connector declares, and takes no other.
        declared = getattr(self.connector.execution, "venue_transports", None)
        return dict(declared(self.config)) if declared else {}

    def guard(self):
        # Verify in every privileged request; a provider switch must not reuse an earlier
        # request's result. Checks within one request share its single verification.
        once(("production_guard", self.reader.chain), self.verify_provider)

    def verify_provider(self):
        network = evm_network(self.reader.chain.chain_id)
        if int(self.reader._rpc("eth_chainId", []), 16) != int(network.chain.reference):
            raise DataError("chain_mismatch", "production provider has a different chain")
        client = self.reader._rpc("web3_clientVersion", [])
        if not isinstance(client, str) or any(
            name in client.lower() for name in ("anvil", "hardhat", "ganache")
        ):
            raise DataError("environment_mismatch", "production provider identifies a test node")
        # Callers such as plan preparation pin their observation block before this
        # guard runs and read at it afterwards. The genesis pin must not replace it.
        with self.reader.retained_pin():
            if self.reader.pin_block(0).hash != network.genesis:
                raise DataError("chain_mismatch", "production provider has a different genesis")
        return True


def validate_endpoint(endpoint):
    parsed = urlsplit(endpoint)
    if parsed.scheme != "https" or not parsed.hostname or parsed.fragment or parsed.username:
        raise ValueError("production execution requires an explicit HTTPS RPC endpoint")


def validate_signer_files(key_file: Path | None, signer_file: Path | None):
    if key_file is not None and signer_file is not None:
        raise ValueError("production gateway takes either keystores or a remote signer")


def production_signers(config, key_file, password_file, password, signer_file):
    if key_file is not None:
        return load_production_signers(config, key_file, password_file, password=password)
    if signer_file is not None:
        from almanak.gateway.signers import load_remote_signers

        return load_remote_signers(config, signer_file)
    return {}
