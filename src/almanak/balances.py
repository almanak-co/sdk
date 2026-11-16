"""Native and ERC-20 balance observations at a pinned block.

Native chain assets use the CAIP-19 SLIP-44 asset type (for example
`eip155:1/slip44:60`), matching `almanak.gateway.evm.native_asset`; ERC-20
assets use the contract-address asset type (`eip155:1/erc20:0x...`). Both are
fungible CAIP-19 asset types (`AssetType`): a CAIP-19 asset id identifies one
non-fungible token and cannot name a balance denomination, so it is rejected.
"""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AccountReference, AssetType, ChainId
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.evm import erc20_address, native_asset
from almanak.models import Account, Block, Chain, Model, address

Decimal = Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]*)$")]
Decimals = Annotated[int, Field(strict=True, ge=0, le=255)]


class BalanceObservation(Model):
    """One account's raw balance of one asset at one pinned block.

    `amount` is the exact base-unit integer as a canonical decimal string; a
    string keeps uint256 quantities exact through JSON. `decimals` is
    presentation metadata only: 18 for the native asset (the chain-defined
    convention in `almanak.gateway.evm.native_metadata`), the on-chain
    `decimals()` result for ERC-20, or None when the contract has no
    conforming `decimals()`. Missing decimals never imply a value.
    """

    asset: AssetType
    account: Account
    block: Block
    amount: Decimal
    denomination: Literal["native", "erc20"]
    decimals: Decimals | None = None

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.account.chain != self.block.chain or self.asset.chain != self.block.chain.chain_id:
            raise ValueError("balance observation has mixed chain/environment scope")
        expected = "native" if self.asset.namespace == "slip44" else "erc20"
        if self.asset.namespace not in ("slip44", "erc20") or self.denomination != expected:
            raise ValueError("balance denomination does not match the asset namespace")
        if self.denomination == "native" and self.decimals != 18:
            raise ValueError("native balances use the chain-defined 18 decimals")
        return self


def read_balance(
    reader: EthereumReader, asset: AssetType, account: Account, block: int | str
) -> BalanceObservation:
    if not isinstance(asset, AssetType):
        raise ValueError("balance reads require a CAIP-19 asset type")
    if asset.chain != reader.chain.chain_id:
        raise ValueError("asset chain differs from the reader chain")
    if account.chain != reader.chain:
        raise ValueError("account chain/environment differs from the reader chain")
    if asset.namespace == "slip44":
        if asset != native_asset(reader.chain.chain_id):
            raise ValueError("SLIP-44 asset is not the reader chain's native asset")
    elif asset.namespace == "erc20":
        erc20_address(asset, reader.chain.chain_id)
    else:
        raise ValueError("balance reads support only slip44 (native) and erc20 assets")
    pinned = reader.pin_block(block)
    if asset.namespace == "slip44":
        return _native_balance(reader, asset, account, pinned)
    return _erc20_balance(reader, asset, account, pinned)


def parse_balance_request(
    asset: str, account: str, chain_hint: ChainId | None
) -> tuple[AssetType, Account]:
    """Validate raw CLI strings into a balance request.

    The account chain is the hint when given, otherwise the asset's chain; the
    two must agree. The environment label is "mainnet", matching the resolved
    reader chain for live endpoints; `read_balance` re-checks the account
    against the reader's exact chain/environment, so other environments build
    `Account` directly instead of using this helper.
    """
    try:
        asset_type = AssetType(asset)
    except ValueError:
        raise ValueError(f"invalid CAIP-19 asset type: {asset!r}") from None
    if asset_type.namespace not in ("slip44", "erc20"):
        raise ValueError("balance assets must be slip44 (native) or erc20")
    chain = chain_hint if chain_hint is not None else asset_type.chain
    if not isinstance(chain, ChainId) or chain.namespace != "eip155":
        raise ValueError("balance reads require an EIP-155 chain")
    if chain != asset_type.chain:
        raise ValueError("account chain hint differs from the asset chain")
    try:
        validated = address(account)
    except ValueError:
        raise ValueError(f"invalid EVM account address: {account!r}") from None
    return asset_type, Account(
        chain=Chain(chain_id=chain, environment="mainnet"),
        address=AccountReference(validated),
    )


def _native_balance(
    reader: EthereumReader, asset: AssetType, account: Account, block: Block
) -> BalanceObservation:
    raw = reader._rpc(
        "eth_getBalance",
        [account.address, {"blockHash": block.hash, "requireCanonical": True}],
    )
    try:
        amount = int(raw, 16)
    except (TypeError, ValueError):
        raise DataError("malformed", "eth_getBalance: invalid balance quantity") from None
    return BalanceObservation(
        asset=asset,
        account=account,
        block=block,
        amount=str(amount),
        denomination="native",
        decimals=18,
    )


def _erc20_balance(
    reader: EthereumReader, asset: AssetType, account: Account, block: Block
) -> BalanceObservation:
    contract = erc20_address(asset, reader.chain.chain_id)
    (amount,) = reader.call(
        contract, "balanceOf(address)", block, inputs=("address",), args=(account.address,)
    )
    return BalanceObservation(
        asset=asset,
        account=account,
        block=block,
        amount=str(amount),
        denomination="erc20",
        decimals=_optional_decimals(reader, contract, block),
    )


def _optional_decimals(reader: EthereumReader, contract: str, block: Block) -> int | None:
    try:
        (decimals,) = reader.call(contract, "decimals()", block, outputs=("uint8",))
    except DataError as exc:
        # A non-conforming contract reverts or returns non-ABI data; provider,
        # transport, and envelope failures remain errors and must not read as None.
        if exc.kind in ("rpc", "abi"):
            return None
        raise
    return decimals
