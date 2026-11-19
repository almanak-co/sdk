"""Static connector support, independent of credentials and current market state."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.connectors.identifiers import ProtocolVariant
from almanak.models import Model


class RunMode(StrEnum):
    FORK = "fork"
    PRODUCTION = "production"
    BACKTEST = "backtest"


class EconomicAction(StrEnum):
    INCREASE_POSITION = "increase_position"
    DECREASE_POSITION = "decrease_position"
    CLOSE_POSITION = "close_position"
    REVOKE_POSITION = "revoke_position"
    SUPPLY = "supply"
    WITHDRAW = "withdraw"
    BORROW = "borrow"
    REPAY = "repay"
    WITHDRAW_AMOUNT = "withdraw_amount"
    REPAY_AMOUNT = "repay_amount"
    COLLATERAL = "collateral"
    REFRESH = "aave_refresh"
    SWAP = "swap_exact_input"
    OPEN_LIQUIDITY = "open_liquidity"
    INCREASE_LIQUIDITY = "increase_liquidity"
    DECREASE_LIQUIDITY = "decrease_liquidity"
    COLLECT = "collect_liquidity_fees"
    CLOSE_LIQUIDITY = "close_liquidity"
    REVOKE = "revoke_exchange"
    EXIT_EXCHANGE = "exit_exchange"
    # HyperCore custody and leverage (blueprint 30, section 12).
    DEPOSIT = "deposit"
    SET_LEVERAGE = "set_leverage"


# OP-family production declarations rely on the gateway's quote-time L1/operator
# exposure bound and exact pinned receipt charges (`gateway/op_fees.py`).
OP_FEE_ACCOUNTING = "pinned Jovian L1 and operator fee accounting"
# Each of these connectors' Parameters refuses a second allocation on one fund; the
# author check enforces the same rule offline and their support lists it.
ONE_ALLOCATION_PER_FUND = "one allocation per fund"
ONE_ALLOCATION_PER_FUND_CONNECTORS = frozenset(
    {
        "uniswap_v3",
        "uniswap_v4",
        "pancakeswap_v3",
        "aerodrome_slipstream",
        "aerodrome",
        "curve",
        "gmx_v2",
        "hyperliquid",
    }
)
OP_FAMILY_CHAINS = (ChainId("eip155:8453"), ChainId("eip155:10"))


def op_fee_restrictions(chain: ChainId) -> tuple[str, ...]:
    return (OP_FEE_ACCOUNTING,) if chain in OP_FAMILY_CHAINS else ()


class HistoricalSupport(Model):
    model: str
    mode: Literal["historical_path", "economic"]
    actions: tuple[EconomicAction, ...]
    backtest_start_at: datetime | None = None
    variants: tuple[ProtocolVariant, ...] = ()

    @model_validator(mode="after")
    def time(self):
        if self.backtest_start_at is not None and self.backtest_start_at.utcoffset() is None:
            raise ValueError("historical start requires an explicit timezone")
        return self


class ConnectorChainSupport(Model):
    chain: ChainId
    modes: tuple[RunMode, ...]
    actions: tuple[EconomicAction, ...]
    account_profiles: tuple[Literal["eoa", "safe_roles"], ...]
    restrictions: tuple[str, ...]
    historical: tuple[HistoricalSupport, ...] = ()
    variants: tuple[ProtocolVariant, ...] = ()
    # Actions `almanak ax` runs as direct operator commands on this chain: production,
    # for an EOA wallet, without a strategy. The gateway admits and plans an operator
    # request only for an action listed here.
    direct_actions: tuple[EconomicAction, ...] = Field(default=(), exclude_if=lambda v: not v)

    @model_validator(mode="after")
    def consistent(self):
        if (RunMode.BACKTEST in self.modes) != bool(self.historical):
            raise ValueError("backtest support requires its explicit historical model")
        if len({h.model for h in self.historical}) != len(self.historical):
            raise ValueError("historical model revisions must be unique within a chain")
        if len(set(self.modes)) != len(self.modes) or len(set(self.actions)) != len(self.actions):
            raise ValueError("connector support entries must be unique")
        self.validate_variants()
        self.validate_direct()
        return self

    def validate_direct(self):
        if not self.direct_actions:
            return
        if len(set(self.direct_actions)) != len(self.direct_actions):
            raise ValueError("direct actions must be unique")
        if set(self.direct_actions) - set(self.actions):
            raise ValueError("a direct action must be a supported action")
        if RunMode.PRODUCTION not in self.modes or "eoa" not in self.account_profiles:
            raise ValueError("direct actions require production support for EOA accounts")

    def validate_variants(self):
        if len(set(self.variants)) != len(self.variants):
            raise ValueError("protocol variants must be unique")
        for historical in self.historical:
            if len(set(historical.variants)) != len(historical.variants):
                raise ValueError("historical protocol variants must be unique")
            if bool(self.variants) != bool(historical.variants):
                raise ValueError("historical models must declare their protocol variants")
            if set(historical.variants) - set(self.variants):
                raise ValueError("historical model declares an unsupported protocol variant")

    def require_model(self, model):
        for historical in self.historical:
            if historical.model == model:
                return historical
        raise ValueError(f"historical model {model!r} is unsupported on {self.chain}")

    def require_variant(self, variant, *, model=None):
        variants = self.require_model(model).variants if model is not None else self.variants
        if variant not in (variants or (None,)):
            raise ValueError(f"protocol variant {variant!r} is unsupported on {self.chain}")

    def require(self, mode: RunMode, action=None, account_profile=None, *, model=None):
        if mode not in self.modes:
            raise ValueError(f"{mode.value} is unsupported on {self.chain}")
        actions = self.require_model(model).actions if mode == RunMode.BACKTEST else self.actions
        if action is not None and action not in actions:
            raise ValueError(f"{action} is unsupported for {mode.value} on {self.chain}")
        if account_profile is not None and account_profile not in self.account_profiles:
            raise ValueError(f"account profile is unsupported on {self.chain}")

    def require_direct(self, action):
        """Refuse an operator request for an action this chain does not serve directly."""
        if action not in self.direct_actions:
            raise ValueError(f"{action} is unsupported as a direct action on {self.chain}")


def declarations(
    actions: tuple[EconomicAction, ...],
    restrictions: tuple[str, ...],
    historical: dict[ChainId, tuple[HistoricalSupport, ...]],
    *,
    chains: tuple[ChainId, ...],
    direct: tuple[EconomicAction, ...] = (),
) -> tuple[ConnectorChainSupport, ...]:
    if any(not isinstance(chain, ChainId) for chain in chains):
        raise ValueError("connector declarations require validated chain identities")
    if len(set(chains)) != len(chains):
        raise ValueError("connector chains must be unique")
    if set(historical) - set(chains):
        raise ValueError("historical support requires a declared execution chain")
    return tuple(
        ConnectorChainSupport(
            chain=chain,
            modes=(RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
            if historical.get(chain)
            else (RunMode.FORK, RunMode.PRODUCTION),
            actions=actions,
            account_profiles=("eoa", "safe_roles"),
            restrictions=restrictions,
            historical=historical.get(chain, ()),
            direct_actions=direct,
        )
        for chain in chains
    )


def aave_support():
    actions = (
        EconomicAction.SUPPLY,
        EconomicAction.WITHDRAW,
        EconomicAction.BORROW,
        EconomicAction.REPAY,
    )
    economic = (
        *actions,
        EconomicAction.WITHDRAW_AMOUNT,
        EconomicAction.REPAY_AMOUNT,
        EconomicAction.COLLATERAL,
        EconomicAction.REFRESH,
    )
    detailed = HistoricalSupport(
        model="aave-v4-supply-historical-shares-v1",
        mode="historical_path",
        actions=(EconomicAction.SUPPLY, EconomicAction.WITHDRAW),
        backtest_start_at=datetime(2026, 3, 30, tzinfo=UTC),
    )
    histories: dict[ChainId, tuple[HistoricalSupport, ...]] = {
        ChainId(chain): (
            HistoricalSupport(
                model="aave-v4-economic-rates-v1",
                mode="economic",
                actions=economic,
                backtest_start_at=start,
            ),
        )
        for chain, start in (
            ("eip155:1", datetime(2026, 3, 30, tzinfo=UTC)),
            ("eip155:43114", datetime(2026, 9, 5, tzinfo=UTC)),
        )
    }
    histories[ChainId("eip155:1")] = (detailed, *histories[ChainId("eip155:1")])
    return declarations(
        actions,
        ("verified V4 implementation", "local fund/Spoke collateral", "standard ERC-20 assets"),
        histories,
        chains=(ChainId("eip155:1"), ChainId("eip155:43114")),
        direct=(EconomicAction.SUPPLY, EconomicAction.WITHDRAW),
    )


NATIVE_V4_CURRENCIES = (
    "two standard ERC-20 currencies, or the native coin held as its wrapped token"
    " for trading and liquidity"
)
NATIVE_V4_LIQUIDITY = (
    "native-coin liquidity in one token pair per chain, never beside ERC-20 liquidity"
    " on that chain; a native mint measures more gas than an ERC-20 mint, and check warns"
    " when max_gas_limit is below the measured native mint, measured on another chain"
    " where this one has no measurement"
)


def uniswap_support():
    actions = (
        EconomicAction.SWAP,
        EconomicAction.OPEN_LIQUIDITY,
        EconomicAction.COLLECT,
        EconomicAction.CLOSE_LIQUIDITY,
        EconomicAction.REVOKE,
        EconomicAction.EXIT_EXCHANGE,
    )
    restrictions = (
        "hookless pools, or reviewed hook profiles (Pons hooks for trading only)",
        "two standard ERC-20 currencies",
        "single pool",
        ONE_ALLOCATION_PER_FUND,
        "fixed LP range or recentring band (bands backtest in economic mode only)",
    )
    # Native pools trade and hold liquidity on exactly the chains whose router route is
    # reviewed; the PositionManager wraps and unwraps a native leg on each of them.
    from almanak.connectors.uniswap_v4.native_router import routes

    native = set(routes())

    def chain_restrictions(chain):
        if str(chain) not in native:
            return restrictions
        return (
            restrictions[0],
            NATIVE_V4_CURRENCIES,
            NATIVE_V4_LIQUIDITY,
            *restrictions[2:],
        )

    starts = (
        ("eip155:1", datetime(2026, 8, 20, tzinfo=UTC)),
        ("eip155:43114", datetime(2026, 9, 5, tzinfo=UTC)),
        ("eip155:8453", datetime(2026, 8, 20, tzinfo=UTC)),
        ("eip155:42161", datetime(2026, 8, 20, tzinfo=UTC)),
        ("eip155:10", datetime(2026, 8, 20, tzinfo=UTC)),
    )
    supports = declarations(
        actions,
        restrictions,
        {
            ChainId(chain): (
                HistoricalSupport(
                    model="uniswap-v4-exact-quoter-fixed-path-lp-v1",
                    mode="historical_path",
                    actions=actions,
                    backtest_start_at=start,
                ),
                HistoricalSupport(
                    model="uniswap-v4-economic-integer-fixed-growth-v1",
                    mode="economic",
                    actions=actions,
                    backtest_start_at=start,
                ),
            )
            for chain, start in starts
        },
        chains=tuple(ChainId(chain) for chain, _ in starts),
    )
    economic_model = "uniswap-v4-economic-integer-fixed-growth-v1"
    # Polygon and Robinhood historical models exclude REVOKE.
    extensions = tuple(
        ConnectorChainSupport.model_validate(
            {
                "chain": ChainId(chain),
                "modes": (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
                if chain in ("eip155:56", "eip155:137", "eip155:4663")
                else (RunMode.FORK, RunMode.BACKTEST),
                "actions": actions,
                "account_profiles": ("eoa", "safe_roles"),
                "restrictions": chain_restrictions(chain),
                "historical": (
                    HistoricalSupport(
                        model=economic_model,
                        mode="economic",
                        actions=historical_actions,
                        backtest_start_at=start,
                    ),
                ),
            }
        )
        for chain, start, historical_actions in (
            ("eip155:56", datetime(2026, 9, 9, 9, 38, 46, tzinfo=UTC), actions),
            (
                "eip155:137",
                datetime(2026, 9, 8, 19, 29, 13, tzinfo=UTC),
                tuple(action for action in actions if action != EconomicAction.REVOKE),
            ),
            (
                "eip155:4663",
                datetime(2026, 9, 18, 4, 28, 8, tzinfo=UTC),
                tuple(action for action in actions if action != EconomicAction.REVOKE),
            ),
        )
    )
    public = tuple(
        ConnectorChainSupport.model_validate(
            {
                **support.model_dump(),
                "restrictions": chain_restrictions(support.chain)
                + op_fee_restrictions(support.chain),
            }
        )
        for support in supports
    )
    return (*public, *extensions)


# Candidate boundaries are the retained backtest window starts of the candidate
# references under strategies/uniswap_v4_<chain>_* and their composed siblings.
# They qualify private verification only and advertise no public support.
UNISWAP_CANDIDATE_STARTS: tuple[tuple[str, datetime], ...] = ()


def uniswap_candidate_entry(template, chain, start):
    """Build one validated candidate entry from a public V4 entry's models and actions."""
    return ConnectorChainSupport.model_validate(
        {
            **template.model_dump(),
            "chain": ChainId(chain),
            "modes": (RunMode.FORK, RunMode.BACKTEST),
            # A candidate is not a production cell, so it serves no direct action.
            "direct_actions": (),
            "historical": tuple(
                {**historical.model_dump(), "backtest_start_at": start}
                for historical in template.historical
            ),
            "restrictions": (
                "candidate fork and backtest verification only; not promoted",
                *template.restrictions,
            ),
        }
    )


def candidate_uniswap_support():
    """Public V4 entries plus unregistered chains for private qualification tests only."""
    supports = uniswap_support()
    template = next(support for support in supports if support.chain == ChainId("eip155:1"))
    return (
        *supports,
        *(
            uniswap_candidate_entry(template, chain, start)
            for chain, start in UNISWAP_CANDIDATE_STARTS
        ),
    )


def historical_capability(support, chain, connector, model):
    from almanak.backtest.types import Capability, UnixTime

    if chain.environment != "mainnet":
        raise ValueError("historical execution requires explicit mainnet source identity")
    support.require(RunMode.BACKTEST, model=model)
    historical = support.require_model(model)
    return Capability(
        connector=connector,
        chain=chain,
        model=historical.model,
        actions=tuple(action.value for action in historical.actions),
        backtest_start_at=UnixTime(historical.backtest_start_at)
        if historical.backtest_start_at is not None
        else None,
    )
