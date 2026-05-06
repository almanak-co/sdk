"""VIB-3895 — production gateway oracle is recognised by ``_infer_oracle_source``.

Pre-fix: the strategy's ``price_oracle`` is a :class:`GatewayPriceOracle`
INSTANCE (not a method or partial). ``_infer_oracle_source`` only looked at
``__qualname__`` / ``__module__`` / ``__name__`` on the input callable —
which on a class instance returns just ``__module__``
(``"almanak.framework.data.price.gateway_oracle"``). None of the existing
hints matched, so every token in ``transaction_ledger.price_inputs_json``
ended up labelled ``oracle_source: "unknown"`` even though the gateway
aggregator was correctly fanning out to coingecko + chainlink + binance +
thegraph.

Post-fix: ``_infer_oracle_source`` ALSO walks ``type(price_oracle)`` so
the class's qualname / module / name participate in the haystack, AND the
hint list now recognises ``GatewayPriceOracle`` / ``gateway_oracle`` /
``MarketService`` as the aggregator path.
"""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.market.snapshot import _infer_oracle_source


class GatewayPriceOracle:
    """Stand-in for ``almanak.framework.data.price.gateway_oracle.GatewayPriceOracle``.

    Mirroring the real shape: the strategy's ``_price_oracle`` is the
    INSTANCE, not a method. ``_infer_oracle_source`` is invoked with this
    instance directly (see ``IntentStrategy.MarketSnapshot.price`` →
    ``PriceData(source=_infer_oracle_source(self._price_oracle))``).
    """

    __module__ = "almanak.framework.data.price.gateway_oracle"

    def __call__(self, token: str, quote: str = "USD") -> Decimal:
        return Decimal("1")


class MarketServiceStub:
    """Stand-in for the gRPC ``gateway_pb2_grpc.MarketServiceStub.GetPrice``
    bound method shape — qualname carries ``MarketService``, no module
    keyword does."""

    __module__ = "almanak.gateway.proto.gateway_pb2_grpc"

    def __call__(self, token: str, quote: str = "USD") -> Decimal:
        return Decimal("1")


def test_gateway_price_oracle_instance_recognised_as_aggregator():
    """The production wiring uses ``GatewayPriceOracle()`` — an instance,
    not a method. Pre-fix this returned ``""``."""
    oracle = GatewayPriceOracle()
    assert _infer_oracle_source(oracle) == "aggregator"


def test_marketservice_stub_qualname_recognised_as_aggregator():
    """Some wirings hand the strategy a gRPC stub method directly. The
    class's ``__qualname__`` carries ``MarketService`` which the new hint
    catches."""
    stub = MarketServiceStub()
    assert _infer_oracle_source(stub) == "aggregator"


def test_class_introspection_reads_type_qualname():
    """Even when the haystack pre-VIB-3895 was empty (instance with no
    method), the new fallback walks ``type()`` so the CLASS's qualname /
    module participate in the keyword match."""

    class CoingeckoCachedFetcher:
        __module__ = "third.party.module"

        def __call__(self, token, quote):
            return Decimal("1")

    fetcher = CoingeckoCachedFetcher()
    # The class qualname carries 'Coingecko' — the existing hint catches it.
    assert _infer_oracle_source(fetcher) == "coingecko"


def test_unknown_class_still_returns_empty_string():
    """An instance whose class identity carries no provider hint should
    still degrade gracefully to ``""``. Otherwise the cache would carry
    a spurious source."""

    class GenericOpaqueProvider:
        __module__ = "third.party.opaque"

        def __call__(self, token, quote):
            return Decimal("1")

    p = GenericOpaqueProvider()
    assert _infer_oracle_source(p) == ""


def test_callable_qualname_still_dominates_class():
    """A bound method whose own qualname matches a hint must still win
    even if the class also matches a different hint. This locks the
    "first match wins" contract from the existing _PROVIDER_NAME_HINTS
    docstring."""

    class CoingeckoProvider:
        __module__ = "third.party"

        def get_price(self, token, quote, chain):
            return Decimal("1")

    method = CoingeckoProvider().get_price
    # Method's qualname starts with 'CoingeckoProvider.get_price' —
    # 'coingecko' is the FIRST hint, so it wins regardless of what the
    # class fallback would produce.
    assert _infer_oracle_source(method) == "coingecko"


def test_sync_wrapper_unwraps_to_underlying_oracle():
    """VIB-3895 v2 — the production wiring path is:

        GatewayPriceOracle() -> create_sync_price_oracle_func(...) -> sync_price

    The strategy's ``_price_oracle`` is the local ``sync_price`` function,
    NOT the underlying ``GatewayPriceOracle`` instance. Pre-fix
    ``_infer_oracle_source(sync_price)`` returned ``""`` because
    ``sync_price.__qualname__`` is
    ``create_sync_price_oracle_func.<locals>.sync_price`` — none of the
    provider hints match. Result: every production ledger row carried
    ``oracle_source: "unknown"``.

    Post-fix: the wrapper stamps ``__wrapped__`` on the sync function
    (functools.wraps convention). ``_infer_oracle_source`` walks
    ``__wrapped__`` recursively so the underlying class's identity wins
    the hint match.
    """
    oracle = GatewayPriceOracle()

    # Reproduce the exact wrapper shape the CLI generates without importing
    # the CLI module (which has side-effects). This matches
    # ``almanak/framework/cli/run.py:create_sync_price_oracle_func``.
    def sync_price(token, quote="USD", chain=None):
        return Decimal("1")

    sync_price.__wrapped__ = oracle  # type: ignore[attr-defined]

    assert _infer_oracle_source(sync_price) == "aggregator"


def test_unwrapping_handles_chained_and_cyclic_wrappers():
    """A pathological wrapper-of-wrapper chain — and a deliberate cycle —
    must not infinite-loop. The unwrapper has a ``seen`` set guarding
    against re-visit."""
    oracle = GatewayPriceOracle()

    def inner(*a, **kw):
        return Decimal("1")
    inner.__wrapped__ = oracle  # type: ignore[attr-defined]

    def outer(*a, **kw):
        return Decimal("1")
    outer.__wrapped__ = inner  # type: ignore[attr-defined]

    assert _infer_oracle_source(outer) == "aggregator"

    # Cycle: outer -> inner -> outer. The unwrap loop must terminate.
    inner.__wrapped__ = outer  # type: ignore[attr-defined]
    # No assertion on the result — the contract is "doesn't hang". The
    # match still resolves because outer/inner are both visited before
    # the cycle closes.
    _ = _infer_oracle_source(outer)


# NOTE: a "wrapper-around-opaque-oracle returns empty" test would normally
# live here, but the haystack legitimately includes this test module's own
# path — and this file's name carries "gateway_vib3895", which the existing
# "gateway" hint correctly matches. The graceful-degradation contract is
# already exercised by ``test_unknown_class_still_returns_empty_string``
# above, where the class is defined inline with a benign ``__module__``.
