"""GMX scan pages through the composed external-effects path, alone and beside Across.

The pages are the three a real fork run of the GMX position reference retained: one
carrying a position event and two empty ones. None carries a liquidation; the capture
that does is retained evidence this suite cannot assume. The GMX reader is replaced by
one that serves those pages in order, so the composed routing, the per-component page
view, the commit, the gateway's independent verification, the audit and the union
decode are the real code. The fork's node identity is relabeled to the test's own.
"""

import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import external_effects
from almanak.connectors.composed.inventory import refresh
from almanak.connectors.composed.lifecycle import component_state
from almanak.connectors.composed.models import Config, target_assets
from almanak.connectors.composed.requests import ComposedExternalEffects
from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.gmx_v2 import liquidation_reader
from almanak.connectors.gmx_v2.liquidation_types import Scan as GmxScan
from almanak.connectors.gmx_v2.state import Checkpoint as GmxCheckpoint
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Balance, WalletAsset
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway.ethereum import DataError
from almanak.work import merge, project
from tests.gmx_retired import current_json
from tests.test_across_records import ARB_NODE, FORK_ARB
from tests.test_across_records import config as config  # noqa: F401 - a fixture
from tests.test_across_settlement import FILL_DEADLINE, flight, here, refunded
from tests.test_composed_across import balances, checkpoint, earn

GMX, ACROSS = ConnectorId("gmx_v2"), ConnectorId("across")
CAPTURE = Path(__file__).with_name("fixtures") / (
    "evidence/2026-09-30-gmx-deployment-rotation/fork/gmx_v2_arbitrum_position/checkpoint.json.gz"
)
CAPTURED_NODE = "0x9727c7a8190a132d13673e52e927e0e417ec490f7fa9d5a661a3ffeaf7166b7f"


@pytest.fixture
def captured():
    raw = current_json(gzip.decompress(CAPTURE.read_bytes()))
    return GmxCheckpoint.model_validate_json(raw.replace(CAPTURED_NODE, ARB_NODE))


def gmx_inventory(captured):
    target = captured.config.target("Main")
    return tuple(
        Balance(key=WalletAsset(account="fund", chain=FORK_ARB, asset=asset), amount=0)
        for asset in target_assets(target)
    )


def holding(state, captured):
    """The checkpoint while the captured position is held, which is what a scan follows."""
    held = next(o.before.owned for o in captured.outcomes if o.before.owned.position is not None)
    owned = tuple(held if o.target == held.target else o for o in state.owned)
    return state.model_copy(update={"owned": refresh(owned, state.inventory)})


def gmx_only(captured):
    parameters = Config(
        decision_trigger=captured.config.decision_trigger,
        accounts=captured.config.accounts,
        allocations=tuple(captured.config.allocations),
        inventory=gmx_inventory(captured),
        fork_bindings=captured.config.fork_bindings,
        asset_metadata=captured.config.asset_metadata,
    )
    # One chain: the checkpoint has a single execution slot and no per-chain work.
    return holding(checkpoint(parameters, initialized=True), captured)


def mixed(captured, across, transfers):
    accounts = across.accounts.model_copy(
        update={"bindings": (*captured.config.accounts.bindings, *across.accounts.bindings)}
    )
    base = tuple(b for b in across.fork_bindings if b.chain != FORK_ARB)
    parameters = Config(
        decision_trigger=captured.config.decision_trigger,
        accounts=accounts,
        allocations=(*captured.config.allocations, *across.allocations, earn()),
        inventory=(*gmx_inventory(captured), *balances(source=0)),
        fork_bindings=(*captured.config.fork_bindings, *base),
        asset_metadata=captured.config.asset_metadata,
    )
    return holding(checkpoint(parameters, transfers=transfers), captured)


class Reader:
    """A chain that pins the captured GMX blocks and is otherwise the Across fake."""

    def __init__(self, pages, chain=None):
        self.pinned = {b.number: b for page in pages for b in (page.start, page.end)}
        self.inner, self.chain = chain, FORK_ARB

    def pin_block(self, number):
        return self.pinned[number] if number in self.pinned else self.inner.pin_block(number)

    def __getattr__(self, name):
        return getattr(self.inner, name)


@pytest.fixture
def served(monkeypatch, captured):
    """Serve the captured pages in order to whatever asks the GMX reader, and record
    the configuration and the retained pages each call saw."""
    pages, seen = captured.external_scans, []

    def page(backend, state, target, reference=None):
        seen.append((backend.config, state.external_scans))
        index = len(state.external_scans)
        return pages[index] if index < len(pages) else None

    def logs(reader, target, start, end):
        return next(p.events for p in pages if (p.start, p.end) == (start, end))

    monkeypatch.setattr(liquidation_reader, "page", page)
    monkeypatch.setattr(liquidation_reader, "logs", logs)
    return seen


def backend(state, reader):
    fake = SimpleNamespace(config=state.config, reader=reader)
    fake.audit_history = lambda current: ComposedExternalEffects.audit(fake, current)
    return fake


def tick(state, reader):
    """One external-effects tick of Arbitrum: propose, commit, verify, merge."""
    fake, local = backend(state, reader), project(state, FORK_ARB)
    page = ComposedExternalEffects.reconcile(fake, local)
    if page is None:
        return state
    committed = external_effects.commit(local, local, page)
    assert gateway_effects.verified_commit(fake, local, committed) == committed
    return merge(state, FORK_ARB, committed)


def restarted(state):
    return Checkpoint.model_validate_json(state.model_dump_json())


def test_a_gmx_only_composed_deployment_records_audits_and_decodes_its_pages(captured, served):
    pages = captured.external_scans
    assert len(pages) == 3 and pages[0].events and not any(p.outcome for p in pages)
    state, reader = gmx_only(captured), Reader(pages)
    for _ in range(4):
        state = restarted(tick(state, reader))
    # The fourth tick found nothing: the pages are retained once, in order, as GMX's own.
    assert state.external_scans == pages
    assert all(type(page) is GmxScan for page in state.external_scans)
    assert state.outcomes == () and state.transfers == ()
    # The reader was asked through GMX's own configuration and saw its own pages only.
    assert all(seen == state.config.component(GMX) for seen, _ in served)
    assert component_state(state, GMX).external_scans == pages
    ComposedExternalEffects.audit(backend(state, reader), project(state, FORK_ARB))
    # A retained page whose block is no longer the canonical one stops the audit.
    reader.pinned[pages[1].end.number] = pages[2].end
    with pytest.raises(DataError, match="no longer canonical"):
        ComposedExternalEffects.audit(backend(state, reader), project(state, FORK_ARB))


def test_a_committed_page_is_refused_when_the_gateway_reads_another(captured, served):
    pages = captured.external_scans
    state, reader = gmx_only(captured), Reader(pages)
    local = project(state, FORK_ARB)
    committed = external_effects.commit(local, local, pages[1])
    with pytest.raises(ValueError, match="differs from independently verified"):
        gateway_effects.verified_commit(backend(state, reader), local, committed)


def test_gmx_and_across_pages_alternate_and_keep_their_order_across_a_restart(
    captured,
    served,
    config,  # noqa: F811
):
    pages = captured.external_scans
    # A transfer already proven unfilled: Arbitrum, its source chain, scans for the refund.
    transfer = flight(config).model_copy(update={"unfilled": True})
    state = mixed(captured, config, (transfer,))
    reader = Reader(pages, here(FILL_DEADLINE + 50))
    for _ in range(5):
        state = restarted(tick(state, reader))
    order = [page.connector for page in state.external_scans]
    # Each tick starts after the connector of the last committed page, read back from
    # the decoded checkpoint. The second tick reads the bridge although GMX has a page
    # ready; the fourth finds the bridge with nothing new and reads GMX's last page.
    assert order == [GMX, ACROSS, GMX, GMX]
    assert [type(page) is GmxScan for page in state.external_scans] == [True, False, True, True]
    # Each component is shown its own pages, and GMX was asked with only its own retained.
    assert component_state(state, GMX).external_scans == pages
    (bridge_page,) = component_state(state, ACROSS).external_scans
    assert bridge_page.connector == ACROSS and bridge_page.outcome is None
    assert all(page.connector == GMX for _, retained in served for page in retained)
    assert all(seen == state.config.component(GMX) for seen, _ in served)
    # A page alone leaves ownership, cash and the transfer as they were.
    assert state.outcomes == () and state.transfers == (transfer,)
    ComposedExternalEffects.audit(backend(state, reader), project(state, FORK_ARB))
    # With GMX out of pages the bridge is still read: a stranger's refund leaf is a new page.
    refunded(config, reader.inner, reader.inner.head + 2, [5], tx="0x" + "d0" * 32)
    state = restarted(tick(state, reader))
    assert state.external_scans[-1].connector == ACROSS and len(state.external_scans) == 5
