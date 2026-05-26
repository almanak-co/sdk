"""4-layer intent test for TraderJoe V2 LP_COLLECT_FEES on Ethereum Anvil fork.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow for
collecting fees from a Liquidity Book position via ``LBPair.collectFees``:

1. Open an LP position in the USDT/USDC binStep=1 LBPair (setup).
2. Build a ``CollectFeesIntent`` for the same pool.
3. Compile to ActionBundle via :class:`IntentCompiler`, dispatching through
   ``TraderJoeV2Compiler.compile_collect_fees``.
4. Execute via the chain's orchestrator. Under the default-on Zodiac model
   this is a :class:`ZodiacOrchestrator` that derives the manifest from the
   intents we compile and applies new ``(target, selector)`` pairs to the
   Roles Modifier — proving the manifest authorises ``LBPair.collectFees``.
5. Either: parse the receipt + assert ``ClaimedFees`` was emitted +
   non-negative wallet deltas; or — when the LBPair reverts on its own
   internal zero-fee guard — assert the failure is NOT a Zodiac
   authorisation denial (see "Authz coverage decision" below).

NO MOCKING. All transactions execute on a real Anvil fork and verify state.

Authz coverage decision (issue #1855)
-------------------------------------
The pair ``(traderjoe_v2, LP_COLLECT_FEES)`` was deferred from the on-chain
permission gate when Phase A–F shipped because activating it required:

- a ``CollectFeesIntent`` test that compiles AND executes through Zodiac
  (Phase A–F's harness-branch shape);
- an authz-meaningful pass criterion that doesn't rely on synthetic fee
  revenue accruing to a freshly-minted position.

Phase G's pivot to default-on Zodiac means (1) is automatic — every intent
test in ``tests/intents/<chain>/`` runs through Safe + Roles + late-binding
manifest by default, so the act of compiling + executing a ``CollectFeesIntent``
under the standard ``orchestrator`` fixture IS the authz proof.

For (2): the ticket explicitly accepts "skip the fee-balance assertion and
only assert authz succeeds" as the production-correct minimum. We take that
route deliberately. The alternative — Anvil time-travel + simulated swap
activity to accrue real fees — would fabricate fee revenue that wouldn't
exist on a freshly minted position and introduce fragility (fork-block
sensitivity, slippage tolerance, route stability).

Concretely "authz succeeds" means: the Zodiac Roles Modifier did NOT block
the call. The :class:`ZodiacOrchestrator` raises ``AuthorizationFailed``
when an inner revert's 4-byte selector matches the Roles authz error set
(``ConditionViolation`` / ``NoMembership`` / legacy variants); any other
revert is a protocol-layer concern that the manifest doesn't speak to.

The TraderJoe V2 LBPair (V2.0 implementation) reverts ``collectFees`` with
empty revert-data when ``account`` has zero accrued fees across the
requested bins. On a freshly minted position no swap activity has crossed
the bins yet so all fee accumulators are zero — the inner revert is a
LBPair contract-level guard, NOT a manifest gap. We accept either outcome
as authz-positive: a successful collection (fees > 0 path; verify the
ClaimedFees event + non-negative wallet deltas) or a non-authz revert
(fees = 0 path; verify the orchestrator did NOT raise
``AuthorizationFailed``).

To run::

    uv run pytest tests/intents/ethereum/test_traderjoe_v2_collect_fees.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.traderjoe_v2 import TraderJoeV2Adapter, TraderJoeV2Config
from almanak.connectors.traderjoe_v2.permission_hints import (
    _TRADERJOE_COLLECT_FEES_SELECTOR,
)
from almanak.connectors.traderjoe_v2.receipt_parser import (
    TraderJoeV2EventType,
    TraderJoeV2ReceiptParser,
)
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import (
    CollectFeesIntent,
    IntentCompiler,
    LPOpenIntent,
)
from almanak.framework.intents.vocabulary import IntentType
from tests.intents import _traderjoe_v2_layer5 as _l5
from tests.intents._permission_onchain_harness import (
    AuthorizationFailed,
    is_zodiac_authz_revert,
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_traderjoe_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "ethereum"

# USDT/USDC LBPair with binStep=1 (the only TJv2 LBPair on Ethereum carrying
# meaningful reserves as of 2026-05-14: ~497 USDT / ~70 USDC). Pinned in
# ``almanak.core.contracts.TRADERJOE_V2_LBPAIRS`` so the static permission
# entry for ``collectFees`` lands on this exact address at manifest time.
# Token X = USDT, Token Y = USDC (verified on-chain via LBPair.getTokenX/Y).
POOL = "USDT/USDC/1"
# Sizing is intentionally tiny: the pool only has ~$500 of total liquidity at
# this fork block, so opening with 50/50 (the avalanche reference template
# size) would distort the active bin set and risk reverting. Mirrors the
# sibling ``test_traderjoe_v2_lp.py`` on this chain for consistency.
LP_AMOUNT_USDT = Decimal("5")  # token X
LP_AMOUNT_USDC = Decimal("5")  # token Y

# Stables: USDC-per-USDT range ~1:1. Range bounds mirror the sibling
# ``test_traderjoe_v2_lp.py`` so the position shape is identical to the
# other TJv2 LP intent tests on this Ethereum fork.
RANGE_LOWER = Decimal("0.5")
RANGE_UPPER = Decimal("2")

BIN_STEP = 1


# =============================================================================
# Layer-5 accounting helpers (epic VIB-4591, ticket VIB-4598)
# =============================================================================
#
# Shared across all five TraderJoe V2 LP intent-test files via
# ``tests/intents/_traderjoe_v2_layer5.py`` (gemini PR #2366: de-duplicate
# the ~180-line-per-file block). The thin chain-bound wrapper below binds
# this file's ``CHAIN_NAME`` so call sites stay one-liners. The module
# docstring documents the bin-model directional null-contract in full.
#
# This file only exercises LP_COLLECT_FEES (fee-only harvest); its
# parser↔event equality is asserted inline against the ClaimedFees
# ``parsed_fees_x`` / ``parsed_fees_y`` totals, so the close-specific
# ``assert_close_parser_event_equality`` helper is intentionally not bound.


def _enrich_for_accounting(execution_result, intent, wallet: str, bundle_metadata: dict | None = None):
    return _l5.enrich_for_accounting(
        execution_result,
        intent,
        chain=CHAIN_NAME,
        wallet=wallet,
        bundle_metadata=bundle_metadata,
    )


_payload = _l5.payload
_to_human = _l5.to_human
_assert_identity = _l5.assert_identity
_assert_no_lot_id = _l5.assert_no_lot_id
_assert_accounting_persisted_or_xfail = _l5.assert_accounting_persisted_or_xfail
_assert_bin_model_null_contract = _l5.assert_bin_model_null_contract


# =============================================================================
# Helpers
# =============================================================================


def _get_position_via_adapter(
    rpc_url: str,
    wallet: str,
    token_x: str,
    token_y: str,
    bin_step: int,
):
    """Query position using TraderJoeV2Adapter (matches sibling LP test)."""
    config = TraderJoeV2Config(
        chain=CHAIN_NAME,
        wallet_address=wallet,
        rpc_url=rpc_url,
    )
    adapter = TraderJoeV2Adapter(config)
    return adapter.get_position(token_x, token_y, bin_step, wallet=wallet)


async def _open_position_via_intent(
    funded_wallet: str,
    orchestrator: ExecutionOrchestrator,
    price_oracle: dict[str, Decimal],
    anvil_rpc_url: str,
) -> None:
    """Open an LP position so ``CollectFeesIntent`` has something to target.

    Mirrors ``test_traderjoe_v2_lp.py::_open_position_via_intent``. The same
    intent flow is used so the orchestrator's late-binding manifest also
    applies ``addLiquidity`` (LBRouter) targets on top of ``collectFees``
    (LBPair) — confirming the multi-step manifest extension path works.
    """
    intent = LPOpenIntent(
        pool=POOL,
        amount0=LP_AMOUNT_USDT,
        amount1=LP_AMOUNT_USDC,
        range_lower=RANGE_LOWER,
        range_upper=RANGE_UPPER,
        protocol="traderjoe_v2",
        chain=CHAIN_NAME,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=funded_wallet,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    open_compilation = compiler.compile(intent)
    assert open_compilation.status.value == "SUCCESS", (
        f"Setup LP_OPEN compilation failed: {open_compilation.error}"
    )
    assert open_compilation.action_bundle is not None

    open_execution = await orchestrator.execute(open_compilation.action_bundle)
    assert open_execution.success, f"Setup LP_OPEN execution failed: {open_execution.error}"


def _is_authz_failure(error_message: str | None) -> bool:
    """Return True iff ``error_message`` carries a Zodiac authz selector.

    ``ZodiacOrchestrator.execute`` raises ``AuthorizationFailed`` when the
    inner revert's 4-byte selector is in
    :data:`tests.intents._permission_onchain_harness._ZODIAC_AUTHZ_ERROR_SELECTORS`,
    so reaching this helper means the orchestrator already ruled the failure
    NOT-authz. Belt-and-suspenders: re-parse the formatted error string for
    a literal selector and double-check via :func:`is_zodiac_authz_revert`.
    Defensive — guards against the orchestrator's authz classification ever
    drifting from this test's interpretation of "authz succeeded".
    """
    if not error_message:
        return False
    # ``ZodiacOrchestrator.execute`` formats non-authz reverts as:
    #   "Inner tx reverted under execTransactionWithRole (..., selector=0xabcdef12)..."
    # — extract the selector and run it through the authoritative classifier.
    marker = "selector="
    idx = error_message.find(marker)
    if idx == -1:
        return False
    selector = error_message[idx + len(marker) : idx + len(marker) + 10]
    return is_zodiac_authz_revert(selector.lower())


# =============================================================================
# CollectFeesIntent Tests
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lp
class TestTraderJoeV2CollectFeesIntent:
    """Test TraderJoe V2 LP_COLLECT_FEES using ``CollectFeesIntent``.

    Verifies the full Intent flow:

    - LP_OPEN setup mints a position into the USDT/USDC binStep=1 LBPair.
    - ``CollectFeesIntent`` creation succeeds (no protocol_params required —
      the connector adapter discovers the position from the LBPair on-chain).
    - ``IntentCompiler`` routes through ``TraderJoeV2Compiler.compile_collect_fees`` and
      builds an ActionBundle whose only TX is
      ``LBPair.collectFees(account, binIds)``.
    - The orchestrator executes the bundle. Under default-on Zodiac, this
      doubles as the manifest-authorisation proof: if the LBPair
      ``collectFees`` selector is missing from the static permissions,
      ``execTransactionWithRole`` would surface ``AuthorizationFailed``.
    """

    @pytest.mark.intent(IntentType.LP_OPEN, IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_collect_fees_usdt_usdc(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
        layer5_accounting_harness,
        anvil_eth_call_adapter,
    ):
        """LP_COLLECT_FEES on a freshly-opened USDT/USDC LB position.

        4-Layer Verification:

        1. **Compilation**: ``IntentCompiler`` -> SUCCESS with ActionBundle
           containing exactly one ``traderjoe_v2_collect_fees`` TX targeted
           at the LBPair address.
        2. **Execution**: orchestrator does NOT raise ``AuthorizationFailed``.
           Either ``execution_result.success == True`` (the LBPair accrued
           non-zero fees and the collect succeeded) OR
           ``execution_result.success == False`` with a non-authz inner
           revert (the LBPair's zero-fee guard fired). Both outcomes prove
           the manifest authorises ``LBPair.collectFees``; what they
           disprove is "the Roles Modifier blocked the call". This is the
           production-correct minimum the ticket calls option (a).
        3. **Receipt parsing** (success path): ``TraderJoeV2ReceiptParser``
           emits a ``CLAIMED_FEES`` event for the LBPair address.
        4. **Balance deltas** (success path): USDT / USDC balances are
           non-negative and equal to the parser-extracted fee amounts.

        Per the issue body's "fix shape" option (a): we deliberately do NOT
        attempt to accrue fees via Anvil time-travel + synthetic swap
        activity. The authz-success criterion is the load-bearing assertion
        for permission coverage; fee-revenue assertions belong in connector
        unit tests, not in the on-chain authz harness.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc_addr = tokens["USDC"]
        usdt_addr = tokens["USDT"]
        fail_if_traderjoe_pool_missing(web3, CHAIN_NAME, usdt_addr, usdc_addr, BIN_STEP)
        usdc_decimals = get_token_decimals(web3, usdc_addr)
        usdt_decimals = get_token_decimals(web3, usdt_addr)

        print(f"\n{'=' * 80}")
        print("Test: LP_COLLECT_FEES USDT/USDC via CollectFeesIntent (TraderJoe V2)")
        print(f"{'=' * 80}")

        # 1. Setup: open LP position so collectFees has bins to target.
        print("\n--- Setup: Opening LP position ---")
        await _open_position_via_intent(funded_wallet, orchestrator, price_oracle, anvil_rpc_url)

        position = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=usdt_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position is not None and position.bin_ids, (
            "Setup LP_OPEN must yield a TraderJoe V2 position with bin IDs"
        )
        print(f"Position opened across {len(position.bin_ids)} bins")

        # Record balances BEFORE collect to drive the balance-delta layer.
        usdt_before = get_token_balance(web3, usdt_addr, funded_wallet)
        usdc_before = get_token_balance(web3, usdc_addr, funded_wallet)
        print(f"USDT before collect: {format_token_amount(usdt_before, usdt_decimals)}")
        print(f"USDC before collect: {format_token_amount(usdc_before, usdc_decimals)}")

        # 2. Layer 1: Compilation
        collect_intent = CollectFeesIntent(
            pool=POOL,
            protocol="traderjoe_v2",
            chain=CHAIN_NAME,
        )
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("\nCompiling CollectFeesIntent...")
        compilation_result = compiler.compile(collect_intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"LP_COLLECT_FEES compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        bundle = compilation_result.action_bundle
        assert len(bundle.transactions) == 1, (
            f"TraderJoe V2 LP_COLLECT_FEES should compile to exactly one TX, "
            f"got {len(bundle.transactions)}"
        )
        first_tx = bundle.transactions[0]
        tx_to = first_tx["to"] if isinstance(first_tx, dict) else first_tx.to
        tx_data = first_tx["data"] if isinstance(first_tx, dict) else first_tx.data
        tx_data_hex = tx_data.hex() if hasattr(tx_data, "hex") else tx_data
        if not tx_data_hex.startswith("0x"):
            tx_data_hex = f"0x{tx_data_hex}"
        # The collect TX must target the LBPair, not the LBRouter — the
        # static permission for ``collectFees`` lives on the LBPair only,
        # so a misrouted TX would surface as AuthorizationFailed even when
        # the manifest is correct.
        assert Web3.to_checksum_address(tx_to) == Web3.to_checksum_address(
            position.pool_address
        ), (
            f"collect_fees TX must target the LBPair {position.pool_address}, "
            f"got {tx_to}"
        )
        # Pin the selector too: a wrong LBPair method (e.g. ``increaseOracleLength``)
        # at the right target would surface later as ``AuthorizationFailed`` and
        # look like a manifest regression even though the bug is in the compiler.
        # Imported from ``permission_hints`` so the test and the manifest read
        # the same canonical value.
        assert tx_data_hex[:10].lower() == _TRADERJOE_COLLECT_FEES_SELECTOR, (
            f"collect_fees TX must use selector {_TRADERJOE_COLLECT_FEES_SELECTOR} "
            f"(collectFees(address,uint256[])), got {tx_data_hex[:10]}"
        )
        print(f"ActionBundle: 1 TX targeting LBPair {tx_to}, selector {tx_data_hex[:10]}")

        # 3. Layer 2: Execution. Authz-success means the orchestrator does
        #    not raise AuthorizationFailed. If the LBPair reverts internally
        #    on its zero-fee guard the orchestrator returns success=False
        #    with a non-authz selector — that's still authz-positive.
        print("\nExecuting LP_COLLECT_FEES via orchestrator...")
        try:
            execution_result = await orchestrator.execute(bundle)
        except AuthorizationFailed as exc:
            pytest.fail(
                f"Zodiac Roles Modifier blocked LBPair.collectFees: {exc}\n"
                "This means the LP_COLLECT_FEES manifest is missing the "
                "LBPair collectFees(address,uint256[]) selector — issue #1855."
            )

        # Sanity: if execution failed, surface that the failure is NOT a
        # Zodiac authz revert (that would already have raised above, but
        # guard against drift in the orchestrator's classification).
        if not execution_result.success:
            assert not _is_authz_failure(execution_result.error), (
                f"Inner-tx revert carries a Zodiac authz selector: "
                f"{execution_result.error}. Manifest gap — issue #1855."
            )
            # Layer 4: balance conservation on the failure path. A revert
            # inside the LBPair (e.g. the zero-fee guard) MUST roll back to
            # the snapshot — neither USDT nor USDC may move. Catches a
            # silent partial-effect regression in either the LBPair or the
            # orchestrator's revert handling.
            usdt_after_failed = get_token_balance(web3, usdt_addr, funded_wallet)
            usdc_after_failed = get_token_balance(web3, usdc_addr, funded_wallet)
            assert usdt_after_failed == usdt_before, (
                f"On failed LP_COLLECT_FEES, USDT balance must be unchanged "
                f"(before={usdt_before}, after={usdt_after_failed})."
            )
            assert usdc_after_failed == usdc_before, (
                f"On failed LP_COLLECT_FEES, USDC balance must be unchanged "
                f"(before={usdc_before}, after={usdc_after_failed})."
            )
            print(
                f"Execution returned success=False with a non-authz inner revert: "
                f"{execution_result.error}"
            )
            print(
                "This is the LBPair's zero-fee guard firing on a freshly minted "
                "position (no swap activity has crossed the bins yet). The "
                "Roles Modifier authorised the call to LBPair.collectFees — "
                "that's the load-bearing authz proof for #1855."
            )
            # Layer 5 — N/A on the zero-fee revert path.
            # ``execution_result.success is False``: the LBPair reverted on
            # its internal zero-fee guard, so there is no successful
            # LP_COLLECT_FEES intent to book. The Layer-5 success helper
            # requires a successful ExecutionResult; ``assert_no_accounting_
            # on_failure`` would assert the books-side mirror of a *failed
            # on-chain effect*, but here the contract being proven is authz,
            # not accounting, and the balance-conservation asserts above are
            # the correct books-side mirror for this revert. Layer 5 fires
            # only on the fees>0 success branch below.
            print("\nALL AUTHZ ASSERTIONS PASSED (option (a) — authz-only)")
            return

        # Successful execution path — the position had non-zero fees, so we
        # can ALSO exercise the receipt-parsing and balance-delta layers.
        # Enrichment for accounting is deferred to just before the Layer-5
        # call (after Layers 3/4), so an enricher regression cannot mask
        # the receipt-parse/balance hard asserts (CodeRabbit PR #2366).
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions")

        # 4. Layer 3: Receipt parsing. Assert ClaimedFees was emitted on
        #    the LBPair (the canonical event for LBPair.collectFees).
        parser = TraderJoeV2ReceiptParser()
        found_claimed_fees_event = False
        parsed_fees_x = 0
        parsed_fees_y = 0
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            receipt_dict = tx_result.receipt.to_dict()
            parse_result = parser.parse_receipt(receipt_dict)
            assert parse_result.success, (
                f"Receipt parsing must succeed, got: {parse_result.error}"
            )
            for event in parse_result.events:
                if event.event_type == TraderJoeV2EventType.CLAIMED_FEES:
                    found_claimed_fees_event = True
                    assert (
                        Web3.to_checksum_address(event.contract_address)
                        == Web3.to_checksum_address(position.pool_address)
                    ), (
                        f"ClaimedFees emitted on {event.contract_address}, "
                        f"expected LBPair {position.pool_address}"
                    )
            collected = parser.extract_collected_fees(receipt_dict)
            if collected and collected.success:
                parsed_fees_x += int(collected.fees_x or 0)
                parsed_fees_y += int(collected.fees_y or 0)
        assert found_claimed_fees_event, (
            "Receipt must surface a ClaimedFees event on the LBPair — "
            "the canonical signal that LBPair.collectFees executed."
        )
        print(
            f"Parser observed ClaimedFees on LBPair "
            f"(fees_x={parsed_fees_x}, fees_y={parsed_fees_y})"
        )

        # 5. Layer 4: Balance deltas. Wallet must not LOSE tokens during a
        #    fee collection. Parser-extracted amounts must agree with wallet
        #    deltas — the receipt parser and on-chain state must match.
        usdt_after = get_token_balance(web3, usdt_addr, funded_wallet)
        usdc_after = get_token_balance(web3, usdc_addr, funded_wallet)
        usdt_delta = usdt_after - usdt_before
        usdc_delta = usdc_after - usdc_before
        print(f"USDT delta: {format_token_amount(usdt_delta, usdt_decimals)}")
        print(f"USDC delta: {format_token_amount(usdc_delta, usdc_decimals)}")
        assert usdt_delta >= 0, (
            f"USDT balance must not decrease during fee collection, got {usdt_delta}"
        )
        assert usdc_delta >= 0, (
            f"USDC balance must not decrease during fee collection, got {usdc_delta}"
        )
        assert usdt_delta == parsed_fees_x, (
            f"USDT wallet delta ({usdt_delta}) must equal parser fees_x "
            f"({parsed_fees_x}) — receipt parser and on-chain state disagree"
        )
        assert usdc_delta == parsed_fees_y, (
            f"USDC wallet delta ({usdc_delta}) must equal parser fees_y "
            f"({parsed_fees_y}) — receipt parser and on-chain state disagree"
        )

        # 6. Position must remain open after fee collection. Fee collection
        #    explicitly does NOT remove liquidity (that's the LP_CLOSE path);
        #    if the bin set went empty something else broke.
        position_after = _get_position_via_adapter(
            rpc_url=anvil_rpc_url,
            wallet=funded_wallet,
            token_x=usdt_addr,
            token_y=usdc_addr,
            bin_step=BIN_STEP,
        )
        assert position_after is not None and position_after.bin_ids, (
            "Position must remain open after LP_COLLECT_FEES — "
            "fee collection must NOT remove liquidity"
        )
        assert set(position_after.bin_ids) == set(position.bin_ids), (
            "Bin set must be unchanged by fee collection "
            f"(before={sorted(position.bin_ids)}, "
            f"after={sorted(position_after.bin_ids)})"
        )

        # 7. Layer 5 — assert the real accounting pipeline persisted
        # LP_COLLECT_FEES (fees>0 success branch only; the zero-fee revert
        # path returned above as Layer-5 N/A by construction). Enrichment
        # runs HERE, after Layers 1–4 (CodeRabbit PR #2366).
        accounting_result = _enrich_for_accounting(
            execution_result, collect_intent, funded_wallet, bundle.metadata
        )
        collect_accounting_row = await _assert_accounting_persisted_or_xfail(
            layer5_accounting_harness,
            intent=collect_intent,
            result=accounting_result,
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            expected_event_type="LP_COLLECT_FEES",
            price_oracle=price_oracle,
            eth_call_reader=anvil_eth_call_adapter,
        )
        _assert_identity(collect_accounting_row, event_type="LP_COLLECT_FEES", wallet=funded_wallet)
        collect_payload = _payload(collect_accounting_row)
        assert collect_payload["position_key"] == collect_accounting_row["position_key"]
        _assert_no_lot_id(collect_accounting_row, collect_payload)
        # #2 bin-model directional null-contract (no fabricated tick/hash fields).
        _assert_bin_model_null_contract(collect_payload, event_type="LP_COLLECT_FEES")
        # #3 parser ↔ event exact scaled-int equality. A fee-only harvest:
        # principal amounts are measured-zero; the fee legs carry the value
        # and must equal the parser-extracted ClaimedFees amounts exactly
        # (token X = USDT → fees0, token Y = USDC → fees1).
        assert Decimal(collect_payload["amount0"]) == Decimal("0")
        assert Decimal(collect_payload["amount1"]) == Decimal("0")
        assert Decimal(collect_payload["fees0_collected"]) == _to_human(parsed_fees_x, usdt_decimals)
        assert Decimal(collect_payload["fees1_collected"]) == _to_human(parsed_fees_y, usdc_decimals)

        print("\nALL 4 LAYERS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
