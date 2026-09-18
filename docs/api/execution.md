# Execution

The execution pipeline compiles intents, signs transactions, simulates, and submits them on-chain.

## GatewayExecutionOrchestrator

The primary orchestrator used when running with the gateway sidecar.

::: almanak.framework.execution.GatewayExecutionOrchestrator
    options:
      heading_level: 2
      members_order: source

::: almanak.framework.execution.ExecutionOrchestrator
    options:
      heading_level: 2
      members_order: source

::: almanak.framework.execution.ExecutionResult
    options:
      heading_level: 2

::: almanak.framework.execution.ExecutionContext
    options:
      heading_level: 2

## Result Enrichment

After successful execution, `ResultEnricher` automatically extracts data from transaction receipts (position IDs, swap amounts, etc.) and attaches it to the result.

::: almanak.framework.execution.ResultEnricher
    options:
      heading_level: 3

::: almanak.framework.execution.SwapAmounts
    options:
      heading_level: 3

::: almanak.framework.execution.LPCloseData
    options:
      heading_level: 3

## Signers

::: almanak.framework.execution.LocalKeySigner
    options:
      heading_level: 3

## Simulators

::: almanak.framework.execution.DirectSimulator
    options:
      heading_level: 3

::: almanak.framework.execution.TenderlySimulator
    options:
      heading_level: 3

## Receipt Parsing

::: almanak.framework.execution.ReceiptParserRegistry
    options:
      heading_level: 3

## Exceptions

::: almanak.framework.execution.ExecutionError
    options:
      heading_level: 3

::: almanak.framework.execution.SimulationError
    options:
      heading_level: 3

::: almanak.framework.execution.SigningError
    options:
      heading_level: 3
