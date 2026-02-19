# Execution

The execution pipeline compiles intents, signs transactions, simulates, and submits them on-chain.

## GatewayExecutionOrchestrator

The primary orchestrator used when running with the gateway sidecar.

::: almanak.framework.execution.GatewayExecutionOrchestrator
    options:
      show_root_heading: true
      members_order: source

## ExecutionOrchestrator

::: almanak.framework.execution.ExecutionOrchestrator
    options:
      show_root_heading: true
      members_order: source

## ExecutionResult

::: almanak.framework.execution.ExecutionResult
    options:
      show_root_heading: true

## ExecutionContext

::: almanak.framework.execution.ExecutionContext
    options:
      show_root_heading: true

## Result Enrichment

After successful execution, `ResultEnricher` automatically extracts data from transaction receipts (position IDs, swap amounts, etc.) and attaches it to the result.

### ResultEnricher

::: almanak.framework.execution.ResultEnricher
    options:
      show_root_heading: true

### SwapAmounts

::: almanak.framework.execution.SwapAmounts
    options:
      show_root_heading: true

### LPCloseData

::: almanak.framework.execution.LPCloseData
    options:
      show_root_heading: true

## Signers

### LocalKeySigner

::: almanak.framework.execution.LocalKeySigner
    options:
      show_root_heading: true

## Simulators

### DirectSimulator

::: almanak.framework.execution.DirectSimulator
    options:
      show_root_heading: true

### TenderlySimulator

::: almanak.framework.execution.TenderlySimulator
    options:
      show_root_heading: true

## Receipt Parsing

### ReceiptParserRegistry

::: almanak.framework.execution.ReceiptParserRegistry
    options:
      show_root_heading: true

## Exceptions

::: almanak.framework.execution.ExecutionError
    options:
      show_root_heading: true

::: almanak.framework.execution.SimulationError
    options:
      show_root_heading: true

::: almanak.framework.execution.SigningError
    options:
      show_root_heading: true
