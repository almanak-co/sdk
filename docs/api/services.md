# Services

Operational services for strategy monitoring, stuck detection, and emergency management.

## StuckDetector

Detects when a strategy is stuck and unable to make progress.

::: almanak.framework.services.StuckDetector
    options:
      show_root_heading: true
      members_order: source

## EmergencyManager

Handles emergency scenarios like position unwinding.

::: almanak.framework.services.EmergencyManager
    options:
      show_root_heading: true
      members_order: source

## OperatorCardGenerator

Generates operator cards for strategy issues.

::: almanak.framework.services.OperatorCardGenerator
    options:
      show_root_heading: true

## PredictionPositionMonitor

Monitors prediction market positions for resolution events.

::: almanak.framework.services.PredictionPositionMonitor
    options:
      show_root_heading: true

## AutoRedemptionService

Automatically redeems resolved prediction market positions.

::: almanak.framework.services.AutoRedemptionService
    options:
      show_root_heading: true

## Models

### StuckDetectionResult

::: almanak.framework.services.StuckDetectionResult
    options:
      show_root_heading: true

### StrategySnapshot

::: almanak.framework.services.StrategySnapshot
    options:
      show_root_heading: true

### EmergencyResult

::: almanak.framework.services.EmergencyResult
    options:
      show_root_heading: true

### FullPositionSummary

::: almanak.framework.services.FullPositionSummary
    options:
      show_root_heading: true
