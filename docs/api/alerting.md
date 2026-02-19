# Alerting

Alert management with Slack and Telegram channels, cooldown tracking, and escalation policies.

## AlertManager

::: almanak.framework.alerting.AlertManager
    options:
      show_root_heading: true
      members_order: source

## GatewayAlertManager

::: almanak.framework.alerting.GatewayAlertManager
    options:
      show_root_heading: true

## Channels

### SlackChannel

::: almanak.framework.alerting.SlackChannel
    options:
      show_root_heading: true

### TelegramChannel

::: almanak.framework.alerting.TelegramChannel
    options:
      show_root_heading: true

## Configuration

### AlertConfig

::: almanak.framework.alerting.AlertConfig
    options:
      show_root_heading: true

### AlertRule

::: almanak.framework.alerting.AlertRule
    options:
      show_root_heading: true

### AlertChannel

::: almanak.framework.alerting.AlertChannel
    options:
      show_root_heading: true

## Escalation

### EscalationPolicy

::: almanak.framework.alerting.EscalationPolicy
    options:
      show_root_heading: true

### EscalationLevel

::: almanak.framework.alerting.EscalationLevel
    options:
      show_root_heading: true

## Results

### AlertSendResult

::: almanak.framework.alerting.AlertSendResult
    options:
      show_root_heading: true
