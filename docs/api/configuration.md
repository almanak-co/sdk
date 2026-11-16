# Configuration API

Public author inputs parse once into `AuthorInputs`; resolution binds them to an
operator configuration and retained market facts before parameters exist.

::: almanak.configuration.unresolved

::: almanak.configuration.parameters

Backtest inputs resolve against those same parameters and explicit operator
provider bindings.

::: almanak.configuration.backtest_inputs

Runtime artifacts use the resolved parameter codecs. Archived author fixtures are
private evidence inputs under `scripts/legacy_configuration/`; they are not an SDK
configuration API.
