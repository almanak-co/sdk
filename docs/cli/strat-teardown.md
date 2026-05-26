
# strat teardown

Manage strategy teardowns.

    The teardown system allows safely closing all positions for a strategy.
    Teardowns can be initiated via CLI, dashboard, config, or risk guards.
    

## Usage

```
Usage: almanak strat teardown [OPTIONS] COMMAND [ARGS]...
```

## Arguments


## Options

* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat teardown [OPTIONS] COMMAND [ARGS]...

  Manage strategy teardowns.

  The teardown system allows safely closing all positions for a strategy.
  Teardowns can be initiated via CLI, dashboard, config, or risk guards.

Options:
  --help  Show this message and exit.

Commands:
  cancel   Cancel a pending or in-progress teardown.
  execute  Execute teardown directly from a strategy working directory.
  list     List teardown requests in the strategy-folder DB.
  request  Request a teardown for a strategy.
  status   Check teardown status for a strategy.
```

