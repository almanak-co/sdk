
# strat example

Download the example/tutorial strategies.

## Usage

```
Usage: almanak strat example [OPTIONS]
```

## Arguments


## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir`
    Working directory to download the example strategy. Defaults to the current directory.


* `strategy_name`:
    * Type: Choice(['tutorial_uniswap_swap', 'tutorial_hello_world'])
    * Default: `tutorial_uniswap_swap`
    * Usage: `--strategy-name`
    The name of the example strategy to download. Defaults to 'tutorial_uniswap_swap'.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat example [OPTIONS]

  Download the example/tutorial strategies.

Options:
  --working-dir PATH              Working directory to download the example
                                  strategy. Defaults to the current directory.
  --strategy-name [tutorial_uniswap_swap|tutorial_hello_world]
                                  The name of the example strategy to
                                  download. Defaults to
                                  'tutorial_uniswap_swap'.
  --help                          Show this message and exit.
```

