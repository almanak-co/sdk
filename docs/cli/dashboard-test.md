
# dashboard test

Test the dashboard locally using Streamlit.

## Usage

```
Usage: almanak dashboard test [OPTIONS]
```

## Arguments


## Options

* `working_dir`:
    * Type: `Path`
    * Default: `.`
    * Usage: `--working-dir`
    Working directory containing the strategy files. Defaults to the current directory.


* `local_storage`:
    * Type: `Path`
    * Default: `./local_storage`
    * Usage: `--local-storage`
    Path to the local storage directory for testing.


* `server_port`:
    * Type: INT
    * Default: `8501`
    * Usage: `--server-port`
    Port to run the Streamlit server on. Defaults to 8501.


* `preset` (REQUIRED):
    * Type: STRING
    * Default: `None`
    * Usage: `--preset`
    Preset to use for the dashboard.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak dashboard test [OPTIONS]

  Test the dashboard locally using Streamlit.

Options:
  --working-dir PATH     Working directory containing the strategy files.
                         Defaults to the current directory.
  --local-storage PATH   Path to the local storage directory for testing.
  --server-port INTEGER  Port to run the Streamlit server on. Defaults to
                         8501.
  --preset TEXT          Preset to use for the dashboard.  [required]
  --help                 Show this message and exit.
```

