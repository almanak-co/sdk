
# almanak dashboard

Start the Almanak Operator Dashboard.

    The dashboard provides a web UI for monitoring and managing strategies.
    It connects to the gateway for all data access.

    IMPORTANT: The gateway must be running before starting the dashboard.
    Start the gateway first with: almanak gateway

    Examples:

    
        # Start dashboard (gateway must be running)
        almanak gateway &  # Terminal 1
        almanak dashboard  # Terminal 2

    
        # Start dashboard on custom port
        almanak dashboard --port 8502

    
        # Connect to remote gateway
        almanak dashboard --gateway-host 192.168.1.100 --gateway-port 50051
    

## Usage

```
Usage: almanak dashboard [OPTIONS]
```

## Arguments


## Options

* `port`:
    * Type: INT
    * Default: `8501`
    * Usage: `--port`
    Streamlit port number (default: 8501).


* `gateway_host`:
    * Type: STRING
    * Default: `127.0.0.1`
    * Usage: `--gateway-host`
    Gateway gRPC host.


* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Usage: `--gateway-port`
    Gateway gRPC port.


* `no_browser`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--no-browser`
    Don't open browser automatically.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak dashboard [OPTIONS]

  Start the Almanak Operator Dashboard.

  The dashboard provides a web UI for monitoring and managing strategies. It
  connects to the gateway for all data access.

  IMPORTANT: The gateway must be running before starting the dashboard. Start
  the gateway first with: almanak gateway

  Examples:

      # Start dashboard (gateway must be running)
      almanak gateway &  # Terminal 1
      almanak dashboard  # Terminal 2

      # Start dashboard on custom port
      almanak dashboard --port 8502

      # Connect to remote gateway
      almanak dashboard --gateway-host 192.168.1.100 --gateway-port 50051

Options:
  --port INTEGER          Streamlit port number (default: 8501).
  --gateway-host TEXT     Gateway gRPC host.  [env var: ALMANAK_GATEWAY_HOST,
                          GATEWAY_HOST; default: 127.0.0.1]
  --gateway-port INTEGER  Gateway gRPC port.  [env var: ALMANAK_GATEWAY_PORT,
                          GATEWAY_PORT; default: 50051]
  --no-browser            Don't open browser automatically.
  --help                  Show this message and exit.
```

