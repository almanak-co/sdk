
# almanak dashboard

Start the Almanak Operator Dashboard.

The dashboard provides a web UI for monitoring and managing strategies.
It connects to the gateway for all data access.

The gateway must be running before starting the dashboard.
Start the gateway first with: `almanak gateway`

## Usage

```
Usage: almanak dashboard [OPTIONS]
```

## Options

* `port`:
    * Type: INT
    * Default: `8501`
    * Env: `DASHBOARD_PORT`
    * Usage: `--port`
    Streamlit port number (default: 8501).

* `gateway_host`:
    * Type: STRING
    * Default: `localhost`
    * Env: `GATEWAY_HOST`
    * Usage: `--gateway-host`
    Gateway hostname (default: localhost).

* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Env: `GATEWAY_PORT`
    * Usage: `--gateway-port`
    Gateway gRPC port (default: 50051).

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

  The dashboard provides a web UI for monitoring and managing strategies.
  It connects to the gateway for all data access.

  IMPORTANT: A gateway must be running before starting the dashboard.
  Start a standalone gateway with: almanak gateway

  Examples:

      # Start gateway, then dashboard
      almanak gateway &
      almanak dashboard

      # Start dashboard on custom port
      almanak dashboard --port 8502

      # Connect to remote gateway
      almanak dashboard --gateway-host 192.168.1.100 --gateway-port 50051

Options:
  --port INTEGER          Streamlit port number (default: 8501).
  --gateway-host TEXT     Gateway hostname (default: localhost).
  --gateway-port INTEGER  Gateway gRPC port (default: 50051).
  --no-browser            Don't open browser automatically.
  --help                  Show this message and exit.
```
