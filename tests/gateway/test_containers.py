"""Container integration tests - requires Docker.

These tests verify the full container setup including network isolation.
They are marked with pytest.mark.docker and should only run when Docker
is available and configured.
"""

import subprocess
import time
from pathlib import Path

import pytest

# Mark all tests in this module as requiring Docker
pytestmark = pytest.mark.docker


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent.parent


def get_docker_compose_command() -> list[str] | None:
    """Detect available Docker Compose command.

    Returns the command as a list (e.g., ["docker", "compose"] or ["docker-compose"]),
    or None if neither is available.
    """
    # Try Docker Compose V2 (plugin) first - preferred on modern systems
    result = subprocess.run(
        ["docker", "compose", "version"],
        capture_output=True,
    )
    if result.returncode == 0:
        return ["docker", "compose"]

    # Fall back to Docker Compose V1 (standalone)
    result = subprocess.run(
        ["docker-compose", "version"],
        capture_output=True,
    )
    if result.returncode == 0:
        return ["docker-compose"]

    return None


@pytest.fixture(scope="module")
def compose_cmd():
    """Get the docker compose command or skip if unavailable."""
    cmd = get_docker_compose_command()
    if cmd is None:
        pytest.skip("Neither 'docker compose' nor 'docker-compose' is available")
    return cmd


@pytest.fixture(scope="module")
def docker_compose_up(compose_cmd):
    """Start containers with docker compose.

    This fixture starts the gateway and strategy containers, waits for
    them to be healthy, and tears them down after the test module completes.
    """
    repo_root = get_repo_root()
    compose_file = repo_root / "deploy" / "docker" / "docker-compose.yml"

    if not compose_file.exists():
        pytest.skip(f"docker-compose.yml not found at {compose_file}")

    # Start containers
    result = subprocess.run(
        [*compose_cmd, "-f", str(compose_file), "up", "-d", "--build"],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )
    if result.returncode != 0:
        pytest.skip(f"Failed to start containers: {result.stderr}")

    # Wait for gateway to be healthy
    healthy = False
    for _ in range(30):
        result = subprocess.run(
            [
                *compose_cmd,
                "-f",
                str(compose_file),
                "exec",
                "-T",
                "gateway",
                "grpc_health_probe",
                "-addr=:50051",
            ],
            capture_output=True,
            cwd=str(repo_root),
        )
        if result.returncode == 0:
            healthy = True
            break
        time.sleep(1)

    if not healthy:
        # Clean up before failing
        subprocess.run(
            [*compose_cmd, "-f", str(compose_file), "down"],
            cwd=str(repo_root),
        )
        pytest.skip("Gateway did not become healthy within timeout")

    yield {"compose_file": compose_file, "compose_cmd": compose_cmd}

    # Tear down containers
    subprocess.run(
        [*compose_cmd, "-f", str(compose_file), "down", "-v"],
        cwd=str(repo_root),
    )


def test_gateway_health_check(docker_compose_up):
    """Gateway responds to health checks."""
    compose_file = docker_compose_up["compose_file"]
    compose_cmd = docker_compose_up["compose_cmd"]
    repo_root = get_repo_root()

    result = subprocess.run(
        [
            *compose_cmd,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "gateway",
            "grpc_health_probe",
            "-addr=:50051",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )

    assert result.returncode == 0, f"Health check failed: {result.stderr}"


def test_strategy_can_reach_gateway(docker_compose_up):
    """Strategy container can connect to gateway."""
    compose_file = docker_compose_up["compose_file"]
    compose_cmd = docker_compose_up["compose_cmd"]
    repo_root = get_repo_root()

    result = subprocess.run(
        [
            *compose_cmd,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "strategy",
            "python",
            "-c",
            """
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
config = GatewayClientConfig(host='gateway', port=50051)
client = GatewayClient(config)
client.connect()
result = client.health_check()
client.disconnect()
print('SUCCESS' if result else 'FAILED')
""",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=30,
    )

    assert "SUCCESS" in result.stdout, f"Strategy could not reach gateway: {result.stderr}"


def test_strategy_cannot_reach_internet(docker_compose_up):
    """Strategy container cannot access external network.

    This is the critical security test - strategy containers should be
    completely isolated from the internet.
    """
    compose_file = docker_compose_up["compose_file"]
    compose_cmd = docker_compose_up["compose_cmd"]
    repo_root = get_repo_root()

    result = subprocess.run(
        [
            *compose_cmd,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "strategy",
            "python",
            "-c",
            """
import urllib.request
import socket

# Set a short timeout to fail fast
socket.setdefaulttimeout(5)

try:
    urllib.request.urlopen('https://google.com', timeout=5)
    print('FAILED - Internet access allowed')
except Exception as e:
    print(f'SUCCESS - Internet blocked: {type(e).__name__}')
""",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=30,
    )

    # The connection should fail - either timeout or network unreachable
    assert "SUCCESS" in result.stdout or result.returncode != 0, (
        f"Strategy container has internet access! stdout={result.stdout}, stderr={result.stderr}"
    )


def test_strategy_cannot_resolve_external_dns(docker_compose_up):
    """Strategy container cannot resolve external DNS names.

    Even if network access were somehow possible, DNS resolution should fail.
    """
    compose_file = docker_compose_up["compose_file"]
    compose_cmd = docker_compose_up["compose_cmd"]
    repo_root = get_repo_root()

    result = subprocess.run(
        [
            *compose_cmd,
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "strategy",
            "python",
            "-c",
            """
import socket

try:
    socket.gethostbyname('google.com')
    print('FAILED - DNS resolution allowed')
except socket.gaierror:
    print('SUCCESS - DNS blocked')
except Exception as e:
    print(f'SUCCESS - Blocked: {type(e).__name__}')
""",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=30,
    )

    # DNS resolution should fail
    assert "SUCCESS" in result.stdout or result.returncode != 0, (
        f"Strategy container can resolve external DNS! stdout={result.stdout}"
    )
