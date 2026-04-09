#!/usr/bin/env python3
"""Load test for POST /api/v1/ping endpoint using httpx async.

Standalone script — does NOT run as part of pytest. Requires a running
server instance (docker-compose up).

Usage:
    python tests/load/ping_load_test.py [OPTIONS]

Options (via environment variables):
    LOAD_TEST_HOST          Target host (default: http://localhost:8000)
    LOAD_TEST_USERS         Concurrent users (default: 50)
    LOAD_TEST_DURATION      Duration in seconds (default: 60)
    LOAD_TEST_RAMP          Users added per second (default: 10)

Output:
    Prints a summary table with RPS, p50/p95/p99 latency, and error rate.
"""

import asyncio
import logging
import os
import statistics
import time
from dataclasses import dataclass, field

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configuration
HOST = os.getenv("LOAD_TEST_HOST", "http://localhost:8000")
NUM_USERS = int(os.getenv("LOAD_TEST_USERS", "50"))
DURATION_SECONDS = int(os.getenv("LOAD_TEST_DURATION", "60"))
RAMP_RATE = int(os.getenv("LOAD_TEST_RAMP", "10"))

PING_URL = "/api/v1/ping"
REGISTER_URL = "/api/v1/auth/register"
LOGIN_URL = "/api/v1/auth/login"

PING_PAYLOADS = [
    # Minimal ping
    {
        "timezone": "America/New_York",
        "app_version": "1.0.0",
    },
    # Ping with health
    {
        "timezone": "America/New_York",
        "app_version": "1.0.0",
        "device_health": {
            "accessibility_enabled": True,
            "lyft_running": True,
            "screen_on": True,
        },
    },
    # Ping with stats
    {
        "timezone": "America/New_York",
        "app_version": "1.0.0",
        "stats": {
            "batch_id": "load-test-batch",
            "cycles_since_last_ping": 5,
            "rides_found": 2,
        },
        "last_cycle_duration_ms": 15000,
    },
    # Full ping
    {
        "timezone": "America/New_York",
        "app_version": "1.0.0",
        "device_health": {
            "accessibility_enabled": True,
            "lyft_running": True,
            "screen_on": True,
        },
        "stats": {
            "batch_id": "load-test-batch-full",
            "cycles_since_last_ping": 10,
            "rides_found": 3,
            "accept_failures": [
                {
                    "reason": "price_too_low",
                    "ride_price": 8.50,
                    "timestamp": "2026-01-01T12:00:00Z",
                }
            ],
        },
        "last_cycle_duration_ms": 14500,
        "location": {
            "latitude": 40.7128,
            "longitude": -74.0060,
        },
    },
]


@dataclass
class RequestResult:
    status_code: int
    latency_ms: float
    success: bool


@dataclass
class LoadTestResults:
    results: list[RequestResult] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def total_requests(self) -> int:
        return len(self.results)

    @property
    def successful(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return self.total_requests - self.successful

    @property
    def error_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.failed / self.total_requests) * 100

    @property
    def rps(self) -> float:
        if self.duration == 0:
            return 0.0
        return self.total_requests / self.duration

    @property
    def latencies(self) -> list[float]:
        return sorted(r.latency_ms for r in self.results if r.success)

    def percentile(self, p: float) -> float:
        lats = self.latencies
        if not lats:
            return 0.0
        k = (len(lats) - 1) * p
        f = int(k)
        c = f + 1
        if c >= len(lats):
            return lats[-1]
        return lats[f] + (k - f) * (lats[c] - lats[f])

    def summary(self) -> str:
        lats = self.latencies
        return (
            "\n"
            "╔══════════════════════════════════════════╗\n"
            "║     PING ENDPOINT LOAD TEST RESULTS      ║\n"
            "╠══════════════════════════════════════════╣\n"
            f"║ Duration:         {self.duration:>8.1f}s              ║\n"
            f"║ Concurrent users: {NUM_USERS:>8d}               ║\n"
            f"║ Total requests:   {self.total_requests:>8d}               ║\n"
            f"║ Successful:       {self.successful:>8d}               ║\n"
            f"║ Failed:           {self.failed:>8d}               ║\n"
            f"║ Error rate:       {self.error_rate:>7.1f}%               ║\n"
            "╠══════════════════════════════════════════╣\n"
            f"║ RPS:              {self.rps:>8.1f}               ║\n"
            f"║ Min latency:      {(min(lats) if lats else 0):>7.1f}ms              ║\n"
            f"║ p50 latency:      {self.percentile(0.50):>7.1f}ms              ║\n"
            f"║ p95 latency:      {self.percentile(0.95):>7.1f}ms              ║\n"
            f"║ p99 latency:      {self.percentile(0.99):>7.1f}ms              ║\n"
            f"║ Max latency:      {(max(lats) if lats else 0):>7.1f}ms              ║\n"
            f"║ Mean latency:     {(statistics.mean(lats) if lats else 0):>7.1f}ms              ║\n"
            f"║ Stdev:            {(statistics.stdev(lats) if len(lats) > 1 else 0):>7.1f}ms              ║\n"
            "╚══════════════════════════════════════════╝\n"
        )

    def status_breakdown(self) -> str:
        counts: dict[int, int] = {}
        for r in self.results:
            counts[r.status_code] = counts.get(r.status_code, 0) + 1
        lines = ["Status code breakdown:"]
        for code in sorted(counts):
            lines.append(f"  {code}: {counts[code]}")
        return "\n".join(lines)


async def setup_user(client: httpx.AsyncClient, user_id: int) -> tuple[str | None, str]:
    """Register and login a test user, return (token, device_id)."""
    email = f"loadtest-{user_id}@test.local"
    password = "LoadTest123!"
    device_id = f"load-device-{user_id}"

    # Register (ignore 409 if exists)
    await client.post(REGISTER_URL, json={"email": email, "password": password})

    # Login
    resp = await client.post(LOGIN_URL, json={"email": email, "password": password})
    token = None
    if resp.status_code == 200:
        token = resp.json().get("access_token")
    return token, device_id


async def ping_worker(
    client: httpx.AsyncClient,
    token: str | None,
    device_id: str,
    results: LoadTestResults,
    stop_event: asyncio.Event,
):
    """Send pings in a loop until stop_event is set."""
    headers: dict[str, str] = {"X-Device-ID": device_id}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    payload_idx = 0
    while not stop_event.is_set():
        payload = PING_PAYLOADS[payload_idx % len(PING_PAYLOADS)]
        # Vary batch_id to avoid dedup
        body = {**payload}
        if "stats" in body:
            body["stats"] = {**body["stats"], "batch_id": f"lt-{time.monotonic_ns()}"}

        start = time.monotonic()
        try:
            resp = await client.post(PING_URL, json=body, headers=headers)
            latency = (time.monotonic() - start) * 1000
            results.results.append(
                RequestResult(
                    status_code=resp.status_code,
                    latency_ms=latency,
                    success=200 <= resp.status_code < 300,
                )
            )
        except httpx.HTTPError:
            latency = (time.monotonic() - start) * 1000
            results.results.append(RequestResult(status_code=0, latency_ms=latency, success=False))

        payload_idx += 1
        # Small delay to simulate real device behavior (1-3s between pings)
        await asyncio.sleep(1.0 + (payload_idx % 3) * 0.5)


async def run_load_test() -> LoadTestResults:
    """Run the load test with gradual user ramp-up."""
    results = LoadTestResults()
    stop_event = asyncio.Event()

    logger.info(
        "Starting load test: %d users, %ds duration, ramp %d/s, target: %s",
        NUM_USERS,
        DURATION_SECONDS,
        RAMP_RATE,
        HOST,
    )

    async with httpx.AsyncClient(base_url=HOST, timeout=30.0) as client:
        # Setup users
        logger.info("Setting up %d test users...", NUM_USERS)
        user_configs = []
        for i in range(NUM_USERS):
            token, device_id = await setup_user(client, i)
            user_configs.append((token, device_id))
        logger.info("User setup complete.")

        # Start workers with ramp-up
        results.start_time = time.monotonic()
        tasks = []
        for i, (token, device_id) in enumerate(user_configs):
            task = asyncio.create_task(ping_worker(client, token, device_id, results, stop_event))
            tasks.append(task)

            # Ramp-up delay
            if (i + 1) % RAMP_RATE == 0 and i < NUM_USERS - 1:
                await asyncio.sleep(1.0)

        logger.info("All %d workers started. Running for %ds...", NUM_USERS, DURATION_SECONDS)

        # Wait for duration
        await asyncio.sleep(DURATION_SECONDS)
        stop_event.set()

        # Wait for all workers to finish current request
        await asyncio.gather(*tasks, return_exceptions=True)
        results.end_time = time.monotonic()

    return results


def main():
    results = asyncio.run(run_load_test())
    print(results.summary())
    print(results.status_breakdown())


if __name__ == "__main__":
    main()
