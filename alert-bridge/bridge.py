"""
Alertmanager → Loki → Telegram bridge.

Receives Alertmanager v4 webhook payloads, fetches the actual log lines
that triggered each alert from Loki (using the alert's loki_query
annotation), and posts a single Telegram message per alert with the
real log content embedded.

Stdlib only — runs in a vanilla python image with the script mounted in.
"""

import json
import logging
import os
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100").rstrip("/")
PORT = int(os.environ.get("PORT", "8080"))
MAX_LINES = int(os.environ.get("MAX_LINES", "8"))
LOOKBACK_MINUTES = int(os.environ.get("LOOKBACK_MINUTES", "5"))
# Telegram caps a single message at 4096 chars; keep a margin for the
# wrapping HTML so the API never rejects.
TELEGRAM_MAX = 3800

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bridge")


def html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def query_loki(
    query: str, start_ns: int, end_ns: int, limit: int, direction: str = "backward"
) -> list[tuple[int, str]]:
    """Returns raw [(ts_ns, line), ...] from Loki, no sorting."""
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": str(start_ns),
            "end": str(end_ns),
            "limit": str(limit),
            "direction": direction,
        }
    )
    url = f"{LOKI_URL}/loki/api/v1/query_range?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
    except Exception as exc:
        log.warning("loki query failed for %r: %s", query, exc)
        return []
    rows: list[tuple[int, str]] = []
    for stream in data.get("data", {}).get("result", []):
        for ts_ns_str, line in stream.get("values", []):
            rows.append((int(ts_ns_str), line))
    return rows


def fetch_matches(match_query: str) -> list[tuple[str, str]]:
    """Returns up to MAX_LINES matching lines, oldest first."""
    end_ns = int(datetime.now(UTC).timestamp() * 1_000_000_000)
    start_ns = end_ns - LOOKBACK_MINUTES * 60 * 1_000_000_000
    rows = query_loki(match_query, start_ns, end_ns, limit=MAX_LINES)
    rows.sort()
    return [
        (datetime.fromtimestamp(ts / 1e9, tz=UTC).strftime("%H:%M:%S"), line)
        for ts, line in rows[-MAX_LINES:]
    ]


def format_message(alert: dict) -> str:
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})
    status = alert.get("status", "firing")
    emoji = "🔴" if status == "firing" else "🟢"
    name = labels.get("alertname", "Alert")
    container = labels.get("container", "?")

    head = f"{emoji} <b>{html_escape(name)}</b> in <code>{html_escape(container)}</code>"
    parts = [head]

    if status == "firing":
        query = annotations.get("loki_query")
        if query:
            matches = fetch_matches(query)
            if matches:
                rendered = "\n".join(f"{ts}  {line}" for ts, line in matches)
                if len(rendered) > TELEGRAM_MAX - 200:
                    rendered = rendered[-(TELEGRAM_MAX - 200) :]
                    rendered = "…\n" + rendered[rendered.find("\n") + 1 :]
                parts.append(f"<pre>{html_escape(rendered)}</pre>")
            else:
                parts.append(
                    "<i>(no matching log lines in the last "
                    f"{LOOKBACK_MINUTES} min — query may be too narrow)</i>"
                )
    else:
        parts.append("<i>resolved</i>")

    grafana = annotations.get("grafana_url")
    if grafana:
        parts.append(f'<a href="{html_escape(grafana)}">🔍 Open in Grafana</a>')

    return "\n".join(parts)


def send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    body = json.dumps(
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    ).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.error("telegram returned %s: %s", resp.status, resp.read()[:300])
    except urllib.error.HTTPError as exc:
        log.error("telegram HTTP %s: %s", exc.code, exc.read()[:300])
    except Exception as exc:
        log.error("telegram send failed: %s", exc)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return
        size = int(self.headers.get("Content-Length", "0"))
        try:
            payload = json.loads(self.rfile.read(size) or b"{}")
        except Exception as exc:
            log.warning("bad payload: %s", exc)
            self.send_response(400)
            self.end_headers()
            return
        for alert in payload.get("alerts", []):
            try:
                send_telegram(format_message(alert))
            except Exception as exc:
                log.exception("failed to handle alert: %s", exc)
        self.send_response(200)
        self.end_headers()


def main():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    log.info("alert-bridge listening on :%d", PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
