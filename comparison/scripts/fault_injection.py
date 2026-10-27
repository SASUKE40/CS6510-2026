#!/usr/bin/env python3
"""Single-component crash experiment for every architecture.

For each architecture this starts every process on fresh databases (and, for
the event-based system, an isolated topic prefix on the existing Kafka broker).
Then, for each component in turn, it:

1. prepares an open basket with one scanned unit;
2. crashes the component (SIGKILL, or `docker kill` for the broker);
3. probes the seven public endpoints through the client-facing port;
4. restarts the component and records the time until it is healthy and until
   a full start/scan/complete checkout succeeds again;
5. after the system settles, records the final state of the basket whose
   completion was attempted during the outage.

Requires the release binaries of every architecture and, for the event-based
system, a Kafka broker from event-based/docker-compose.yml.

Usage: python3 comparison/scripts/fault_injection.py [arch ...]
Writes comparison/data/fault_injection.json.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "comparison" / "data" / "fault_injection.json"
BASE = "http://127.0.0.1:8080"
COMPOSE = ROOT / "event-based" / "docker-compose.yml"
PROBE_TIMEOUT = 15
SETTLE_SECONDS = 12

ENDPOINTS = ["GET /items", "POST /transactions", "POST /transactions/{id}/items",
             "POST /transactions/{id}/complete", "GET /transactions/{id}",
             "GET /inventory/low-stock", "GET /analytics/popular-items"]


def request(method, path, body=None, timeout=PROBE_TIMEOUT):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read()
            status = resp.status
    except urllib.error.HTTPError as err:
        payload, status = err.read(), err.code
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as err:
        reason = getattr(err, "reason", err)
        status = "timeout" if "timed out" in str(reason) else "refused"
        payload = b""
    elapsed = (time.monotonic() - started) * 1000
    try:
        parsed = json.loads(payload) if payload else None
    except json.JSONDecodeError:
        parsed = None
    return status, elapsed, parsed


def wait_http(url, deadline=60):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status < 500:
                    return True
        except Exception:
            pass
        time.sleep(0.05)
    return False


class Component:
    def __init__(self, name, argv=None, health=None, docker=False):
        self.name, self.argv, self.health, self.docker = name, argv, health, docker
        self.proc = None

    def start(self, logdir, first=False):
        if self.docker:
            subprocess.run(["docker", "compose", "-f", str(COMPOSE), "start", "kafka"], check=True, capture_output=True)
            subprocess.run(["docker", "compose", "-f", str(COMPOSE), "up", "-d", "--wait"], check=True, capture_output=True)
            return
        argv = [a for a in self.argv if first or not a.startswith("--reset")]
        log = open(Path(logdir) / f"{self.name}.log", "a")
        self.proc = subprocess.Popen(argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        if self.health and not wait_http(self.health, 90):
            raise RuntimeError(f"{self.name} did not become healthy")

    def crash(self):
        if self.docker:
            subprocess.run(["docker", "compose", "-f", str(COMPOSE), "kill", "kafka"], check=True, capture_output=True)
            return
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def systems(data):
    def single(binary):
        return [Component("server", [f"{binary}", "--port=8080", f"--db={data}/checkout.sqlite", "--reset"],
                          f"{BASE}/items")]

    def services(arch, extra, dbs):
        b = f"{arch}/target/release"
        ports = {"inventory-service": 8081, "basket-service": 8082, "payment-service": 8083, "analytics-service": 8084}
        comps = []
        for name, port in ports.items():
            argv = [f"{b}/{name}", f"--port={port}"]
            argv += [f"--db={data}/{name.split('-')[0]}.sqlite" if dbs else f"--db={data}/checkout.sqlite"]
            if dbs:
                argv.append("--reset=true")
            argv += extra.get(name, [])
            comps.append(Component(name, argv, f"http://127.0.0.1:{port}/health"))
        comps.append(Component("checkout-gateway", [f"{b}/checkout-gateway", "--port=8080",
                                                    "--inventory=http://127.0.0.1:8081", "--basket=http://127.0.0.1:8082",
                                                    "--payment=http://127.0.0.1:8083", "--analytics=http://127.0.0.1:8084"],
                               f"{BASE}/health"))
        return comps

    inv, bas = "--inventory=http://127.0.0.1:8081", "--basket=http://127.0.0.1:8082"
    bus = ["--kafka=127.0.0.1:9092", f"--topic-prefix=faults{int(time.time())}"]
    return {
        "monolith": (single("monolith/target/release/checkout-monolith"), None),
        "layered": (single("layered/target/release/checkout-layered"), None),
        "pipeline": (single("pipeline/target/release/checkout-pipeline"), None),
        "service-based": (services("service-based", {}, dbs=False),
                          ["service-based/target/release/checkout-db-admin", f"--db={data}/checkout.sqlite", "--reset=true"]),
        "microservices": (services("microservices", {"basket-service": [inv], "payment-service": [bas, inv],
                                                     "analytics-service": [bas]}, dbs=True), None),
        "event-based": ([Component("kafka-broker", docker=True)]
                        + services("event-based", {n: bus for n in ("inventory-service", "basket-service",
                                                                     "payment-service", "analytics-service")}, dbs=True),
                        None),
    }


def open_basket(sku="SKU-000100"):
    status, _, tx = request("POST", "/transactions", {"stationId": "fault-probe"})
    if status != 201 and status != 200:
        raise RuntimeError(f"could not start a probe transaction: {status} {tx}")
    tid = tx["transactionId"]
    status, _, _ = request("POST", f"/transactions/{tid}/items", {"sku": sku})
    if status not in (200, 201):
        raise RuntimeError(f"could not scan into the probe transaction: {status}")
    return tid


def probe(tid, other):
    out = {}
    for ep in ENDPOINTS:
        if ep == "GET /items":
            r = request("GET", "/items")
        elif ep == "POST /transactions":
            r = request("POST", "/transactions", {"stationId": "fault-probe"})
        elif ep == "POST /transactions/{id}/items":
            r = request("POST", f"/transactions/{tid}/items", {"sku": "SKU-000100"})
        elif ep == "POST /transactions/{id}/complete":
            r = request("POST", f"/transactions/{tid}/complete")
        elif ep == "GET /transactions/{id}":
            r = request("GET", f"/transactions/{other}")
        elif ep == "GET /inventory/low-stock":
            r = request("GET", "/inventory/low-stock")
        else:
            r = request("GET", "/analytics/popular-items")
        status, ms, _ = r
        out[ep] = {"status": status, "ms": round(ms, 1), "ok": isinstance(status, int) and status < 300}
    return out


def full_checkout_ok():
    try:
        tid = open_basket("SKU-000200")
    except RuntimeError:
        return False
    status, _, _ = request("POST", f"/transactions/{tid}/complete", timeout=5)
    return status == 200


def run(arch):
    data = tempfile.mkdtemp(prefix=f"faults-{arch}-")
    comps, admin = systems(data)[arch]
    print(f"== {arch} (data in {data})", flush=True)
    if admin:
        subprocess.run(admin, cwd=ROOT, check=True, capture_output=True)
    for c in comps:
        c.start(data, first=True)
    wait_http(f"{BASE}/items", 60)
    tid, other = open_basket(), open_basket()
    results = {"baseline": probe(tid, other), "failures": {}}
    print("  baseline:", sum(v["ok"] for v in results["baseline"].values()), "/ 7", flush=True)
    try:
        for c in comps:
            tid, other = open_basket(), open_basket()
            c.crash()
            time.sleep(1)
            probes = probe(tid, other)
            restart_at = time.monotonic()
            c.start(data)
            healthy_ms = (time.monotonic() - restart_at) * 1000
            recovered_ms = None
            while time.monotonic() - restart_at < 90:
                if full_checkout_ok():
                    recovered_ms = (time.monotonic() - restart_at) * 1000
                    break
                time.sleep(0.25)
            time.sleep(SETTLE_SECONDS)
            _, _, final = request("GET", f"/transactions/{tid}")
            results["failures"][c.name] = {
                "probes": probes,
                "available": sum(v["ok"] for v in probes.values()),
                "restart_to_healthy_ms": round(healthy_ms),
                "restart_to_checkout_ms": round(recovered_ms) if recovered_ms is not None else None,
                "probe_basket_final_status": (final or {}).get("status"),
            }
            r = results["failures"][c.name]
            print(f"  {c.name:18} up={r['available']}/7 healthy={r['restart_to_healthy_ms']}ms "
                  f"checkout={r['restart_to_checkout_ms']}ms final={r['probe_basket_final_status']} "
                  + " ".join(f"{k.split()[-1]}={v['status']}" for k, v in probes.items()), flush=True)
    finally:
        for c in reversed(comps):
            if not c.docker:
                c.stop()
    return results


def main():
    archs = sys.argv[1:] or ["monolith", "layered", "pipeline", "service-based", "microservices", "event-based"]
    existing = json.loads(OUT.read_text()) if OUT.exists() else {}
    for arch in archs:
        existing[arch] = run(arch)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(existing, indent=2))


if __name__ == "__main__":
    os.chdir(ROOT)
    main()
