"""Berth against nginx, on the same machine, with the same backends.

The brief for this project said to be honest when nginx wins. It will. The
question worth answering is by how much, under what conditions, and why.

Fairness rules, because a proxy benchmark is easy to rig in either direction:

* **The backend is never the bottleneck.** Both proxies forward to nginx
  serving a fixed body, which answers far faster than either proxy can.
* **Cores are pinned.** Berth is one Python process on one core, so the main
  comparison is nginx with one worker on that same core. nginx with four workers
  is reported separately, because that is how nginx is actually deployed and
  pretending otherwise would flatter Berth.
* **The load generator gets its own cores**, so it is not stealing time from the
  thing it is measuring.
* **The direct row** is wrk against a backend with no proxy at all, which is
  the ceiling neither proxy can exceed on this hardware.

    python -m bench.versus_nginx --duration 15 --connections 64
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent

PROXY_CPU = "2"
BACKEND_CPUS = "6,7"
WRK_CPUS = "4,5"
NGINX_MULTI_CPUS = "0,1,2,3"


def sh(*args: str, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(list(args), check=check, capture_output=True, text=True, **kwargs)


def wait_for_port(port: int, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1).read()
            return
        except Exception:  # noqa: BLE001 - still starting
            time.sleep(0.2)
    raise RuntimeError(f"nothing answering on port {port}")


def start_nginx(name: str, conf: str, cpus: str) -> None:
    sh("docker", "rm", "-f", name, check=False)
    # Inside the project rather than the system temp directory: this Docker
    # install cannot bind-mount files from /tmp.
    run_dir = HERE / ".run" / name
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "nginx.conf"
    path.write_text(conf)
    os.chmod(path.parent, 0o755)
    os.chmod(path, 0o644)
    sh("docker", "run", "-d", "--name", name, "--network", "host", "--cpuset-cpus", cpus,
       "-v", f"{path}:/etc/nginx/nginx.conf:ro", "nginx:alpine")


def wrk(port: int, duration: int, connections: int, threads: int) -> dict:
    result = sh("docker", "run", "--rm", "--network", "host", "--cpuset-cpus", WRK_CPUS,
                "williamyeh/wrk", f"-t{threads}", f"-c{connections}", f"-d{duration}s",
                "--latency", f"http://127.0.0.1:{port}/", timeout=duration + 60)
    return parse_wrk(result.stdout)


_UNIT = {"us": 0.001, "ms": 1.0, "s": 1000.0}


def _ms(text: str) -> float:
    match = re.fullmatch(r"([\d.]+)(us|ms|s)", text.strip())
    return round(float(match.group(1)) * _UNIT[match.group(2)], 3) if match else float("nan")


def parse_wrk(output: str) -> dict:
    parsed: dict[str, object] = {"raw": output}
    rps = re.search(r"Requests/sec:\s+([\d.]+)", output)
    parsed["requests_per_s"] = round(float(rps.group(1))) if rps else None
    for label, key in (("50%", "p50_ms"), ("75%", "p75_ms"), ("90%", "p90_ms"), ("99%", "p99_ms")):
        match = re.search(rf"^\s*{re.escape(label)}\s+(\S+)", output, re.MULTILINE)
        parsed[key] = _ms(match.group(1)) if match else None
    errors = re.search(r"Non-2xx or 3xx responses:\s+(\d+)", output)
    socket_errors = re.search(r"Socket errors:.*", output)
    parsed["non_2xx"] = int(errors.group(1)) if errors else 0
    parsed["socket_errors"] = socket_errors.group(0) if socket_errors else None
    return parsed


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="bench.versus_nginx", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--duration", type=int, default=15)
    p.add_argument("--connections", type=int, default=64)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    template = (HERE / "nginx-proxy.conf").read_text()
    berth = None
    rows: list[tuple[str, dict]] = []
    try:
        start_nginx("berth-bench-backend", (HERE / "nginx-backend.conf").read_text(), BACKEND_CPUS)
        wait_for_port(9101)
        wait_for_port(9102)

        start_nginx("berth-bench-nginx1",
                    template.replace("WORKERS", "1").replace("PORT", "9201"), PROXY_CPU)
        start_nginx("berth-bench-nginx4",
                    template.replace("WORKERS", "4").replace("PORT", "9204"), NGINX_MULTI_CPUS)
        wait_for_port(9201)
        wait_for_port(9204)

        berth = subprocess.Popen(
            ["taskset", "-c", PROXY_CPU, sys.executable, "-m", "berth.cli",
             "--backend", "127.0.0.1:9101", "--backend", "127.0.0.1:9102",
             "--listen", "127.0.0.1:9300", "--admin-port", "9301",
             "--health-path", "/"],
            cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for_port(9300)

        plan = [
            ("direct to backend, no proxy", 9101),
            ("nginx, 1 worker on 1 core", 9201),
            ("berth, 1 process on 1 core", 9300),
            ("nginx, 4 workers on 4 cores", 9204),
        ]
        for label, port in plan:
            # A short warm-up so connection pools are full before the clock runs.
            wrk(port, 3, args.connections, args.threads)
            rows.append((label, wrk(port, args.duration, args.connections, args.threads)))
            print(f"measured: {label}", file=sys.stderr)
    finally:
        if berth is not None:
            berth.send_signal(signal.SIGTERM)
            try:
                berth.wait(timeout=10)
            except subprocess.TimeoutExpired:
                berth.kill()
        for name in ("berth-bench-backend", "berth-bench-nginx1", "berth-bench-nginx4"):
            sh("docker", "rm", "-f", name, check=False)

    if args.json:
        print(json.dumps([{"setup": label, **{k: v for k, v in r.items() if k != "raw"}}
                          for label, r in rows], indent=2))
        return 0

    berth_rps = next(r["requests_per_s"] for label, r in rows if label.startswith("berth"))
    print(f"\nwrk -t{args.threads} -c{args.connections} -d{args.duration}s, 3 byte body, "
          f"2 backends\n")
    print(f"{'setup':<30} {'req/s':>9} {'p50':>9} {'p90':>9} {'p99':>9} {'vs berth':>9}")
    for label, r in rows:
        ratio = r["requests_per_s"] / berth_rps if berth_rps else float("nan")
        print(f"{label:<30} {r['requests_per_s']:>9,} {r['p50_ms']:>7}ms "
              f"{r['p90_ms']:>7}ms {r['p99_ms']:>7}ms {ratio:>8.1f}x")
        if r["non_2xx"] or r["socket_errors"]:
            print(f"{'':<30} errors: non-2xx {r['non_2xx']}, {r['socket_errors']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
