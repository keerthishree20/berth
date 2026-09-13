"""Command line interface.

    berth --backend 127.0.0.1:9001 --backend 127.0.0.1:9002
    berth --config berth.json
    berth --backend 127.0.0.1:9001 --strategy consistent_hash --hash-header x-session-id

Stats are served on the admin port at /stats (JSON) and /metrics (Prometheus).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from .balancer import STRATEGIES
from .config import Config
from .proxy import Proxy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="berth", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", help="a JSON configuration file")
    parser.add_argument("--backend", action="append", default=[],
                        help="host:port, repeat for each backend")
    parser.add_argument("--listen", default="127.0.0.1:8080", help="host:port to accept on")
    parser.add_argument("--admin-port", type=int, default=8081,
                        help="port for /stats and /metrics; 0 disables")
    parser.add_argument("--strategy", choices=STRATEGIES, default="least_connections")
    parser.add_argument("--hash-header", default=None,
                        help="key consistent hashing on this header instead of client address")
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--health-path", default="/health")
    parser.add_argument("--access-log", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def load_config(args) -> Config:
    if args.config:
        return Config.from_file(args.config)
    if not args.backend:
        raise SystemExit("give at least one --backend host:port, or --config")

    host, _, port = args.listen.rpartition(":")
    return Config.from_addresses(
        args.backend,
        listen_host=host or "127.0.0.1",
        listen_port=int(port),
        admin_port=args.admin_port or None,
        strategy=args.strategy,
        hash_key="header" if args.hash_header else "client_ip",
        hash_header=(args.hash_header or "x-session-id").lower(),
        retries=args.retries,
        health_path=args.health_path,
        access_log=args.access_log,
    )


async def run(config: Config) -> None:
    proxy = Proxy(config)
    await proxy.start()
    print(f"berth listening on {config.listen_host}:{proxy.port}, "
          f"{len(proxy.backends)} backends, strategy {config.strategy}", file=sys.stderr)
    if proxy.admin_port:
        print(f"stats on http://{config.listen_host}:{proxy.admin_port}/stats", file=sys.stderr)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    await stop.wait()
    await proxy.stop()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-5s %(name)s %(message)s")
    try:
        config = load_config(args)
    except ValueError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
