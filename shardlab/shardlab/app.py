"""沙盘启动入口：python -m shardlab.app [--port 18080]"""
from __future__ import annotations

import argparse

from .db import init_db
from .seed import seed
from .server import serve


def main() -> None:
    parser = argparse.ArgumentParser(description="在线分片拆分沙盘 shardlab")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--fresh", action="store_true", help="启动前清空数据并重新播种")
    args = parser.parse_args()
    init_db()
    seed(reset=args.fresh)
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
