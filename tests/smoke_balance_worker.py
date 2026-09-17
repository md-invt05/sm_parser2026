"""Finite live smoke: two addresses, 20 networks, temporary DB; no block indexing.

Run explicitly: .venv/Scripts/python.exe tests/smoke_balance_worker.py
"""
import asyncio
import json
import logging
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import scan_defi as s


async def main():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = s.load_config(ROOT / "config.yaml")
    chains = {k: v for k, v in cfg.chains.items() if v.enabled}
    addresses = ["0xdac17f958d2ee523a2206206994597c13d831ec7",
                 "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]
    global_sem = asyncio.Semaphore(cfg.global_rpc_concurrency)
    pools = {}
    with tempfile.TemporaryDirectory(prefix="balance-smoke-", dir=ROOT) as folder:
        cfg = replace(cfg, db_path=Path(folder) / "smoke.db", export_dir=Path(folder) / "reports")
        db = s.DB(cfg.db_path)
        try:
            for chain in chains.values():
                # One candidate per network keeps this smoke small; production uses all fallbacks.
                pool = s.RpcPool(chain.key, chain.rpc[:1], cfg, chain.chain_id, global_sem)
                await pool.__aenter__()
                pools[chain.key] = pool
            reports = await asyncio.gather(*(pool.preflight() for pool in pools.values()))
            print("preflight_networks_ready", sum(any(r.get("ok") for r in rows) for rows in reports), flush=True)
            s.seed_tokens(db, s.load_token_seed(ROOT / "tokens.yaml"), list(chains))
            db.upsert_contracts([("ethereum", addr, None, None, None, "smoke") for addr in addresses])
            started = time.monotonic()
            await asyncio.wait_for(s.check_multichain_balances(
                db, chains, pools, s.PriceBook(cfg.http_timeout_sec, cfg.price_batch_size),
                cfg, cfg.min_usd, asyncio.Event(), once=True,
            ), timeout=120)
            print("elapsed_seconds", round(time.monotonic() - started, 2), flush=True)
            for row in db.latest_address_scans():
                estimate = db.rabby_estimate(row["address"])
                print(json.dumps({"address": row["address"], "rpc_status": row["status"],
                    "rpc_usd": row["total_usd"], "rpc_coverage": row["coverage"],
                    "rabby_status": estimate["status"] if estimate else None,
                    "rabby_usd": estimate["estimated_usd"] if estimate else None}), flush=True)
            assert db.conn.execute("SELECT COUNT(*) FROM address_scans").fetchone()[0] == 2
            assert db.conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            s.export_xlsx(db, cfg, chains, cfg.min_usd)
            print("smoke_ok; temporary DB and reports only", flush=True)
        finally:
            for pool in pools.values():
                await pool.__aexit__(None, None, None)
            db.close()


if __name__ == "__main__":
    asyncio.run(main())
