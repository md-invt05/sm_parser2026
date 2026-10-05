"""Bounded, read-only OnFinality ListTransactions measurements.

No credentials, hostnames, transaction digests, or full error details are printed.
The probe never writes a database and stops after a fixed number of pages.
"""

import argparse
import asyncio
import json
import random
import time

from check_sui_grpc import safe_error_diagnostic, setting
from sui_grpc_wire_pb2 import (GetServiceInfoRequest,
                               QUERY_END_REASON_CHECKPOINT_BOUND)
from sui_support import SuiConfig, SuiGrpcClient


async def probe(client, start, count, limit, max_pages, retries, min_interval):
    started = time.monotonic()
    after = None
    requests = transactions = retry_count = 0
    terminal = None
    status = "OK"
    category = "none"
    rate_metadata = {}
    next_request_at = getattr(client, "_probe_next_at", time.monotonic())
    def result():
        return {"start": start, "end": start + count, "limit": limit,
                "requests": requests, "transactions": transactions,
                "duration_sec": round(time.monotonic() - started, 3),
                "terminal_checkpoint_bound": terminal == QUERY_END_REASON_CHECKPOINT_BOUND,
                "terminal_reason": terminal, "status": status,
                "error_category": category, "retries": retry_count,
                "rate_metadata": rate_metadata}
    for _ in range(max_pages):
        for attempt in range(retries + 1):
            wait = next_request_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            next_request_at = time.monotonic() + min_interval
            client._probe_next_at = next_request_at
            requests += 1
            try:
                cursor = None
                terminal = None
                page_transactions = 0
                async for frame in client.list_range(start, start + count, after):
                    page_transactions += int(frame.HasField("transaction"))
                    if frame.HasField("end"):
                        terminal = int(frame.end.reason)
                        cursor = bytes(frame.watermark.cursor)
                transactions += page_transactions
                status, category, rate_metadata = "OK", "none", {}
                break
            except Exception as exc:
                status, category, rate_metadata = safe_error_diagnostic(exc)
                if status != "UNAVAILABLE" or attempt == retries:
                    return result()
                retry_count += 1
                await asyncio.sleep(min(4.0, (2 ** attempt) + random.uniform(0, 0.5)))
        if terminal == QUERY_END_REASON_CHECKPOINT_BOUND:
            break
        if not cursor or cursor == after:
            status = "NON_ADVANCING_WATERMARK"
            break
        after = cursor
    else:
        status = "PAGE_CAP"

    return result()


async def main(offset, cases, max_pages, retries, sequential, min_interval):
    host, key = setting("SUI_GRPC_HOST"), setting("SUI_GRPC_API_KEY")
    if not host or not key:
        raise SystemExit("Sui gRPC credentials missing; values are never displayed")
    cfg = SuiConfig(timeout_sec=20, grpc_checkpoint_chunk=10,
                    grpc_stream_limit=500)
    client = SuiGrpcClient(key, cfg, endpoint=host)
    try:
        await client._pace_request()
        info = await client._info(GetServiceInfoRequest(), timeout=20,
                                  metadata=client.metadata)
        if info.chain.lower() != "mainnet":
            raise SystemExit("gRPC endpoint is not mainnet")
        end = int(info.checkpoint_height) - offset
        if end <= int(info.lowest_available_checkpoint) + 20:
            raise SystemExit("sample lies outside provider retention")
        for count, limit in cases:
            start = end - count
            client.cfg.grpc_stream_limit = limit
            row = await probe(client, start, count, limit, max_pages, retries,
                              min_interval)
            print(json.dumps(row, sort_keys=True), flush=True)
        if sequential:
            client.cfg.grpc_stream_limit = sequential[1]
            for checkpoint in range(end - sequential[0], end):
                row = await probe(client, checkpoint, 1, sequential[1],
                                  max_pages, retries, min_interval)
                row["sequential"] = True
                print(json.dumps(row, sort_keys=True), flush=True)
                if not row["terminal_checkpoint_bound"]:
                    break
    finally:
        await client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offset", type=int, default=200)
    parser.add_argument("--case", action="append", default=[],
                        help="checkpoint_count:page_limit, e.g. 1:10")
    parser.add_argument("--sequential", help="count:page_limit, e.g. 5:10")
    parser.add_argument("--max-pages", type=int, default=40)
    parser.add_argument("--unavailable-retries", type=int, default=1)
    parser.add_argument("--min-interval", type=float, default=0.05,
                        help="minimum seconds between ListTransactions pages")
    args = parser.parse_args()
    cases = [tuple(map(int, case.split(":"))) for case in args.case]
    sequential = tuple(map(int, args.sequential.split(":"))) if args.sequential else None
    if (not 2 <= args.offset <= 100_000 or not 1 <= args.max_pages <= 100 or
            not 0 <= args.unavailable_retries <= 2 or
            not 0.05 <= args.min_interval <= 10 or
            not (cases or sequential) or
            (sequential is not None and
             (not 1 <= sequential[0] <= 10 or not 1 <= sequential[1] <= 500)) or
            any(not 1 <= count <= 10 or not 1 <= limit <= 500
                for count, limit in cases)):
        parser.error("probe limits exceeded")
    asyncio.run(main(args.offset, cases, args.max_pages,
                     args.unavailable_retries, sequential, args.min_interval))
