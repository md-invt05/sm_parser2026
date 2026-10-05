"""Read-only Sui gRPC preflight; never prints credentials or opens a database."""

import asyncio
import argparse
import os
import re
import time
from pathlib import Path

from sui_support import SuiConfig, SuiGrpcClient
from sui_grpc_wire_pb2 import GetServiceInfoRequest, QUERY_END_REASON_CHECKPOINT_BOUND


def setting(name: str) -> str:
    if os.getenv(name):
        return os.environ[name].strip()
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            if line.startswith(name + "="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def safe_error_diagnostic(exc: Exception) -> tuple[str, str, dict[str, str]]:
    """Only expose bounded error categories and documented numeric rate headers."""
    code = getattr(exc, "code", None)
    status = code().name if callable(code) else "n/a"
    details = getattr(exc, "details", None)
    message = (details() or "").lower() if callable(details) else ""
    if re.search(r"(received message larger|max_receive_message_length|message size)", message):
        category = "message_size"
    elif re.search(r"(response units|\bru\b|quota|credits|billing)", message):
        category = "account_quota_or_ru"
    elif re.search(r"(rate limit|too many requests|requests per)", message):
        category = "rate_limit"
    elif re.search(r"(stream limit|scan limit|result limit)", message):
        category = "server_stream_limit"
    elif re.search(r"(response size|payload size|response too large)", message):
        category = "response_size"
    else:
        category = "undetermined"
    metadata_fn = getattr(exc, "trailing_metadata", None)
    metadata = {}
    if callable(metadata_fn):
        for key, value in metadata_fn() or ():
            if re.fullmatch(r"(?:x-)?(?:rate-?limit|retry-after)[a-z-]*", key.lower()) and \
                    re.fullmatch(r"[0-9.]{1,20}", str(value)):
                metadata[key.lower()] = str(value)
    return status, category, metadata


async def main(sample_checkpoints: int = 0, page_limit: int = 500,
               quick_sample: bool = False, max_pages: int = 3,
               offset_from_head: int = 100) -> None:
    host = setting("SUI_GRPC_HOST")
    key = setting("SUI_GRPC_API_KEY")
    if not host or not key:
        raise SystemExit("Sui gRPC host or key is missing (credential not displayed)")
    if not 1 <= page_limit <= 500:
        raise SystemExit("page_limit must be 1..500")
    if not 1 <= max_pages <= 100 or not 2 <= offset_from_head <= 100_000:
        raise SystemExit("max_pages must be 1..100 and offset_from_head 2..100000")
    client = SuiGrpcClient(key, SuiConfig(timeout_sec=20, grpc_stream_limit=page_limit),
                           endpoint=host)
    try:
        if quick_sample:
            raw = await client._info(GetServiceInfoRequest(), timeout=20,
                                     metadata=client.metadata)
            info = {"chain": raw.chain, "head": int(raw.checkpoint_height),
                    "lowest": int(raw.lowest_available_checkpoint)}
            if info["chain"].lower() != "mainnet":
                raise RuntimeError("not mainnet")
        else:
            info = await client.preflight()
            print(f"Sui gRPC preflight: chain={info['chain']} head={info['head']} "
                  f"lowest={info['lowest']} history_start={info['history_start']} "
                  "30-day retention=ok ListTransactions=ok")
        if sample_checkpoints:
            if not 1 <= sample_checkpoints <= 10:
                raise ValueError("sample_checkpoints must be 1..10")
            end = max(int(info["lowest"]) + 1,
                      int(info["head"]) - offset_from_head)
            start = max(int(info["lowest"]), end - sample_checkpoints)
            after = None
            frames = 0
            complete = False
            began = time.monotonic()
            requests = 0
            transactions = 0
            last_terminal = "none"
            for _ in range(max_pages):
                requests += 1
                terminal = None
                cursor = None
                async for frame in client.list_range(start, end, after):
                    frames += 1
                    transactions += int(frame.HasField("transaction"))
                    if frame.HasField("end"):
                        terminal = int(frame.end.reason)
                        cursor = bytes(frame.watermark.cursor)
                last_terminal = str(terminal)
                if terminal == QUERY_END_REASON_CHECKPOINT_BOUND:
                    complete = True
                    break
                if not cursor or cursor == after:
                    break
                after = cursor
            print(f"Sui gRPC sample: start={start} end={end} limit={page_limit} "
                  f"requests={requests} transactions={transactions} frames={frames} "
                  f"elapsed_sec={time.monotonic()-began:.2f} "
                  f"terminal_reason={last_terminal} bounded_range_complete={complete}")
    except Exception as exc:
        status, category, metadata = safe_error_diagnostic(exc)
        print(f"Sui gRPC request failed: {type(exc).__name__} status={status} "
              f"category={category} rate_metadata={metadata} "
              "(endpoint and credential hidden)")
        raise SystemExit(1) from None
    finally:
        await client.close()


if __name__ == "__main__":
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--sample-checkpoints", type=int, default=0)
    args.add_argument("--page-limit", type=int, default=500)
    args.add_argument("--quick-sample", action="store_true")
    args.add_argument("--max-pages", type=int, default=3)
    args.add_argument("--offset-from-head", type=int, default=100)
    parsed = args.parse_args()
    asyncio.run(main(parsed.sample_checkpoints, parsed.page_limit,
                     parsed.quick_sample, parsed.max_pages,
                     parsed.offset_from_head))
