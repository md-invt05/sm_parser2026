"""Validate dRPC key/network combinations without printing credentials."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import urllib.error
import urllib.request


NETWORKS = [
    ("ethereum", 1), ("bsc", 56), ("polygon", 137), ("arbitrum", 42161),
    ("optimism", 10), ("base", 8453), ("polygon-zkevm", 1101), ("zksync", 324),
    ("robinhood", 4663), ("linea", 59144), ("scroll", 534352), ("mantle", 5000),
    ("blast", 81457), ("celo", 42220), ("gnosis", 100), ("cronos", 25),
    ("kava", 2222), ("metis", 1088), ("harmony-0", 1666600000),
]


def check(job: tuple[int, str, int, str]) -> tuple[int, str, bool, str]:
    key_index, slug, expected, token = job
    endpoint = f"https://lb.drpc.live/{slug}/{token}"
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}).encode()
    request = urllib.request.Request(
        endpoint, data=payload, headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            result = json.loads(response.read()).get("result")
        actual = int(result, 16) if isinstance(result, str) else None
        return key_index, slug, actual == expected, f"chain_id={actual}" if actual is not None else "missing result"
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
        return key_index, slug, False, type(exc).__name__


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("keys", nargs="+", help="dRPC keys; they are never printed")
    args = parser.parse_args()
    jobs = [
        (key_index, slug, chain_id, token)
        for key_index, token in enumerate(args.keys, 1)
        for slug, chain_id in NETWORKS
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(check, jobs))
    failures = [row for row in results if not row[2]]
    print(f"RESULT valid={len(results) - len(failures)}/{len(results)}")
    for key_index, slug, _ok, reason in failures:
        print(f"FAIL key#{key_index} network={slug} reason={reason}")


if __name__ == "__main__":
    main()
