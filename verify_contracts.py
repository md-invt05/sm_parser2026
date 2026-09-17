"""Verify EVM contract bytecode through configured RPC endpoints.

Example:
  .venv\\Scripts\\python.exe verify_contracts.py 0x... 0x...
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import urllib.error
import urllib.request

from dotenv import load_dotenv


NETWORKS = [
    ("ethereum", "ETHEREUM_RPC"), ("bsc", "BSC_RPC"),
    ("polygon", "POLYGON_RPC"), ("arbitrum", "ARBITRUM_RPC"),
    ("optimism", "OPTIMISM_RPC"), ("base", "BASE_RPC"),
    ("zk", "ZK_RPC"), ("zksync", "ZKSYNC_RPC"),
    ("robinhood", "ROBINHOOD_RPC"), ("linea", "LINEA_RPC"),
    ("scroll", "SCROLL_RPC"), ("mantle", "MANTLE_RPC"),
    ("blast", "BLAST_RPC"), ("celo", "CELO_RPC"),
    ("gnosis", "GNOSIS_RPC"), ("cronos", "CRONOS_RPC"),
    ("kava", "KAVA_RPC"), ("metis", "METIS_RPC"),
    ("harmony", "HARMONY_RPC"),
]
CORE_COUNT = 6


def code_at(address: str, chain: str, env_name: str) -> tuple[str, str, int | None, str | None]:
    endpoints = [url.strip() for url in os.environ.get(env_name, "").split(",") if url.strip()]
    if not endpoints:
        return address, chain, None, "no configured RPC"
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "eth_getCode", "params": [address, "latest"],
    }).encode()
    last_error = "unknown RPC error"
    for endpoint in endpoints:
        request = urllib.request.Request(
            endpoint, data=payload, headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                body = json.loads(response.read())
            code = body.get("result")
            if not isinstance(code, str) or not code.startswith("0x"):
                last_error = "malformed JSON-RPC result"
                continue
            byte_length = (len(code) - 2) // 2
            return address, chain, byte_length, None
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = type(exc).__name__
    return address, chain, None, last_error


def query(addresses: list[str], networks: list[tuple[str, str]]) -> list[tuple[str, str, int | None, str | None]]:
    jobs = [(address, chain, env_name) for address in addresses for chain, env_name in networks]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        return list(pool.map(lambda job: code_at(*job), jobs))


def main() -> None:
    parser = argparse.ArgumentParser(description="Confirm EVM bytecode without using the scanner database")
    parser.add_argument("addresses", nargs="+", help="one or more 0x-prefixed EVM addresses")
    args = parser.parse_args()
    addresses = [item.lower() for item in args.addresses]
    invalid = [item for item in addresses if len(item) != 42 or not item.startswith("0x")]
    if invalid:
        raise SystemExit(f"invalid EVM address(es): {', '.join(invalid)}")
    load_dotenv()
    found: dict[str, list[tuple[str, int]]] = {address: [] for address in addresses}
    errors: dict[str, list[str]] = {address: [] for address in addresses}
    for address, chain, byte_length, error in query(addresses, NETWORKS[:CORE_COUNT]):
        if byte_length:
            found[address].append((chain, byte_length))
        elif error:
            errors[address].append(chain)
    unresolved = [address for address in addresses if not found[address]]
    if unresolved:
        for address, chain, byte_length, error in query(unresolved, NETWORKS[CORE_COUNT:]):
            if byte_length:
                found[address].append((chain, byte_length))
            elif error:
                errors[address].append(chain)
    for address in addresses:
        details = ", ".join(f"{chain}: {size} B" for chain, size in found[address])
        if details:
            print(f"CONTRACT  {address}  {details}")
        elif errors[address]:
            print(f"UNKNOWN   {address}  RPC errors: {', '.join(errors[address])}")
        else:
            print(f"NO_CODE   {address}  no bytecode on checked networks")


if __name__ == "__main__":
    main()
