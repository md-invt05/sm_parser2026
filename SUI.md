# Sui / Blockberry and checkpoint verification

Financial Sui reports now contain one row per DeFi project, not one row per Move
package. Blockberry `/dex` provides indexed project TVL and package mappings.
Large pools from `/dex/pools` are checked with `sui_getObject`; verified pool TVL
is audit evidence and is never added to the indexed TVL a second time.

Packages and shared/object-owned objects are not accounts. Account-balance calls
for those IDs are disabled, old package scans remain legacy history, and
technical discoveries are exported separately to `sui_packages.xlsx`.

Pagination continues beyond page ten until the saved checkpoint or 30-day cutoff
is reached. If neither is reached, the scanner records an explicit coverage gap.

Optional `SUI_GRPC_HOST` and `SUI_GRPC_API_KEY` enable an authenticated Sui
mainnet gRPC `LedgerService/ListTransactions` endpoint. For OnFinality use its
TLS `host:port` and API key in the ignored `.env`; the client sends `api-key`
metadata. Chainstack uses `x-token` metadata, while other configured hosts use
Bearer metadata. `SUI_GRPC_FALLBACK_HOST` and `SUI_GRPC_FALLBACK_API_KEY` can
configure a second provider. Each client paces its own calls at no more than
20/s, below the supplied 25/s Chainstack and 30/s OnFinality request limits.
Provider response-unit and stream-size limits can still be tighter.
Run `python scripts/check_sui_grpc.py` for a read-only mainnet and method
preflight; it verifies that the configured page size works near head and that
the retention floor predates the 30-day cutoff. `RESOURCE_EXHAUSTED` keeps
Blockberry in `coverage_unverified` instead of creating a false verified gap.
The credential is never logged. A failed stream is replayed from its checkpoint
start on the fallback; a quota rejection switches immediately, while transport
errors require two consecutive failures. Primary recovery requires two separate
successful preflights and happens only between checkpoint attempts. A checkpoint
becomes verified only after a terminal
`CHECKPOINT_BOUND` frame; transaction records or an interrupted stream are not
enough. Partial records stay deduplicated by digest, and the entire unfinished
checkpoint is replayed after a restart. A separate tip cursor discovers recent
transactions while a persisted checkpoint gap covers the skipped history; four
tip slices alternate with one catch-up slice. `/status` displays both ranges and
the remaining gap. The old Blockberry `last_checkpoint` is never treated as
proof of continuous coverage. If gRPC preflight fails, Blockberry discovery
continues as best-effort with `coverage_unverified`.
During `balance_only`, gRPC preflight can report `grpc_ready discovery_paused`,
but it does not advance or promote verified coverage.

Sui runs as a separate Move-package subsystem and does not change EVM coverage
(`N/20`). Set `BLOCKBERRY_API_KEY` in `.env`; Blockberry is the primary indexed
discovery and project-TVL source. Optional comma-separated `SUI_RPC` endpoints
verify pool objects; the Blockberry key is never forwarded to them.

The worker discovers non-system packages from `Publish` and direct `MoveCall`,
links shared/object-owned state objects from raw transaction changes, and reports
those findings only as technical package data. Project status becomes `incomplete`
when package mapping is missing or the provider reports a partial snapshot.
A temporary provider failure keeps the last trustworthy TVL and category,
labelled with its age and `stale` instead of being reclassified as zero.

Sui outputs are separate: `sui_qualifying.xlsx`,
`sui_below_threshold.xlsx`, and `sui_incomplete.xlsx`. Telegram supports
`/file sui_qualifying|sui_below|sui_incomplete|sui_packages`.

Commands:

```powershell
# Preflight Blockberry (plus all configured EVM networks if not narrowed)
python scan_defi.py --chains sui --rpc-check

# One indexed discovery and balance pass; block arguments mean checkpoints
python scan_defi.py --chains sui --once --min-usd 500000

# Continuous Sui-only work
python scan_defi.py --chains sui --min-usd 500000
```

The local `.env` is ignored by Git. Rotate any API keys that have been published
before deploying the project to a server.
