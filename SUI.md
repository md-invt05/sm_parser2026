# Sui / Blockberry

Financial Sui reports now contain one row per DeFi project, not one row per Move
package. Blockberry `/dex` provides indexed project TVL and package mappings.
Large pools from `/dex/pools` are checked with `sui_getObject`; verified pool TVL
is audit evidence and is never added to the indexed TVL a second time.

Packages and shared/object-owned objects are not accounts. Account-balance calls
for those IDs are disabled, old package scans remain legacy history, and
technical discoveries are exported separately to `sui_packages.xlsx`.

Pagination continues beyond page ten until the saved checkpoint or 30-day cutoff
is reached. If neither is reached, the scanner records an explicit coverage gap.

Sui runs as a separate Move-package subsystem and does not change EVM coverage
(`N/20`). Set `BLOCKBERRY_API_KEY` in `.env`; Blockberry is the primary indexed
discovery and project-TVL source. Optional comma-separated `SUI_RPC` endpoints
verify pool objects; the Blockberry key is never forwarded to them.

The worker discovers non-system packages from `Publish` and direct `MoveCall`,
links shared/object-owned state objects from raw transaction changes, and reports
those findings only as technical package data. Project status becomes `incomplete`
when its indexed snapshot is older than 30 minutes, package mapping is missing,
or the provider reports a partial snapshot. The last known TVL is retained.

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
