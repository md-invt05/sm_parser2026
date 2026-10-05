# Telegram monitoring and Docker deployment

Network samples distinguish `live` and `backfill` cursors. RPC health is persisted
per endpoint fingerprint and method group without storing complete private URLs.
The default profile is `conservative` (`low` remains a compatibility alias), and
Telegram never increases load automatically. Qualifying counts are recalculated
with the current threshold rather than copied from historical scan labels.

The production stack is defined in `docker-compose.yml` and contains five isolated services:

- `scanner` writes heartbeat and minute aggregates to `data/monitoring.db`;
- `exporter` generates XLSX independently, capped at 0.75 CPU and 768 MiB;
- `telegram-bot` reads monitoring data and reports, but receives no RPC secrets;
- `docker-proxy` exposes the container API required for the fixed, labelled scanner target;
- `node-exporter` exposes host metrics only on the internal Compose network.

Required `.env` values:

```dotenv
TELEGRAM_BOT_TOKEN=replace_me
TELEGRAM_CHAT_IDS=123456789
MIN_USD=500000
SCANNER_MEMORY_LIMIT=4g
```

On Linux, protect the file and start the stack:

```bash
chmod 600 .env
docker compose up -d --build
docker compose ps
```

The bot sends a report every six hours by default. The interval is persisted in
`monitoring.db` and can be changed with `/schedule`. Lifecycle and load-profile
changes require a 60-second inline confirmation. Telegram never accepts shell
commands, RPC URLs or arbitrary scanner arguments.

Available commands: `/status`, `/report`, `/networks`, `/resources`, `/errors`,
`/files`, `/file`, `/schedule`, `/load`, `/start`, `/stop`, `/restart`, `/pause`,
`/resume`, `/export`, `/backup`, `/history`, `/help`.

The scanner reads the persisted `conservative`, `low`, `steady`, `normal` or `high` profile on startup.
`steady` is the recommended 24/7 server profile: it reserves four RPC requests for
discovery and six for balances. The protective governor applies to every profile,
including `normal` and `high`. Only `steady` enters automatic `balance_only`
when the executable EVM/Sui queue is critical; the other profiles stay in
`drain` with up to three live slots and no backfill. A large token backlog can
still limit any profile to one live slot. Capacity returns only after ten stable
minutes below the lower thresholds. Switching away from `steady` can increase
the balance backlog because discovery continues.
Discovery uses separate fair live/backfill queues. Profile capacities are `4/1` for
`conservative` and `low`, `6/1` for `normal`, and `10/2` for `high`. Live waiters
older than 30 seconds take FIFO priority; otherwise the largest lag is served first.
`/status` shows active slots, queued networks, and the oldest wait time.
It also shows the due, partial and failed token-log tasks. `/networks` shows
their per-network cursor, oldest task, and active logs RPC. The token-log worker
continues in `balance_only`, capped at 10 requests/minute globally and 2 per
network (normally 30/4), using discovery's RPC quota. Recent 1,000-block windows
have priority; historical work uses at most 20% of the log budget. Pending token
history remains an explicit coverage gap, not a failed balance scan.
Confirmed EVM balances are rechecked after 24 hours above the threshold, 72 hours
from $150k, 30 days for smaller positives, and 90 days for zero/absent chains.
RPC failures retry only the affected chain. Missing prices are retried from saved
amounts without another balance RPC call.
Automatic exports run every six hours and rebuild only the qualifying EVM/Sui files.
`/export` forces the complete report bundle. `/file` sends a nonempty report
immediately when it was generated within the last six hours; otherwise it
queues only that report. Repeated requests for the same pending export share
the same job. The bot separates new, planned, RPC-retry and token-coverage work.
Pause is cooperative: no new block range or address starts while current SQLite
transactions finish. The isolated exporter streams rows to temporary XLSX files
and atomically replaces each completed report; a failed job leaves its predecessor.
Scanner SIGTERM does not wait for an XLSX export. Online SQLite backups are created
daily; the newest seven are kept. Before a server upgrade, make an online backup
and rebuild `scanner` and `exporter`; never use `docker-compose down -v`.

Before server deployment, rotate every RPC/API key that has ever been pasted into
a chat or terminal transcript. Keep `.env` out of Git and set its mode to `600`.
