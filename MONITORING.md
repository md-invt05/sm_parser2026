# Telegram monitoring and Docker deployment

Network samples distinguish `live` and `backfill` cursors. RPC health is persisted
per endpoint fingerprint and method group without storing complete private URLs.
The default profile is `conservative` (`low` remains a compatibility alias), and
Telegram never increases load automatically. Qualifying counts are recalculated
with the current threshold rather than copied from historical scan labels.

The production stack is defined in `docker-compose.yml` and contains four isolated services:

- `scanner` writes heartbeat and minute aggregates to `data/monitoring.db`;
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
discovery and six for balances. Its governor reduces live slots and pauses backfill
while the EVM/Sui queues are large, then restores capacity after ten stable minutes.
Discovery uses separate fair live/backfill queues. Profile capacities are `4/1` for
`conservative` and `low`, `6/1` for `normal`, and `10/2` for `high`. Live waiters
older than 30 seconds take FIFO priority; otherwise the largest lag is served first.
`/status` shows active slots, queued networks, and the oldest wait time.
Automatic exports run every six hours and rebuild only the qualifying EVM/Sui files.
`/export` and non-qualifying `/file` requests build the complete report bundle.
Pause is cooperative: no new block range or address starts while current SQLite
transactions finish. SIGTERM has a 35-second Compose grace period and performs a
final export. Online SQLite backups are created daily; the newest seven are kept.

Before server deployment, rotate every RPC/API key that has ever been pasted into
a chat or terminal transcript. Keep `.env` out of Git and set its mode to `600`.
