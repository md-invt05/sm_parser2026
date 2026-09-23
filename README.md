# DeFi Liquidity Scanner

## Reliable 24/7 mode

The default server profile is `conservative`: six global EVM requests, one
request per network, two addresses and four address/network parts at a time.
Discovery uses separate `live` and `backfill` cursors on every enabled network;
a failed block, receipt, log or code request cannot advance its cursor.

Balance state is persisted per `address + chain`, so a transient failure retries
only that network. Large native balances are quarantined until classified.
Known system reserves remain auditable but are excluded from `total_usd`; this
includes the genesis-prefunded Polygon zkEVM bridge reserve. Polygon zkEVM is
marked `legacy_read_only`.

XLSX generation runs in a separate process over query-only SQLite connections,
only after data changes and at most once per 30 minutes.

Deployment and Telegram monitoring: [MONITORING.md](MONITORING.md).

Последовательный EVM-сканер прямых деплоев и активных контрактов. Основная точка входа осталась прежней: `scan_defi.py`.

Что он делает:

1. Идёт от `start_block` до `latest - confirmations`, находит прямые деплои и уникальные верхнеуровневые `tx.to`.
2. Берёт `contractAddress` деплоя из обязательного receipt, а `tx.to` добавляет только после пакетного `eth_getCode(..., "latest")`.
3. Запоминает ERC-20, которые переводились найденному контракту в обработанном диапазоне.
4. Проверяет тот же адрес во всех 20 сетях через `eth_getCode`. Балансы учитываются только в сетях, где адрес содержит bytecode.
5. Суммирует нативный баланс, seed-токены сети и связанные с контрактом токены по текущим ценам DefiLlama.
6. Пишет одну строку на глобальный адрес в Excel.

Статусы агрегированного результата:

- `qualifying` — уже оценённая сумма не меньше порога, даже если часть сетей временно недоступна;
- `below` — все 20 сетей и все положительные активы проверены, сумма ниже порога;
- `incomplete` — был RPC-сбой, неполный token call или положительный баланс без цены;
- полностью нулевой успешный результат хранится в БД как `below`, но в Excel `below_threshold.xlsx` не попадает.

## Сети

По умолчанию discovery идёт во всех 20 включённых EVM-сетях. Для ограничения поиска
используйте `--chains`, например `--chains ethereum,arbitrum,base`.

Через `--chains` можно выбрать любую из 20 сетей: `ethereum`, `bsc`, `polygon`, `arbitrum`, `optimism`, `base`, `zk`, `zksync`, `robinhood`, `hyperliquid`, `linea`, `scroll`, `mantle`, `blast`, `celo`, `gnosis`, `cronos`, `kava`, `metis`, `harmony`.

Выбор `--chains` влияет только на поиск новых деплоев. Мультичейн-проверка найденного адреса всегда использует все включённые сети.

## Установка

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp env.example .env
```

В Windows активация окружения: `.venv\Scripts\Activate.ps1`.

Публичные RPC предназначены для best-effort и smoke-тестов. Для длительного прохода добавьте private/keyed URL в `.env`. Несколько URL разделяются запятыми. `<CHAIN>_RPC` ставятся перед публичными fallback из `config.yaml`, но не удаляют их.

Полные RPC URL не пишутся в лог: отображается только hostname.

## Запуск

Обычный непрерывный режим со всеми включёнными сетями:

```bash
python scan_defi.py
```

Выбранные сети и свой порог:

```bash
python scan_defi.py --chains ethereum,arbitrum,base --min-usd 100000
```

Диагностика всех RPC без запуска сканера:

```bash
python scan_defi.py --rpc-check
```

Конечный проход для теста:

```bash
python scan_defi.py --chains ethereum --from-block 25900000 --to-block 25900099 --once --index-only
```

Совместимые режимы:

```bash
python scan_defi.py --index-only
python scan_defi.py --balances-only --once
python scan_defi.py --export-only
```

Поиск контрактов среди `tx.to` включён по умолчанию. Отключить его для отдельного запуска:

```bash
python scan_defi.py --no-tx-to-contracts
```

При первом включении новая ревизия индексатора один раз возвращает сохранённый курсор к
настроенному `start_block`, чтобы заполнить ранее пропущенные активные контракты.

`--from-block` не откатывает уже более новый сохранённый курсор. Для каждой сети курсор один, следующий запуск продолжает с `last_indexed + 1`.

## Надёжность RPC и курсора

- перед работой endpoint проверяется через `eth_chainId` и `eth_blockNumber`;
- wrong chain, HTTP 401/403 и обязательный API key отключают endpoint до следующего запуска;
- JSON-RPC error внутри HTTP 200 считается ошибкой;
- пропущенные элементы batch повторяются отдельными вызовами;
- receipts читаются всеми порциями, без прежнего ограничения первых 80 деплоев;
- если отсутствует блок, обязательный receipt или диапазон `eth_getLogs`, курсор не двигается;
- неполный или ошибочный batch `eth_getCode` также не продвигает курсор;
- известные контракты не проверяются повторно, а пустой bytecode кэшируется на 24 часа;
- действуют общий лимит 12 HTTP RPC-запросов, лимит 2 на сеть и адаптивные batch;
- вставки и повторная обработка диапазона идемпотентны.

## Параллельные балансы и дополнительная проверка Rabby

Balance worker обрабатывает **8 адресов одновременно**, распределяя проверки сетей по 12 слотам.
Освободившийся слот адреса сразу берёт следующий адрес из очереди. На отдельную сеть
выделяется до 20 секунд, на весь адрес — до 60 секунд, включая ожидание свободных слотов.
Если все RPC сети находятся в cooldown, balance worker сразу отмечает её недоступной.
Индексатор сохраняет свою прежнюю логику повторов и непрерывного курсора.

Неполные сканы повторяются через 5 минут; полностью успешные — через 24 часа.
При большом числе новых адресов повтор может начаться позже из-за очереди. Лог `[balances]`
показывает длительность каждого адреса и coverage. Параллельные запросы одинаковых цен
объединяются через общий кэш; недоступность цены не означает нулевой баланс.

При ошибке, таймауте или отсутствующей цене RPC-результат сначала сохраняется в SQLite.
Затем отдельный последовательный worker обращается к Rabby:
`/v1/user/total_balance` и `/v1/user/cache_token_list`. Успешный RPC-скан Rabby не запускает.
Очередь Rabby не занимает слоты основной проверки балансов и восстанавливается из БД.

Дополнительная проверка включена в `config.yaml` (`rabby_fallback: true`). Запуск только
балансов найденных контрактов, без индексации новых блоков:

```powershell
.\.venv\Scripts\python.exe scan_defi.py --balances-only
```

Отключить Rabby на один запуск:

```powershell
.\.venv\Scripts\python.exe scan_defi.py --balances-only --no-rabby-fallback
```

`--rabby-fallback` принудительно включает его, если он отключён в YAML.
`--once` обрабатывает конечную очередь; если Rabby вернул rate limit, конечный проход
не ждёт его cooldown. Оставшиеся дополнительные проверки доступны следующему запуску.

В существующих Excel-отчётах появляются отдельные колонки Rabby: оценка USD, диапазон,
статус, coverage, время, сети и токены. `Total USD` и основной статус остаются результатом RPC.
Даже высокая оценка Rabby сама по себе не переносит адрес в `qualifying.xlsx`.
Оценка от предыдущего RPC-скана сохраняет время и помечается как относящаяся к старому скану.

Для допуска `rabby_uncertainty: 0.10` принимается условие:
`оценка = реальная сумма × (1 ± 10%)`. Поэтому диапазон равен
`оценка / 1.1 … оценка / 0.9`. Например, $100 000 в Rabby означает примерно
$90 909–111 111; оценки от $90 000 до $110 000 попадают около порога $100 000.
Это пользовательское допущение, а не гарантированная точность API. Статусы оценки:
`estimated_below`, `near_threshold`, `estimated_above`, `incomplete`.

В оценку входят только поддерживаемые сети с подтверждённым RPC bytecode. Общий
`total_usd_value` кошелька напрямую не используется: он может включать EOA и другие сети.
Если код/сумма сети не подтверждены, либо положительный токен без цены, оценка неполная,
а верхняя граница неизвестна. Положительные unpriced-токены Rabby сохраняются отдельно в БД.

Rabby вызывается не чаще одного запроса в 3 секунды; HTTP 429 включает паузу с учётом
`Retry-After`, HTTP 401/403 отключают дополнительные запросы до перезапуска.
Используются обычные GET без авторизационных заголовков. Доступность и лимиты не гарантированы:
[официальный клиент Rabby](https://github.com/RabbyHub/rabby-api) и
[документация отдельного DeBank Pro API](https://docs.cloud.debank.com/en/readme/api-pro-reference/user).

Discovery scheduling is profile-driven: `conservative/low=4 live + 1 backfill`,
`normal=6+1`, `high=10+2`. Live networks are selected by lag with a 30-second
FIFO starvation guard; backfill uses its own FIFO queue. Current queue/slot data
is visible in `/status` and `logs/status.txt`.

For continuous server operation use `/load steady`. It splits the ten-request RPC
budget into discovery `4` and balances `6`, pauses backfill under queue pressure,
and reduces live slots to `3` or `2` until the backlog recovers. Discovery ranges
have a 180-second watchdog and retain their cursor on timeout.

Основные настройки: `balance_concurrency`, `balance_chain_concurrency`,
`balance_chain_timeout_sec`, `balance_address_timeout_sec`, `balance_retry_sec`,
`discover_tx_to_contracts`, `code_cache_ttl_sec`, `rabby_fallback`,
`rabby_request_interval_sec`, `rabby_timeout_sec`, `rabby_uncertainty`.

## Файлы результата

| Файл | Содержание |
|---|---|
| `reports/qualifying.xlsx` | адреса с известной суммой от порога |
| `reports/below_threshold.xlsx` | полностью проверенные ненулевые адреса ниже порога |
| `reports/incomplete.xlsx` | неполное RPC/price покрытие |
| `logs/scanner.log` | основной ротируемый лог |
| `logs/errors.log` | warning и ошибки |
| `logs/status.txt` | safe head, lag, активный RPC, cooldown и агрегированные статусы |

В отчёте есть общий USD, coverage `N/20`, сети с bytecode, посетевые суммы, токены, источники обнаружения и время проверки.

## SQLite

Старые таблицы и данные не удаляются. Миграция идемпотентно добавляет:

| Таблица | Назначение |
|---|---|
| `contract_tokens` | связь `contract -> token` по сети |
| `address_scans` | агрегированный мультичейн-результат адреса |
| `address_chain_scans` | посетевые части результата |
| `address_token_scans` | token balances, включая положительные unpriced |
| `rabby_estimates` | отдельная оценка Rabby, диапазон, связь с RPC-сканом и unpriced-токены |
| `contract_discoveries` | уникальные источники `direct_deploy` и `active_call` для контракта |
| `contract_code_cache` | результат и время последней проверки `eth_getCode` |

Существующие `chain_state`, `contracts`, `tokens`, `scans` и `scan_tokens` остаются совместимыми.

## Ограничения

- Внутренние `CREATE/CREATE2` не видны без trace/archive RPC и не входят в эту версию.
- EVM не предоставляет универсальный список токенов адреса: проверяются seed-токены и токены, связанные с контрактом через обработанные `Transfer`.
- Балансы и цены текущие, не исторические.
- Одинаковый адрес с bytecode в нескольких сетях намеренно считается одной мультичейн-сущностью.
