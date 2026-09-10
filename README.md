# DeFi Liquidity Scanner

Ончейн-сканер смарт-контрактов для аналитики ликвидности.

Скрипт с заданного блока до `latest`:

1. Находит создания контрактов (`tx.to == null` → `receipt.contractAddress`).
2. Пишет их в SQLite (`data/contracts.db`), умеет резюмироваться после рестарта.
3. Параллельно читает балансы каждого контракта: натив (ETH/BNB/POL/HYPE) + ERC-20/BEP-20 из `tokens.yaml` и токены, которые реально приходили на контракт (`Transfer`).
4. Переводит всё в USD по **текущему** курсу DefiLlama на момент проверки.
5. Выгружает два Excel:
   - `reports/qualifying_*.xlsx` — `total_usd ≥ min_usd` (по умолчанию $100 000)
   - `reports/below_threshold_*.xlsx` — `0 < total_usd < min_usd`, в колонке «Маркировка» текст вида `80,000.00$ (меньше условия 100,000$)`
   - нулевые балансы в Excel **не попадают**, только в БД со статусом `zero`

Сети: Ethereum, BSC, Polygon, Polygon zkEVM (`zk`), ZKsync Era, Robinhood Chain, Hyperliquid HyperEVM.

## Установка

```bash
cd defi_liquidity_scanner
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Публичные RPC быстро упираются в rate limit. Для реального бэкфилла впиши в `.env` ключи Alchemy / Infura / QuickNode / Ankr.

## Настройка

`config.yaml`

- `min_usd` — порог
- `chains.<name>.start_block` — стартовый блок **этой** сети (номера блоков в сетях разные)
- `chains.<name>.enabled` — выключить сеть без удаления
- `chains.<name>.rpc` — запасные публичные URL, если нет `.env`

`tokens.yaml` — список токенов для оценки. Добавляй любые адреса.

Стартовый блок также можно передать флагом `--from-block` (тогда одно число применится ко всем выбранным сетям — удобно только если гоняешь одну сеть).

## Запуск

Обе очереди сразу (индексатор + чекер балансов + периодический Excel раз в 3 минуты):

```bash
python scan_defi.py --chains ethereum --from-block 21000000 --min-usd 100000
```

Несколько сетей:

```bash
python scan_defi.py --chains ethereum,bsc,polygon,zk,zksync,robinhood,hyperliquid
```

Только индекс / только балансы / только выгрузка:

```bash
python scan_defi.py --index-only --chains bsc
python scan_defi.py --balances-only
python scan_defi.py --export-only
```

Остановка: `Ctrl+C`. Перед выходом скрипт ещё раз пишет Excel.

## База

`data/contracts.db`

| таблица        | смысл                                      |
|----------------|--------------------------------------------|
| `chain_state`  | последний проиндексированный блок          |
| `contracts`    | все найденные контракты                    |
| `tokens`       | известные ERC-20 (seed + Transfer)         |
| `scans`        | каждый проход проверки баланса             |
| `scan_tokens`  | разбивка по токенам внутри скана           |

Прогресс не теряется: при повторном запуске индексатор продолжает с `last_indexed + 1`.

## Ограничения (важно)

- Видны только **top-level CREATE** (транзакция без `to`). Внутренние CREATE/CREATE2 через фабрики требуют `debug_traceBlock` на archive-ноде — в эту версию не входит.
- «Все монеты на контракте» на EVM нельзя узнать одним вызовом. Скрипт считает натив + токены из `tokens.yaml` + токены, которые приходили на контракт в просканированных блоках. Экзотический dust без Transfer в окне скана может быть не увиден.
- USD — спот на момент проверки, не историческая цена блока создания.
- HyperEVM (`rpc.hyperliquid.xyz/evm`): `eth_getLogs` ≤ 50 блоков, `eth_call` / `eth_getBalance` только `latest`.
- Polygon zkEVM (`zk`) в 2026 sunset-ится. Сеть оставлена, потому что ты её указал.
- Без платного RPC полный проход Ethereum/BSC с «глубокого» блока займёт дни и будет резаться лимитами.

## Полезные SQL

```sql
SELECT chain, COUNT(*) FROM contracts GROUP BY 1;
SELECT chain, last_status, COUNT(*) FROM contracts GROUP BY 1,2;
SELECT chain, address, last_total_usd FROM contracts
 WHERE last_total_usd >= 100000 ORDER BY last_total_usd DESC;
```
