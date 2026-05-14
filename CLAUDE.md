# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```powershell
cargo build          # compile
cargo run            # build and run
cargo check          # fast type-check without linking
cargo clippy         # lint
```

No tests exist in this project.

## Architecture

Single-binary Rust app (`src/main.rs` + `src/mexc_trade.rs`) that monitors price lag between Binance futures and MEXC futures, optionally trading on detected discrepancies.

### Data flow

1. **Two WebSocket feeds** run as independent `tokio::spawn` tasks:
   - `run_binance`: one task per symbol, subscribes to `<symbol>@bookTicker` on `wss://fstream.binance.com/stream`
   - `run_mexc`: single connection, subscribes `sub.depth.full` for all symbols on `wss://contract.mexc.com/edge`
   - Both send `QuoteUpdate` structs into a shared `mpsc::channel<QuoteUpdate>(20_000)`

2. **Main event loop** (`tokio::select!`) drains the channel and calls `handle_quote`, which does two things per tick:
   - **Lag detection**: when Binance moves ≥ `impulse_bps` within `impulse_window_ms`, a `PendingEvent` is recorded. If MEXC then confirms ≥ `confirm_bps` within `max_lag_ms`, a `LagRecord` is written to CSV and stats are updated.
   - **Diff alert**: checks the current bid/ask spread between exchanges against `alert_diff_bps`; sustained divergence triggers a Telegram alert and optionally opens a trade via `MexcTradeClient`.

3. **Dashboard**: `run_dashboard` serves an Axum HTTP server on `127.0.0.1:8787` with a self-contained HTML dashboard (embedded as `DASHBOARD_HTML` const). `/api/state` returns `DashboardSnapshot` JSON; `/api/config` supports GET/POST for live-adjusting thresholds without restart.

4. **Trading** (`src/mexc_trade.rs`): `MexcTradeClient` opens a market entry on MEXC futures, places a limit exit at the Binance mid price, then waits for either a close signal (diff collapsed) or `trade_timeout_ms` safety timeout before force-closing with a market order.

### Configuration

`config.json` is the primary config file. Secrets (`telegram_bot_token`, `mexc_api_key`, `mexc_api_secret`, `telegram_login_password`) can be overridden from `telegram_config.json` (not committed) or environment variables `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_LOGIN_PASSWORD`.

Live parameters (`impulse_bps`, `confirm_bps`, `alert_diff_bps`, `trade_notional_usdt`, `trade_enabled`) can be changed at runtime via `POST /api/config` without restarting.

### Symbol naming

Binance uses `BTCUSDT` format; internally the app normalizes to `BTC_USDT`. `PEPE_USDT` is a special case — Binance stream uses `1000pepeusdt` and prices are divided by 1000 on parse. MEXC uses `BTC_USDT` natively.

### Output files

- `lag_events.csv` — every confirmed lag event
- `slippage_events.csv` — per-event trade estimate (entry/exit spread, gross/net bps, PnL)
- `stats_snapshots.csv` — periodic per-symbol stats snapshot
