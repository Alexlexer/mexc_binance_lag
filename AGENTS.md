# Repository Guidelines

## Project Structure & Module Organization

This is a single-binary Rust project for monitoring Binance-to-MEXC futures lag.

- `src/main.rs` contains the async runtime, WebSocket feed handling, lag detection, CSV output, Telegram/dashboard endpoints, and live config API.
- `src/mexc_trade.rs` contains MEXC futures trading client logic.
- `config.json` is the primary runtime configuration. Keep machine-local secret overrides in `telegram_config.json` or environment variables.
- `scripts/build-unix.sh`, `run.sh`, `run.cmd`, and `run-visible.cmd` are convenience launch/build scripts.
- `.github/workflows/build.yml` builds release artifacts for Linux, macOS, and Windows.
- Runtime outputs include `lag_events.csv`, `slippage_events.csv`, `stats_snapshots.csv`, and `monitor.log`.

## Build, Test, and Development Commands

- `cargo check` — fast compile/type check; run before committing.
- `cargo build` — compile a debug binary.
- `cargo build --release` — compile the optimized production binary.
- `cargo run` — run locally with `config.json`.
- `cargo clippy` — lint Rust code and catch common mistakes.
- Windows: `.\run.cmd`; Unix-like systems: `chmod +x run.sh scripts/build-unix.sh && ./run.sh`.

## Coding Style & Naming Conventions

Use Rust 2021 conventions and run `cargo fmt` before submitting changes. Prefer clear snake_case names for functions, variables, and modules; use PascalCase for structs/enums. Keep async tasks small and explicit, and preserve existing symbol normalization (`BTCUSDT` externally, `BTC_USDT` internally; `PEPE_USDT` special handling).

## Testing Guidelines

No automated tests currently exist. For changes, at minimum run `cargo check` and `cargo clippy`. When adding tests, place unit tests beside the relevant module with `#[cfg(test)]` and use descriptive names such as `detects_lag_after_binance_impulse`.

## Commit & Pull Request Guidelines

Recent commits use short, imperative messages such as `add /api/trade-stats endpoint` or scoped prefixes like `stats: add min_lag_ms filter`. Follow that style. Pull requests should include a brief summary, config or output-file impacts, validation commands run, and screenshots if the dashboard UI changes.

## Security & Configuration Tips

Do not commit live API keys, bot tokens, passwords, or subscriber lists. Prefer environment variables (`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_LOGIN_PASSWORD`) for secrets. Treat trading changes as high risk: document default behavior, timeouts, and any order-placement changes clearly.
