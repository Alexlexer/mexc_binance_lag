use anyhow::{Context, Result};
use axum::{extract::State, response::Html, routing::get, Json, Router};
use crossterm::{cursor::MoveTo, execute, terminal::{Clear, ClearType}};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::str::FromStr;
use tokio::sync::{mpsc, RwLock};
use tower_http::cors::CorsLayer;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};



type SharedDashboard = Arc<RwLock<DashboardSnapshot>>;

#[derive(Debug, Clone, Default, Serialize)]
struct DashboardSnapshot {
    updated_at: String,
    summary: String,
    impulse_bps: String,
    confirm_bps: String,
    max_lag_ms: i64,
    symbols: Vec<DashboardSymbolRow>,
}

#[derive(Debug, Clone, Serialize)]
struct DashboardSymbolRow {
    symbol: String,
    binance_quotes: u64,
    mexc_quotes: u64,
    impulses: u64,
    matched: u64,
    expired: u64,
    avg_lag_ms: Option<i64>,
    p50_lag_ms: Option<i64>,
    p95_lag_ms: Option<i64>,
    binance_mid: Option<String>,
    mexc_mid: Option<String>,
    price_diff_usdt: Option<String>,
    price_diff_bps: Option<String>,
    pending: Option<String>,
    last_direction: Option<String>,
    last_lag_ms: Option<i64>,
    gross_bps: Option<String>,
    net_fee_bps: Option<String>,
    net_zero_fee_bps: Option<String>,
    pnl_fee_usdt: Option<String>,
    pnl_zero_fee_usdt: Option<String>,
}
#[derive(Debug, Clone, Deserialize)]
struct Config {
    symbols: Vec<String>,
    binance_ws: String,
    mexc_ws: String,
    impulse_bps: Decimal,
    confirm_bps: Decimal,
    impulse_window_ms: i64,
    max_lag_ms: i64,
    event_cooldown_ms: i64,
    stats_interval_secs: u64,
    csv_path: String,
    #[serde(default = "default_stats_csv_path")]
    stats_csv_path: String,
    #[serde(default = "default_slippage_csv_path")]
    slippage_csv_path: String,
    #[serde(default = "default_mexc_taker_fee_bps")]
    mexc_taker_fee_bps: Decimal,
    #[serde(default = "default_trade_notional_usdt")]
    trade_notional_usdt: Decimal,
    #[serde(default)]
    telegram_bot_token: String,
    #[serde(default)]
    telegram_chat_id: String,
    #[serde(default)]
    telegram_login_password: String,
    #[serde(default = "default_telegram_subscribers_path")]
    telegram_subscribers_path: String,
    #[serde(default = "default_alert_diff_bps")]
    alert_diff_bps: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Exchange {
    Binance,
    Mexc,
}

#[derive(Debug, Clone)]
struct QuoteUpdate {
    exchange: Exchange,
    symbol: String,
    bid: Decimal,
    ask: Decimal,
    recv_ts_ms: i64,
}

impl QuoteUpdate {
    fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::from(2)
    }
}

#[derive(Debug, Clone)]
struct PendingEvent {
    symbol: String,
    direction: i32,
    binance_start_mid: Decimal,
    binance_end_mid: Decimal,
    mexc_start_mid: Decimal,
    mexc_start_bid: Decimal,
    mexc_start_ask: Decimal,
    binance_event_ts_ms: i64,
    created_recv_ts_ms: i64,
}

#[derive(Debug, Clone)]
struct LagRecord {
    symbol: String,
    direction: i32,
    lag_ms: i64,
    binance_move_bps: Decimal,
    mexc_move_bps: Decimal,
    binance_start_mid: Decimal,
    binance_end_mid: Decimal,
    mexc_start_mid: Decimal,
    mexc_confirm_mid: Decimal,
    binance_event_ts_ms: i64,
    mexc_recv_ts_ms: i64,
}


#[derive(Debug, Clone)]
struct ActiveDiffAlert {
    started_ms: i64,
    direction: String,
    start_diff_bps: Decimal,
    start_diff_usdt: Decimal,
    max_abs_diff_bps: Decimal,
}
#[derive(Default)]
struct SymbolState {
    last_binance: Option<QuoteUpdate>,
    last_mexc: Option<QuoteUpdate>,
    pending: Option<PendingEvent>,
    last_event_ts_ms: i64,
    active_diff_alert: Option<ActiveDiffAlert>,
}

#[derive(Default, Clone)]
struct LastTradeEstimate {
    direction: String,
    lag_ms: i64,
    gross_cross_bps: Decimal,
    net_fee_bps: Decimal,
    net_zero_fee_bps: Decimal,
    pnl_fee_usdt: Decimal,
    pnl_zero_fee_usdt: Decimal,
}

#[derive(Default, Clone)]
struct SymbolStats {
    binance_quotes: u64,
    mexc_quotes: u64,
    impulses: u64,
    matched: u64,
    expired: u64,
    lags_ms: Vec<i64>,
    last_trade: Option<LastTradeEstimate>,
}

impl SymbolStats {
    fn set_trade_estimate(&mut self, estimate: LastTradeEstimate) {
        self.last_trade = Some(estimate);
    }

    fn add_lag(&mut self, lag_ms: i64) {
        self.matched += 1;
        self.lags_ms.push(lag_ms);
        if self.lags_ms.len() > 2_000 {
            self.lags_ms.drain(0..1_000);
        }
    }

    fn lag_summary(&self) -> Option<(f64, i64, i64, i64, i64)> {
        if self.lags_ms.is_empty() {
            return None;
        }
        let mut xs = self.lags_ms.clone();
        xs.sort_unstable();
        let idx = |pct: usize| ((xs.len().saturating_sub(1)) * pct) / 100;
        let avg = xs.iter().sum::<i64>() as f64 / xs.len() as f64;
        Some((avg, xs[idx(50)], xs[idx(95)], xs[0], xs[xs.len() - 1]))
    }
}

#[derive(Default)]
struct Stats {
    binance_quotes: u64,
    mexc_quotes: u64,
    impulses: u64,
    matched: u64,
    expired: u64,
    lags_ms: Vec<i64>,
    by_symbol: HashMap<String, SymbolStats>,
}

impl Stats {
    fn symbol_mut(&mut self, symbol: &str) -> &mut SymbolStats {
        self.by_symbol.entry(symbol.to_string()).or_default()
    }

    fn record_quote(&mut self, symbol: &str, exchange: Exchange) {
        match exchange {
            Exchange::Binance => {
                self.binance_quotes += 1;
                self.symbol_mut(symbol).binance_quotes += 1;
            }
            Exchange::Mexc => {
                self.mexc_quotes += 1;
                self.symbol_mut(symbol).mexc_quotes += 1;
            }
        }
    }

    fn record_impulse(&mut self, symbol: &str) {
        self.impulses += 1;
        self.symbol_mut(symbol).impulses += 1;
    }

    fn record_expired(&mut self, symbol: &str) {
        self.expired += 1;
        self.symbol_mut(symbol).expired += 1;
    }

    fn set_trade_estimate(&mut self, symbol: &str, estimate: LastTradeEstimate) {
        self.symbol_mut(symbol).set_trade_estimate(estimate);
    }

    fn add_lag(&mut self, symbol: &str, lag_ms: i64) {
        self.matched += 1;
        self.symbol_mut(symbol).add_lag(lag_ms);
        self.lags_ms.push(lag_ms);
        if self.lags_ms.len() > 10_000 {
            self.lags_ms.drain(0..5_000);
        }
    }

    fn summary(&self) -> String {
        if self.lags_ms.is_empty() {
            return format!(
                "quotes Binance={} MEXC={} impulses={} matched=0 expired={}",
                self.binance_quotes, self.mexc_quotes, self.impulses, self.expired
            );
        }
        let mut xs = self.lags_ms.clone();
        xs.sort_unstable();
        let p = |pct: usize| -> i64 {
            let idx = ((xs.len().saturating_sub(1)) * pct) / 100;
            xs[idx]
        };
        let avg = xs.iter().sum::<i64>() as f64 / xs.len() as f64;
        format!(
            "quotes Binance={} MEXC={} impulses={} matched={} expired={} lag avg={:.0}ms p50={}ms p95={}ms min={}ms max={}ms",
            self.binance_quotes,
            self.mexc_quotes,
            self.impulses,
            self.matched,
            self.expired,
            avg,
            p(50),
            p(95),
            xs[0],
            xs[xs.len() - 1]
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("warn".parse()?))
        .init();

    let config = load_config()?;
    let dashboard = Arc::new(RwLock::new(DashboardSnapshot::default()));
    tokio::spawn(run_dashboard(dashboard.clone()));
    tokio::spawn(run_telegram_login_bot(config.clone()));
    open_dashboard_in_browser();

    let (tx, mut rx) = mpsc::channel::<QuoteUpdate>(20_000);
    tokio::spawn(run_binance(config.clone(), tx.clone()));
    tokio::spawn(run_mexc(config.clone(), tx));

    let mut csv = open_csv(&config.csv_path)?;
    let mut stats_csv = open_stats_csv(&config.stats_csv_path)?;
    let mut slippage_csv = open_slippage_csv(&config.slippage_csv_path)?;
    let mut states: HashMap<String, SymbolState> = config
        .symbols
        .iter()
        .map(|s| (s.clone(), SymbolState::default()))
        .collect();
    let mut stats = Stats::default();
    let mut stats_tick = tokio::time::interval(Duration::from_secs(config.stats_interval_secs));
    let mut dashboard_tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            Some(update) = rx.recv() => {
                handle_quote(update, &config, &mut states, &mut stats, &mut csv, &mut slippage_csv).await?;
            }
            _ = dashboard_tick.tick() => {
                let snapshot = build_dashboard_snapshot(&config, &states, &stats);
                *dashboard.write().await = snapshot;
            }
            _ = stats_tick.tick() => {
                expire_old_pending(&config, &mut states, &mut stats);
                let snapshot = build_dashboard_snapshot(&config, &states, &stats);
                *dashboard.write().await = snapshot;
                print_stats(&config, &states, &stats);
                write_stats_snapshot(&mut stats_csv, &config, &states, &stats)?;
            }
        }
    }
}

fn default_stats_csv_path() -> String {
    "stats_snapshots.csv".to_string()
}

fn default_slippage_csv_path() -> String {
    "slippage_events.csv".to_string()
}

fn default_mexc_taker_fee_bps() -> Decimal {
    Decimal::from(6)
}

fn default_trade_notional_usdt() -> Decimal {
    Decimal::from(300)
}

fn default_telegram_subscribers_path() -> String {
    "telegram_subscribers.json".to_string()
}

fn default_alert_diff_bps() -> Decimal {
    Decimal::new(5, 1)
}

fn load_config() -> Result<Config> {
    let text = std::fs::read_to_string("config.json").context("read config.json")?;
    let mut config: Config = serde_json::from_str(&text).context("parse config.json")?;

    if let Ok(local_text) = std::fs::read_to_string("telegram_config.json") {
        if let Ok(local) = serde_json::from_str::<serde_json::Value>(&local_text) {
            if let Some(v) = local.get("telegram_bot_token").and_then(|x| x.as_str()) {
                config.telegram_bot_token = v.to_string();
            }
            if let Some(v) = local.get("telegram_chat_id").and_then(|x| x.as_str()) {
                config.telegram_chat_id = v.to_string();
            }
            if let Some(v) = local.get("telegram_login_password").and_then(|x| x.as_str()) {
                config.telegram_login_password = v.to_string();
            }
        }
    }

    if config.telegram_bot_token.is_empty() {
        config.telegram_bot_token = std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
    }
    if config.telegram_chat_id.is_empty() {
        config.telegram_chat_id = std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
    }
    if config.telegram_login_password.is_empty() {
        config.telegram_login_password = std::env::var("TELEGRAM_LOGIN_PASSWORD").unwrap_or_default();
    }

    anyhow::ensure!(!config.symbols.is_empty(), "config.symbols must not be empty");
    Ok(config)
}

fn open_stats_csv(path: &str) -> Result<File> {
    let exists = std::path::Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(
            file,
            "utc,symbol,binance_quotes,mexc_quotes,impulses,matched,expired,avg_lag_ms,p50_lag_ms,p95_lag_ms,min_lag_ms,max_lag_ms,binance_mid,mexc_mid,binance_age_ms,mexc_age_ms,pending"
        )?;
    }
    Ok(file)
}

fn open_slippage_csv(path: &str) -> Result<File> {
    let exists = std::path::Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(
            file,
            "utc,symbol,direction,lag_ms,entry_bid,entry_ask,exit_bid,exit_ask,entry_spread_bps,exit_spread_bps,gross_cross_bps,fees_bps,net_cross_bps,estimated_pnl_usdt,binance_move_bps,mexc_move_bps"
        )?;
    }
    Ok(file)
}

fn open_csv(path: &str) -> Result<File> {
    let exists = std::path::Path::new(path).exists();
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(
            file,
            "utc,symbol,direction,lag_ms,binance_move_bps,mexc_move_bps,binance_start_mid,binance_end_mid,mexc_start_mid,mexc_confirm_mid,binance_event_ts_ms,mexc_recv_ts_ms"
        )?;
    }
    Ok(file)
}

async fn handle_quote(
    update: QuoteUpdate,
    config: &Config,
    states: &mut HashMap<String, SymbolState>,
    stats: &mut Stats,
    csv: &mut File,
    slippage_csv: &mut File,
) -> Result<()> {
    let now = update.recv_ts_ms;
    let Some(state) = states.get_mut(&update.symbol) else {
        return Ok(());
    };

    match update.exchange {
        Exchange::Binance => {
            stats.record_quote(&update.symbol, Exchange::Binance);
            let prev = state.last_binance.clone();
            state.last_binance = Some(update.clone());

            let Some(prev) = prev else { return Ok(()); };
            let Some(mexc) = state.last_mexc.clone() else { return Ok(()); };
            let elapsed = update.recv_ts_ms - prev.recv_ts_ms;
            if elapsed < 0 || elapsed > config.impulse_window_ms {
                return Ok(());
            }
            if now - state.last_event_ts_ms < config.event_cooldown_ms {
                return Ok(());
            }

            let prev_mid = prev.mid();
            let new_mid = update.mid();
            if prev_mid <= Decimal::ZERO || new_mid <= Decimal::ZERO {
                return Ok(());
            }
            let move_bps = (new_mid - prev_mid) / prev_mid * Decimal::from(10_000);
            let abs_move_bps = move_bps.abs();
            if abs_move_bps < config.impulse_bps {
                return Ok(());
            }

            let direction = if move_bps > Decimal::ZERO { 1 } else { -1 };
            state.pending = Some(PendingEvent {
                symbol: update.symbol.clone(),
                direction,
                binance_start_mid: prev_mid,
                binance_end_mid: new_mid,
                mexc_start_mid: mexc.mid(),
                mexc_start_bid: mexc.bid,
                mexc_start_ask: mexc.ask,
                binance_event_ts_ms: update.recv_ts_ms,
                created_recv_ts_ms: now,
            });
            state.last_event_ts_ms = now;
            stats.record_impulse(&update.symbol);

        }
        Exchange::Mexc => {
            stats.record_quote(&update.symbol, Exchange::Mexc);
            state.last_mexc = Some(update.clone());
            let Some(pending) = state.pending.clone() else { return Ok(()); };

            if now - pending.created_recv_ts_ms > config.max_lag_ms {
                state.pending = None;
                stats.record_expired(&pending.symbol);
                return Ok(());
            }

            let start = pending.mexc_start_mid;
            let current = update.mid();
            if start <= Decimal::ZERO || current <= Decimal::ZERO {
                return Ok(());
            }
            let mexc_move_bps = (current - start) / start * Decimal::from(10_000);
            let confirmed = if pending.direction > 0 {
                mexc_move_bps >= config.confirm_bps
            } else {
                mexc_move_bps <= -config.confirm_bps
            };
            if !confirmed {
                return Ok(());
            }

            let binance_move_bps = (pending.binance_end_mid - pending.binance_start_mid)
                / pending.binance_start_mid
                * Decimal::from(10_000);
            let record = LagRecord {
                symbol: pending.symbol.clone(),
                direction: pending.direction,
                lag_ms: update.recv_ts_ms - pending.binance_event_ts_ms,
                binance_move_bps,
                mexc_move_bps,
                binance_start_mid: pending.binance_start_mid,
                binance_end_mid: pending.binance_end_mid,
                mexc_start_mid: pending.mexc_start_mid,
                mexc_confirm_mid: current,
                binance_event_ts_ms: pending.binance_event_ts_ms,
                mexc_recv_ts_ms: update.recv_ts_ms,
            };
            write_record(csv, &record)?;
            let estimate = write_slippage_record(slippage_csv, config, &record, &pending, &update)?;
            stats.set_trade_estimate(&record.symbol, estimate);
            stats.add_lag(&record.symbol, record.lag_ms);
            state.pending = None;

        }
    }

    check_price_diff_alert(&update.symbol, config, state).await;

    Ok(())
}

async fn check_price_diff_alert(symbol: &str, config: &Config, state: &mut SymbolState) {
    let (Some(binance), Some(mexc)) = (state.last_binance.as_ref(), state.last_mexc.as_ref()) else {
        return;
    };
    if config.alert_diff_bps <= Decimal::ZERO {
        return;
    }

    let b_mid = binance.mid();
    let m_mid = mexc.mid();
    if b_mid <= Decimal::ZERO || m_mid <= Decimal::ZERO {
        return;
    }

    let diff_usdt = m_mid - b_mid;
    let diff_bps = diff_usdt / b_mid * Decimal::from(10_000);
    let abs_diff_bps = diff_bps.abs();
    let now = now_ms();

    if abs_diff_bps >= config.alert_diff_bps {
        match state.active_diff_alert.as_mut() {
            Some(active) => {
                if abs_diff_bps > active.max_abs_diff_bps {
                    active.max_abs_diff_bps = abs_diff_bps;
                }
            }
            None => {
                let direction = if diff_usdt > Decimal::ZERO {
                    "MEXC выше Binance".to_string()
                } else {
                    "MEXC ниже Binance".to_string()
                };
                state.active_diff_alert = Some(ActiveDiffAlert {
                    started_ms: now,
                    direction: direction.clone(),
                    start_diff_bps: diff_bps,
                    start_diff_usdt: diff_usdt,
                    max_abs_diff_bps: abs_diff_bps,
                });
                let text = format!(
                    "Найдена разница {symbol}\n{direction}\nBinance: {b}\nMEXC: {m}\nРазница: {du} USDT / {db} bps\nПорог: {thr} bps",
                    symbol = symbol,
                    direction = direction,
                    b = b_mid.round_dp(6),
                    m = m_mid.round_dp(6),
                    du = diff_usdt.round_dp(6),
                    db = diff_bps.round_dp(3),
                    thr = config.alert_diff_bps,
                );
                send_telegram_to_subscribers(config, &text).await;
            }
        }
    } else if let Some(active) = state.active_diff_alert.take() {
        let duration_ms = now - active.started_ms;
        let text = format!(
            "Разница закрылась {symbol}\nНаправление: {direction}\nДержалась: {secs:.2} сек\nСтарт: {start_usdt} USDT / {start_bps} bps\nМакс: {max_bps} bps\nФинал: {final_usdt} USDT / {final_bps} bps",
            symbol = symbol,
            direction = active.direction,
            secs = duration_ms as f64 / 1000.0,
            start_usdt = active.start_diff_usdt.round_dp(6),
            start_bps = active.start_diff_bps.round_dp(3),
            max_bps = active.max_abs_diff_bps.round_dp(3),
            final_usdt = diff_usdt.round_dp(6),
            final_bps = diff_bps.round_dp(3),
        );
        send_telegram_to_subscribers(config, &text).await;
    }
}

async fn send_telegram_to_subscribers(config: &Config, text: &str) {
    if config.telegram_bot_token.is_empty() {
        eprintln!("[telegram] skip send: bot token is empty");
        return;
    }
    let subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
    if subscribers.is_empty() {
        eprintln!(
            "[telegram] skip send: no subscribers in {}",
            config.telegram_subscribers_path
        );
        return;
    }
    eprintln!("[telegram] sending alert to {} subscriber(s)", subscribers.len());
    for chat_id in subscribers {
        if let Err(e) = telegram_send_message(config, &chat_id, text).await {
            eprintln!("[telegram] send to {} failed: {:#}", chat_id, e);
        }
    }
}
fn expire_old_pending(config: &Config, states: &mut HashMap<String, SymbolState>, stats: &mut Stats) {
    let now = now_ms();
    for (symbol, state) in states.iter_mut() {
        if let Some(pending) = &state.pending {
            if now - pending.created_recv_ts_ms > config.max_lag_ms {
                let expired_symbol = symbol.clone();
                state.pending = None;
                stats.record_expired(&expired_symbol);
            }
        }
    }
}

fn print_stats(config: &Config, states: &HashMap<String, SymbolState>, stats: &Stats) {
    let mut stdout = io::stdout();
    let _ = execute!(stdout, Clear(ClearType::All), MoveTo(0, 0));
    println!("=== MEXC LAG MONITOR {} | symbols={} ===", Utc::now().format("%Y-%m-%d %H:%M:%S UTC"), config.symbols.len());
    println!("{}", stats.summary());
    println!(
        "thresholds: impulse>={}bps confirm>={}bps max_lag={}ms window={}ms",
        config.impulse_bps, config.confirm_bps, config.max_lag_ms, config.impulse_window_ms
    );
    println!(
        "{:<12} {:>8} {:>8} {:>7} {:>7} {:>7} {:>8} {:>8} {:>8} {:>12} {:>12} {:>8}",
        "symbol", "bnc_q", "mexc_q", "imp", "match", "exp", "avg", "p50", "p95", "bnc_mid", "mexc_mid", "pending"
    );

    for symbol in &config.symbols {
        let symbol_stats = stats.by_symbol.get(symbol).cloned().unwrap_or_default();
        let (avg, p50, p95) = symbol_stats
            .lag_summary()
            .map(|(avg, p50, p95, _min, _max)| (format!("{avg:.0}"), p50.to_string(), p95.to_string()))
            .unwrap_or_else(|| ("-".to_string(), "-".to_string(), "-".to_string()));
        let state = states.get(symbol);
        let b_mid = state
            .and_then(|s| s.last_binance.as_ref())
            .map(|q| q.mid().round_dp(4).to_string())
            .unwrap_or_else(|| "-".to_string());
        let m_mid = state
            .and_then(|s| s.last_mexc.as_ref())
            .map(|q| q.mid().round_dp(4).to_string())
            .unwrap_or_else(|| "-".to_string());
        let pending = state
            .and_then(|s| s.pending.as_ref())
            .map(|p| if p.direction > 0 { "UP" } else { "DOWN" })
            .unwrap_or("-");

        println!(
            "{:<12} {:>8} {:>8} {:>7} {:>7} {:>7} {:>8} {:>8} {:>8} {:>12} {:>12} {:>8}",
            symbol,
            symbol_stats.binance_quotes,
            symbol_stats.mexc_quotes,
            symbol_stats.impulses,
            symbol_stats.matched,
            symbol_stats.expired,
            avg,
            p50,
            p95,
            b_mid,
            m_mid,
            pending
        );
    }
}

fn write_stats_snapshot(
    file: &mut File,
    config: &Config,
    states: &HashMap<String, SymbolState>,
    stats: &Stats,
) -> Result<()> {
    let now = now_ms();
    let utc = Utc::now().to_rfc3339();
    for symbol in &config.symbols {
        let symbol_stats = stats.by_symbol.get(symbol).cloned().unwrap_or_default();
        let (avg, p50, p95, min, max) = symbol_stats
            .lag_summary()
            .map(|(avg, p50, p95, min, max)| {
                (
                    format!("{avg:.0}"),
                    p50.to_string(),
                    p95.to_string(),
                    min.to_string(),
                    max.to_string(),
                )
            })
            .unwrap_or_else(|| ("".to_string(), "".to_string(), "".to_string(), "".to_string(), "".to_string()));
        let state = states.get(symbol);
        let binance_mid = state
            .and_then(|s| s.last_binance.as_ref())
            .map(|q| q.mid().to_string())
            .unwrap_or_default();
        let mexc_mid = state
            .and_then(|s| s.last_mexc.as_ref())
            .map(|q| q.mid().to_string())
            .unwrap_or_default();
        let binance_age = state
            .and_then(|s| s.last_binance.as_ref())
            .map(|q| (now - q.recv_ts_ms).to_string())
            .unwrap_or_default();
        let mexc_age = state
            .and_then(|s| s.last_mexc.as_ref())
            .map(|q| (now - q.recv_ts_ms).to_string())
            .unwrap_or_default();
        let pending = state.and_then(|s| s.pending.as_ref()).is_some();

        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            utc,
            symbol,
            symbol_stats.binance_quotes,
            symbol_stats.mexc_quotes,
            symbol_stats.impulses,
            symbol_stats.matched,
            symbol_stats.expired,
            avg,
            p50,
            p95,
            min,
            max,
            binance_mid,
            mexc_mid,
            binance_age,
            mexc_age,
            pending
        )?;
    }
    file.flush()?;
    Ok(())
}
fn spread_bps(bid: Decimal, ask: Decimal) -> Decimal {
    let mid = (bid + ask) / Decimal::from(2);
    if mid <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (ask - bid) / mid * Decimal::from(10_000)
    }
}

fn write_slippage_record(
    file: &mut File,
    config: &Config,
    r: &LagRecord,
    pending: &PendingEvent,
    mexc_confirm: &QuoteUpdate,
) -> Result<LastTradeEstimate> {
    let entry_bid = pending.mexc_start_bid;
    let entry_ask = pending.mexc_start_ask;
    let exit_bid = mexc_confirm.bid;
    let exit_ask = mexc_confirm.ask;
    let entry_spread_bps = spread_bps(entry_bid, entry_ask);
    let exit_spread_bps = spread_bps(exit_bid, exit_ask);

    let gross_cross_bps = if r.direction > 0 {
        if entry_ask <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            (exit_bid - entry_ask) / entry_ask * Decimal::from(10_000)
        }
    } else if entry_bid <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (entry_bid - exit_ask) / entry_bid * Decimal::from(10_000)
    };

    let fees_bps = config.mexc_taker_fee_bps * Decimal::from(2);
    let net_cross_bps = gross_cross_bps - fees_bps;
    let estimated_pnl_usdt = config.trade_notional_usdt * net_cross_bps / Decimal::from(10_000);
    let pnl_zero_fee_usdt = config.trade_notional_usdt * gross_cross_bps / Decimal::from(10_000);

    writeln!(
        file,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        Utc::now().to_rfc3339(),
        r.symbol,
        if r.direction > 0 { "up" } else { "down" },
        r.lag_ms,
        entry_bid,
        entry_ask,
        exit_bid,
        exit_ask,
        entry_spread_bps,
        exit_spread_bps,
        gross_cross_bps,
        fees_bps,
        net_cross_bps,
        estimated_pnl_usdt,
        r.binance_move_bps,
        r.mexc_move_bps
    )?;
    file.flush()?;

    Ok(LastTradeEstimate {
        direction: if r.direction > 0 { "UP".to_string() } else { "DOWN".to_string() },
        lag_ms: r.lag_ms,
        gross_cross_bps,
        net_fee_bps: net_cross_bps,
        net_zero_fee_bps: gross_cross_bps,
        pnl_fee_usdt: estimated_pnl_usdt,
        pnl_zero_fee_usdt,
    })
}
fn write_record(csv: &mut File, r: &LagRecord) -> Result<()> {
    writeln!(
        csv,
        "{},{},{},{},{},{},{},{},{},{},{},{}",
        Utc::now().to_rfc3339(),
        r.symbol,
        if r.direction > 0 { "up" } else { "down" },
        r.lag_ms,
        r.binance_move_bps,
        r.mexc_move_bps,
        r.binance_start_mid,
        r.binance_end_mid,
        r.mexc_start_mid,
        r.mexc_confirm_mid,
        r.binance_event_ts_ms,
        r.mexc_recv_ts_ms
    )?;
    csv.flush()?;
    Ok(())
}

async fn run_binance(config: Config, tx: mpsc::Sender<QuoteUpdate>) {
    let symbols: HashSet<String> = config.symbols.iter().cloned().collect();
    let url = format!("{}?streams=!bookTicker", config.binance_ws);

    loop {
        let _ = binance_session(&url, &symbols, &tx).await;
        sleep(Duration::from_secs(2)).await;
    }
}

async fn binance_session(
    url: &str,
    symbols: &HashSet<String>,
    tx: &mpsc::Sender<QuoteUpdate>,
) -> Result<()> {
    let (ws, _) = connect_async(url).await?;
    let (_, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            for update in parse_binance_book_tickers(&text) {
                if symbols.contains(&update.symbol) && tx.send(update).await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn run_mexc(config: Config, tx: mpsc::Sender<QuoteUpdate>) {
    loop {
        let _ = mexc_session(&config, &tx).await;
        sleep(Duration::from_secs(2)).await;
    }
}

async fn mexc_session(config: &Config, tx: &mpsc::Sender<QuoteUpdate>) -> Result<()> {
    let (ws, _) = connect_async(config.mexc_ws.as_str()).await?;
    let (mut write, mut read) = ws.split();

    for symbol in &config.symbols {
        let sub = serde_json::json!({
            "method": "sub.depth.full",
            "param": { "symbol": symbol, "limit": 5 }
        });
        write.send(Message::Text(sub.to_string())).await?;
        sleep(Duration::from_millis(80)).await;
    }

    let mut ping_tick = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = ping_tick.tick() => {
                let ping = serde_json::json!({ "method": "ping" });
                write.send(Message::Text(ping.to_string())).await?;
            }
            msg = read.next() => {
                let Some(msg) = msg else { break; };
                let msg = msg?;
                if let Message::Text(text) = msg {
                    if let Some(update) = parse_mexc_depth(&text) {
                        if tx.send(update).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid: String,
    #[serde(rename = "a")]
    ask: String,
}

fn parse_binance_book_tickers(text: &str) -> Vec<QuoteUpdate> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
        return Vec::new();
    };

    let data = value.get("data").cloned().unwrap_or(value);
    match data {
        serde_json::Value::Array(items) => items
            .into_iter()
            .filter_map(parse_binance_book_ticker_value)
            .collect(),
        value => parse_binance_book_ticker_value(value).into_iter().collect(),
    }
}

fn parse_binance_book_ticker_value(value: serde_json::Value) -> Option<QuoteUpdate> {
    let data: BinanceBookTicker = serde_json::from_value(value).ok()?;
    let bid = Decimal::from_str(&data.bid).ok()?;
    let ask = Decimal::from_str(&data.ask).ok()?;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
        return None;
    }
    Some(QuoteUpdate {
        exchange: Exchange::Binance,
        symbol: normalize_binance_symbol(&data.symbol),
        bid,
        ask,
        recv_ts_ms: now_ms(),
    })
}

#[derive(Debug, Deserialize)]
struct MexcDepthMsg {
    channel: Option<String>,
    symbol: Option<String>,
    data: Option<MexcDepthData>,
}

#[derive(Debug, Deserialize)]
struct MexcDepthData {
    bids: Vec<Vec<Decimal>>,
    asks: Vec<Vec<Decimal>>,
}

fn parse_mexc_depth(text: &str) -> Option<QuoteUpdate> {
    let msg: MexcDepthMsg = serde_json::from_str(text).ok()?;
    if msg.channel.as_deref() != Some("push.depth.full") && msg.channel.as_deref() != Some("push.depth") {
        return None;
    }
    let data = msg.data?;
    let best_bid = best_price(&data.bids, true)?;
    let best_ask = best_price(&data.asks, false)?;
    if best_bid <= Decimal::ZERO || best_ask <= Decimal::ZERO || best_bid >= best_ask {
        return None;
    }
    Some(QuoteUpdate {
        exchange: Exchange::Mexc,
        symbol: msg.symbol?,
        bid: best_bid,
        ask: best_ask,
        recv_ts_ms: now_ms(),
    })
}

fn best_price(levels: &[Vec<Decimal>], is_bid: bool) -> Option<Decimal> {
    levels
        .iter()
        .filter_map(|level| level.first().copied())
        .filter(|p| *p > Decimal::ZERO)
        .reduce(|best, price| {
            if is_bid {
                best.max(price)
            } else {
                best.min(price)
            }
        })
}

fn normalize_binance_symbol(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .map(|base| format!("{}_USDT", base))
        .unwrap_or_else(|| symbol.to_string())
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

















fn build_dashboard_snapshot(
    config: &Config,
    states: &HashMap<String, SymbolState>,
    stats: &Stats,
) -> DashboardSnapshot {
    let symbols = config
        .symbols
        .iter()
        .map(|symbol| {
            let symbol_stats = stats.by_symbol.get(symbol).cloned().unwrap_or_default();
            let lag = symbol_stats.lag_summary();
            let state = states.get(symbol);
            let b_mid_dec = state.and_then(|s| s.last_binance.as_ref()).map(|q| q.mid());
            let m_mid_dec = state.and_then(|s| s.last_mexc.as_ref()).map(|q| q.mid());
            let price_diff = match (b_mid_dec, m_mid_dec) {
                (Some(b), Some(m)) => Some(m - b),
                _ => None,
            };
            let price_diff_bps = match (b_mid_dec, price_diff) {
                (Some(b), Some(d)) if b > Decimal::ZERO => Some(d / b * Decimal::from(10_000)),
                _ => None,
            };
            let last_trade = symbol_stats.last_trade.clone();
            DashboardSymbolRow {
                symbol: symbol.clone(),
                binance_quotes: symbol_stats.binance_quotes,
                mexc_quotes: symbol_stats.mexc_quotes,
                impulses: symbol_stats.impulses,
                matched: symbol_stats.matched,
                expired: symbol_stats.expired,
                avg_lag_ms: lag.map(|x| x.0.round() as i64),
                p50_lag_ms: lag.map(|x| x.1),
                p95_lag_ms: lag.map(|x| x.2),
                binance_mid: state
                    .and_then(|s| s.last_binance.as_ref())
                    .map(|q| q.mid().round_dp(6).to_string()),
                mexc_mid: state
                    .and_then(|s| s.last_mexc.as_ref())
                    .map(|q| q.mid().round_dp(6).to_string()),
                price_diff_usdt: price_diff.map(|x| x.round_dp(6).to_string()),
                price_diff_bps: price_diff_bps.map(|x| x.round_dp(3).to_string()),
                pending: state
                    .and_then(|s| s.pending.as_ref())
                    .map(|p| if p.direction > 0 { "UP".to_string() } else { "DOWN".to_string() }),
                last_direction: last_trade.as_ref().map(|x| x.direction.clone()),
                last_lag_ms: last_trade.as_ref().map(|x| x.lag_ms),
                gross_bps: last_trade.as_ref().map(|x| x.gross_cross_bps.round_dp(3).to_string()),
                net_fee_bps: last_trade.as_ref().map(|x| x.net_fee_bps.round_dp(3).to_string()),
                net_zero_fee_bps: last_trade.as_ref().map(|x| x.net_zero_fee_bps.round_dp(3).to_string()),
                pnl_fee_usdt: last_trade.as_ref().map(|x| x.pnl_fee_usdt.round_dp(4).to_string()),
                pnl_zero_fee_usdt: last_trade.as_ref().map(|x| x.pnl_zero_fee_usdt.round_dp(4).to_string()),
            }
        })
        .collect();

    DashboardSnapshot {
        updated_at: Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        summary: stats.summary(),
        impulse_bps: config.impulse_bps.to_string(),
        confirm_bps: config.confirm_bps.to_string(),
        max_lag_ms: config.max_lag_ms,
        symbols,
    }
}

async fn run_dashboard(state: SharedDashboard) {
    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/state", get(dashboard_state))
        .layer(CorsLayer::permissive())
        .with_state(state);
    let addr = SocketAddr::from(([127, 0, 0, 1], 8787));
    let Ok(listener) = tokio::net::TcpListener::bind(addr).await else {
        return;
    };
    let _ = axum::serve(listener, app).await;
}

async fn dashboard_state(State(state): State<SharedDashboard>) -> Json<DashboardSnapshot> {
    Json(state.read().await.clone())
}

async fn dashboard_html() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

fn open_dashboard_in_browser() {
    tokio::spawn(async {
        sleep(Duration::from_millis(800)).await;
        #[cfg(target_os = "windows")]
        let _ = std::process::Command::new("cmd")
            .args(["/C", "start", "", "http://127.0.0.1:8787"])
            .spawn();
        #[cfg(target_os = "macos")]
        let _ = std::process::Command::new("open")
            .arg("http://127.0.0.1:8787")
            .spawn();
        #[cfg(all(unix, not(target_os = "macos")))]
        let _ = std::process::Command::new("xdg-open")
            .arg("http://127.0.0.1:8787")
            .spawn();
    });
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Монитор задержки MEXC</title>
  <style>
    :root { color-scheme: dark; font-family: Segoe UI, Arial, sans-serif; background:#0f1115; color:#e7eaf0; }
    body { margin:0; background:#0f1115; }
    header { position:sticky; top:0; background:#151923; border-bottom:1px solid #2a3040; padding:14px 18px; z-index:2; }
    h1 { margin:0 0 8px; font-size:20px; font-weight:650; }
    .meta { display:flex; flex-wrap:wrap; gap:10px 18px; color:#aab3c5; font-size:13px; }
    main { padding:16px 18px 28px; }
    .summary { display:grid; grid-template-columns: repeat(4, minmax(140px, 1fr)); gap:10px; margin-bottom:14px; }
    .box { background:#171c27; border:1px solid #2a3040; border-radius:6px; padding:10px 12px; }
    .box b { display:block; font-size:18px; margin-top:4px; color:#fff; }
    table { width:100%; border-collapse:collapse; font-size:13px; background:#121722; border:1px solid #2a3040; }
    th, td { padding:7px 8px; border-bottom:1px solid #242b3a; text-align:right; white-space:nowrap; }
    th:first-child, td:first-child { text-align:left; position:sticky; left:0; background:#121722; }
    th { color:#9eabc0; background:#171c27; position:sticky; top:79px; z-index:1; }
    tr.hot td { background:#18251d; }
    tr.pending td { background:#242016; }
    a { color:#8fb7ff; text-decoration:none; }
    .links { margin-top:12px; display:flex; gap:14px; color:#aab3c5; font-size:13px; }
    .muted { color:#7d8799; }
  </style>
</head>
<body>
<header>
  <h1>Монитор задержки Binance -> MEXC</h1>
  <div class="meta">
    <span id="updated">ожидание данных</span>
    <span id="thresholds"></span>
    <span>Файлы: lag_events.csv / slippage_events.csv / stats_snapshots.csv</span>
  </div>
</header>
<main>
  <section class="summary">
    <div class="box">Тики Binance<b id="binanceQuotes">0</b></div>
    <div class="box">Тики MEXC<b id="mexcQuotes">0</b></div>
    <div class="box">Подтвержденные события<b id="matched">0</b></div>
    <div class="box">Средняя задержка<b id="avgLag">-</b></div>
  </section>
  <table>
    <thead><tr>
      <th>Пара</th><th>Цена Binance</th><th>Цена MEXC</th><th>Разница $</th><th>Разница bps</th><th>Имп.</th><th>Совп.</th><th>Средн.</th><th>P95</th><th>Net fee</th><th>Net 0 fee</th><th>PnL 0 fee</th><th>Ожидание</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
</main>
<script>
function fmt(v, suffix='') { return v === null || v === undefined ? '-' : `${v}${suffix}`; }
function sum(rows, key) { return rows.reduce((a, r) => a + (r[key] || 0), 0); }
async function refresh() {
  const res = await fetch('/api/state', { cache: 'no-store' });
  const data = await res.json();
  const rows = data.symbols || [];
  document.getElementById('updated').textContent = `Обновлено: ${data.updated_at || '-'}`;
  document.getElementById('thresholds').textContent = `импульс ${data.impulse_bps} bps | подтверждение ${data.confirm_bps} bps | макс. задержка ${data.max_lag_ms} мс`;
  document.getElementById('binanceQuotes').textContent = sum(rows, 'binance_quotes');
  document.getElementById('mexcQuotes').textContent = sum(rows, 'mexc_quotes');
  document.getElementById('matched').textContent = sum(rows, 'matched');
  const lags = rows.map(r => r.avg_lag_ms).filter(v => v !== null && v !== undefined);
  document.getElementById('avgLag').textContent = lags.length ? `${Math.round(lags.reduce((a,b)=>a+b,0)/lags.length)} ms` : '-';
  document.getElementById('rows').innerHTML = rows.map(r => `
    <tr class="${r.matched ? 'hot' : ''} ${r.pending ? 'pending' : ''}">
      <td>${r.symbol}</td><td>${fmt(r.binance_mid)}</td><td>${fmt(r.mexc_mid)}</td><td>${fmt(r.price_diff_usdt)}</td><td>${fmt(r.price_diff_bps)}</td>
      <td>${r.impulses}</td><td>${r.matched}</td><td>${fmt(r.avg_lag_ms, ' мс')}</td><td>${fmt(r.p95_lag_ms, ' мс')}</td>
      <td>${fmt(r.net_fee_bps)}</td><td>${fmt(r.net_zero_fee_bps)}</td><td>${fmt(r.pnl_zero_fee_usdt, ' USDT')}</td>
      <td>${fmt(r.pending === 'UP' ? 'ВВЕРХ' : (r.pending === 'DOWN' ? 'ВНИЗ' : r.pending))}</td>
    </tr>`).join('');
}
setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>"#;










fn load_telegram_subscribers(path: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<Vec<String>>(&text).ok())
        .unwrap_or_default()
}

fn save_telegram_subscribers(path: &str, subscribers: &[String]) -> Result<()> {
    let text = serde_json::to_string_pretty(subscribers)?;
    std::fs::write(path, text)?;
    Ok(())
}

async fn telegram_send_message(config: &Config, chat_id: &str, text: &str) -> Result<()> {
    if config.telegram_bot_token.is_empty() || chat_id.is_empty() {
        return Ok(());
    }
    let url = format!("https://api.telegram.org/bot{}/sendMessage", config.telegram_bot_token);
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": true
        }))
        .send()
        .await
        .context("telegram sendMessage request failed")?;
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("telegram sendMessage HTTP {}: {}", status, body);
    }
    Ok(())
}

async fn run_telegram_login_bot(config: Config) {
    if config.telegram_bot_token.is_empty() || config.telegram_login_password.is_empty() {
        return;
    }

    let client = reqwest::Client::new();
    let mut offset: i64 = 0;
    loop {
        let url = format!("https://api.telegram.org/bot{}/getUpdates", config.telegram_bot_token);
        let response = client
            .get(&url)
            .query(&[("timeout", "20"), ("offset", &offset.to_string())])
            .send()
            .await;

        if let Ok(resp) = response {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(updates) = json.get("result").and_then(|x| x.as_array()) {
                    for update in updates {
                        if let Some(update_id) = update.get("update_id").and_then(|x| x.as_i64()) {
                            offset = update_id + 1;
                        }
                        let Some(message) = update.get("message") else { continue; };
                        let Some(text) = message.get("text").and_then(|x| x.as_str()) else { continue; };
                        let Some(chat_id) = message
                            .get("chat")
                            .and_then(|x| x.get("id"))
                            .and_then(|x| x.as_i64())
                            .map(|x| x.to_string()) else { continue; };

                        if text.trim() == "/logout" {
                            let mut subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
                            subscribers.retain(|x| x != &chat_id);
                            let _ = save_telegram_subscribers(&config.telegram_subscribers_path, &subscribers);
                            let _ = telegram_send_message(&config, &chat_id, "Вы вышли из рассылки отчетов.").await;
                            continue;
                        }

                        if text.trim() == "/status" {
                            let subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
                            let msg = if subscribers.contains(&chat_id) {
                                "Вы залогинены и будете получать отчеты."
                            } else {
                                "Вы не залогинены. Используйте /login password."
                            };
                            let _ = telegram_send_message(&config, &chat_id, msg).await;
                            continue;
                        }

                        if let Some(password) = text.trim().strip_prefix("/login ") {
                            if password.trim() == config.telegram_login_password {
                                let mut subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
                                if !subscribers.contains(&chat_id) {
                                    subscribers.push(chat_id.clone());
                                    let _ = save_telegram_subscribers(&config.telegram_subscribers_path, &subscribers);
                                }
                                let _ = telegram_send_message(&config, &chat_id, "Логин успешен. Вы будете получать отчеты каждые 5 часов.").await;
                            } else {
                                let _ = telegram_send_message(&config, &chat_id, "Неверный пароль.").await;
                            }
                        }
                    }
                }
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}








