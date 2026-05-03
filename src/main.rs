use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info};

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

#[derive(Default)]
struct SymbolState {
    last_binance: Option<QuoteUpdate>,
    last_mexc: Option<QuoteUpdate>,
    pending: Option<PendingEvent>,
    last_event_ts_ms: i64,
}

#[derive(Default, Clone)]
struct SymbolStats {
    binance_quotes: u64,
    mexc_quotes: u64,
    impulses: u64,
    matched: u64,
    expired: u64,
    lags_ms: Vec<i64>,
}

impl SymbolStats {
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
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let config = load_config()?;
    info!("starting MEXC lag monitor for symbols: {}", config.symbols.join(", "));

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

    loop {
        tokio::select! {
            Some(update) = rx.recv() => {
                handle_quote(update, &config, &mut states, &mut stats, &mut csv, &mut slippage_csv)?;
            }
            _ = stats_tick.tick() => {
                expire_old_pending(&config, &mut states, &mut stats);
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

fn load_config() -> Result<Config> {
    let text = std::fs::read_to_string("config.json").context("read config.json")?;
    let config: Config = serde_json::from_str(&text).context("parse config.json")?;
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
            "utc,symbol,direction,lag_ms,entry_bid,entry_ask,exit_bid,exit_ask,entry_spread_bps,exit_spread_bps,gross_cross_bps,fees_bps,net_cross_bps,binance_move_bps,mexc_move_bps"
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

fn handle_quote(
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
            write_slippage_record(slippage_csv, config, &record, &pending, &update)?;
            stats.add_lag(&record.symbol, record.lag_ms);
            state.pending = None;

        }
    }

    Ok(())
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
    print!("\x1B[2J\x1B[H");
    let _ = io::stdout().flush();
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
) -> Result<()> {
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

    writeln!(
        file,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
        r.binance_move_bps,
        r.mexc_move_bps
    )?;
    file.flush()?;

    Ok(())
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
    let streams = config
        .symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.replace('_', "").to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
    let url = format!("{}?streams={}", config.binance_ws, streams);

    loop {
        if let Err(e) = binance_session(&url, &tx).await {
            error!("Binance session error: {e:#}");
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn binance_session(url: &str, tx: &mpsc::Sender<QuoteUpdate>) -> Result<()> {
    info!("connecting Binance {url}");
    let (ws, _) = connect_async(url).await?;
    info!("connected Binance");
    let (_, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let Message::Text(text) = msg {
            if let Some(update) = parse_binance_book_ticker(&text) {
                if tx.send(update).await.is_err() {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn run_mexc(config: Config, tx: mpsc::Sender<QuoteUpdate>) {
    loop {
        if let Err(e) = mexc_session(&config, &tx).await {
            error!("MEXC session error: {e:#}");
        }
        sleep(Duration::from_secs(2)).await;
    }
}

async fn mexc_session(config: &Config, tx: &mpsc::Sender<QuoteUpdate>) -> Result<()> {
    info!("connecting MEXC {}", config.mexc_ws);
    let (ws, _) = connect_async(config.mexc_ws.as_str()).await?;
    info!("connected MEXC");
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
struct BinanceCombined {
    data: BinanceBookTicker,
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

fn parse_binance_book_ticker(text: &str) -> Option<QuoteUpdate> {
    let wrapper: BinanceCombined = serde_json::from_str(text).ok()?;
    let bid = Decimal::from_str(&wrapper.data.bid).ok()?;
    let ask = Decimal::from_str(&wrapper.data.ask).ok()?;
    if bid <= Decimal::ZERO || ask <= Decimal::ZERO || bid >= ask {
        return None;
    }
    Some(QuoteUpdate {
        exchange: Exchange::Binance,
        symbol: normalize_binance_symbol(&wrapper.data.symbol),
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













