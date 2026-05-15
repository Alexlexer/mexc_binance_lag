mod mexc_trade;
use mexc_trade::MexcTradeClient;
use anyhow::{Context, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{ConnectInfo, Query, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use crossterm::{cursor::MoveTo, execute, terminal::{Clear, ClearType}};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::net::SocketAddr;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::str::FromStr;
use tokio::sync::{mpsc, Mutex, RwLock};
use tower_http::cors::CorsLayer;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};



static TELEGRAM_BLOCKED_UNTIL_MS: AtomicI64 = AtomicI64::new(0);

// ── Auth ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    username: String,
    hash: String,
}

struct Session {
    #[allow(dead_code)]
    username: String,
    expires_ms: i64,
}

#[derive(Default)]
struct LoginAttempts {
    records: HashMap<String, (u32, i64)>, // ip -> (fail_count, blocked_until_ms)
}

impl LoginAttempts {
    fn is_blocked(&self, ip: &str) -> bool {
        if let Some(&(count, blocked_until)) = self.records.get(ip) {
            count >= 5 && now_ms() < blocked_until
        } else {
            false
        }
    }

    fn record_failure(&mut self, ip: &str) {
        let entry = self.records.entry(ip.to_string()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 = now_ms() + 15 * 60 * 1_000;
    }

    fn record_success(&mut self, ip: &str) {
        self.records.remove(ip);
    }
}

type SharedSessions = Arc<RwLock<HashMap<String, Session>>>;
type SharedLoginAttempts = Arc<Mutex<LoginAttempts>>;

// ── Dashboard / app state ────────────────────────────────────────────────────

type SharedDashboard = Arc<RwLock<DashboardSnapshot>>;
type SharedTradeClient = Arc<RwLock<Option<Arc<MexcTradeClient>>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LiveConfig {
    impulse_bps: Decimal,
    confirm_bps: Decimal,
    alert_diff_bps: Decimal,
    max_lag_ms: i64,
    trade_margin_usdt: Decimal,
    trade_leverage: i32,
    trade_enabled: bool,
}

type SharedLiveConfig = Arc<RwLock<LiveConfig>>;

#[derive(Clone)]
struct AppState {
    dashboard: SharedDashboard,
    live_cfg: SharedLiveConfig,
    sessions: SharedSessions,
    users: Arc<Vec<User>>,
    login_attempts: SharedLoginAttempts,
    trade_client: SharedTradeClient,
    slippage_csv_path: Arc<String>,
    config_path: Arc<String>,
}

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
    #[serde(default = "default_alert_min_edge_bps")]
    alert_min_edge_bps: Decimal,
    #[serde(default = "default_alert_min_duration_ms")]
    alert_min_duration_ms: i64,
    #[serde(default = "default_max_quote_age_ms")]
    max_quote_age_ms: i64,
    #[serde(default)]
    mexc_api_key: String,
    #[serde(default)]
    mexc_api_secret: String,
    #[serde(default)]
    trade_enabled: bool,
    #[serde(default = "default_trade_vol")]
    trade_vol: String,
    #[serde(default = "default_trade_leverage")]
    trade_leverage: i32,
    #[serde(default = "default_trade_timeout_ms")]
    trade_timeout_ms: u64,
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
    start_edge_zero_fee_bps: Decimal,
    max_edge_zero_fee_bps: Decimal,
    notified: bool,
}

#[derive(Debug, Clone, Copy)]
struct DiffEdgeEstimate {
    mexc_spread_bps: Decimal,
    edge_zero_fee_bps: Decimal,
    pnl_zero_fee_usdt: Decimal,
}

#[derive(Default)]
struct SymbolState {
    last_binance: Option<QuoteUpdate>,
    last_mexc: Option<QuoteUpdate>,
    pending: Option<PendingEvent>,
    last_event_ts_ms: i64,
    active_diff_alert: Option<ActiveDiffAlert>,
    active_trade_close: Option<tokio::sync::oneshot::Sender<()>>,
    last_diff_alert_closed_ms: i64,
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

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--add-user") {
        return add_user_interactive();
    }

    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env().add_directive("warn".parse()?))
        .init();

    let config = load_config()?;
    let users = load_users();

    let live_cfg: SharedLiveConfig = Arc::new(RwLock::new(LiveConfig {
        impulse_bps: config.impulse_bps,
        confirm_bps: config.confirm_bps,
        alert_diff_bps: config.alert_diff_bps,
        max_lag_ms: config.max_lag_ms,
        trade_margin_usdt: config.trade_notional_usdt,
        trade_leverage: config.trade_leverage,
        trade_enabled: config.trade_enabled,
    }));
    let dashboard = Arc::new(RwLock::new(DashboardSnapshot::default()));
    let sessions: SharedSessions = Arc::new(RwLock::new(HashMap::new()));
    let login_attempts: SharedLoginAttempts = Arc::new(Mutex::new(LoginAttempts::default()));

    let initial_client: Option<Arc<MexcTradeClient>> = if !config.mexc_api_key.is_empty() {
        eprintln!("[trade] API keys loaded, client active");
        Some(Arc::new(MexcTradeClient::new(config.mexc_api_key.clone(), config.mexc_api_secret.clone(), 1)))
    } else {
        eprintln!("[trade] no API keys — set them in the dashboard");
        None
    };
    let trade_client: SharedTradeClient = Arc::new(RwLock::new(initial_client));

    let app_state = AppState {
        dashboard: dashboard.clone(),
        live_cfg: live_cfg.clone(),
        sessions,
        users: Arc::new(users),
        login_attempts,
        trade_client: trade_client.clone(),
        slippage_csv_path: Arc::new(config.slippage_csv_path.clone()),
        config_path: Arc::new("config.json".to_string()),
    };

    tokio::spawn(run_dashboard(app_state));
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
                let tc = trade_client.read().await.clone();
                handle_quote(update, &config, &mut states, &mut stats, &mut csv, &mut slippage_csv, tc, &live_cfg).await?;
            }
            _ = dashboard_tick.tick() => {
                let live = live_cfg.read().await.clone();
                let snapshot = build_dashboard_snapshot(&config, &live, &states, &stats);
                *dashboard.write().await = snapshot;
            }
            _ = stats_tick.tick() => {
                let live = live_cfg.read().await.clone();
                expire_old_pending(&config, &live, &mut states, &mut stats);
                let snapshot = build_dashboard_snapshot(&config, &live, &states, &stats);
                *dashboard.write().await = snapshot;
                print_stats(&config, &live, &states, &stats);
                write_stats_snapshot(&mut stats_csv, &config, &states, &stats)?;
            }
        }
    }
}

// ── Config defaults ───────────────────────────────────────────────────────────

fn default_stats_csv_path() -> String { "stats_snapshots.csv".to_string() }
fn default_slippage_csv_path() -> String { "slippage_events.csv".to_string() }
fn default_mexc_taker_fee_bps() -> Decimal { Decimal::from(6) }
fn default_trade_notional_usdt() -> Decimal { Decimal::from(300) }
fn default_telegram_subscribers_path() -> String { "telegram_subscribers.json".to_string() }
fn default_alert_diff_bps() -> Decimal { Decimal::new(5, 1) }
fn default_alert_min_edge_bps() -> Decimal { Decimal::from(1) }
fn default_alert_min_duration_ms() -> i64 { 500 }
fn default_max_quote_age_ms() -> i64 { 2_000 }
fn default_trade_vol() -> String { "1".to_string() }
fn default_trade_leverage() -> i32 { 10 }
fn default_trade_timeout_ms() -> u64 { 5000 }

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
            if let Some(v) = local.get("mexc_api_key").and_then(|x| x.as_str()) {
                config.mexc_api_key = v.to_string();
            }
            if let Some(v) = local.get("mexc_api_secret").and_then(|x| x.as_str()) {
                config.mexc_api_secret = v.to_string();
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

// ── Auth helpers ──────────────────────────────────────────────────────────────

fn load_users() -> Vec<User> {
    match std::fs::read_to_string("users.json") {
        Ok(text) => serde_json::from_str(&text).unwrap_or_else(|e| {
            eprintln!("[auth] failed to parse users.json: {e}");
            Vec::new()
        }),
        Err(_) => {
            eprintln!("[auth] users.json not found — run with --add-user to create accounts");
            Vec::new()
        }
    }
}

fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| anyhow::anyhow!("hash error: {e}"))
}

fn verify_password(password: &str, hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(hash) else { return false; };
    Argon2::default().verify_password(password.as_bytes(), &parsed).is_ok()
}

fn add_user_interactive() -> Result<()> {
    const USERS_PATH: &str = "users.json";
    let mut users: Vec<User> = if std::path::Path::new(USERS_PATH).exists() {
        let text = std::fs::read_to_string(USERS_PATH)?;
        serde_json::from_str(&text).context("parse users.json")?
    } else {
        Vec::new()
    };

    print!("Username: ");
    std::io::stdout().flush()?;
    let mut username = String::new();
    std::io::stdin().read_line(&mut username)?;
    let username = username.trim().to_string();
    anyhow::ensure!(!username.is_empty(), "username cannot be empty");

    let password = rpassword::prompt_password("Password: ")?;
    anyhow::ensure!(!password.is_empty(), "password cannot be empty");

    let hash = hash_password(&password)?;
    users.retain(|u| u.username != username);
    users.push(User { username: username.clone(), hash });
    std::fs::write(USERS_PATH, serde_json::to_string_pretty(&users)?)?;
    println!("User '{}' saved.", username);
    Ok(())
}

// ── Auth middleware & handlers ────────────────────────────────────────────────

async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if request.method() == axum::http::Method::OPTIONS {
        return next.run(request).await;
    }
    let token = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);

    if let Some(token) = token {
        let sessions = state.sessions.read().await;
        if let Some(session) = sessions.get(&token) {
            if now_ms() < session.expires_ms {
                return next.run(request).await;
            }
        }
    }
    (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    token: String,
}

async fn login_handler(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> Response {
    let ip = addr.ip().to_string();
    {
        let attempts = state.login_attempts.lock().await;
        if attempts.is_blocked(&ip) {
            return (StatusCode::TOO_MANY_REQUESTS, "Too many failed attempts, wait 15 minutes").into_response();
        }
    }

    let user = state.users.iter().find(|u| u.username == req.username).cloned();
    let valid = match user {
        Some(u) => {
            let pw = req.password.clone();
            let hash = u.hash.clone();
            tokio::task::spawn_blocking(move || verify_password(&pw, &hash))
                .await
                .unwrap_or(false)
        }
        None => {
            // Always burn argon2 time to prevent username enumeration via timing
            let pw = req.password.clone();
            let _ = tokio::task::spawn_blocking(move || {
                let _ = Argon2::default().verify_password(
                    pw.as_bytes(),
                    &PasswordHash::new("$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$RdescudvJCsgt3ub+b+dWRWJTmaaJObE").unwrap_or_else(|_| panic!()),
                );
            }).await;
            false
        }
    };

    if valid {
        state.login_attempts.lock().await.record_success(&ip);
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(|c| c as char)
            .collect();
        let expires_ms = now_ms() + 7 * 24 * 60 * 60 * 1_000;
        state.sessions.write().await.insert(token.clone(), Session {
            username: req.username,
            expires_ms,
        });
        (StatusCode::OK, Json(LoginResponse { token })).into_response()
    } else {
        state.login_attempts.lock().await.record_failure(&ip);
        (StatusCode::UNAUTHORIZED, "Invalid credentials").into_response()
    }
}

async fn logout_handler(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> StatusCode {
    if let Some(token) = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        state.sessions.write().await.remove(token);
    }
    StatusCode::OK
}

// ── API key management ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct KeysStatus {
    active: bool,
}

#[derive(Deserialize)]
struct KeysRequest {
    mexc_api_key: String,
    mexc_api_secret: String,
}

async fn keys_get(State(state): State<AppState>) -> Json<KeysStatus> {
    let active = state.trade_client.read().await.is_some();
    Json(KeysStatus { active })
}

async fn keys_post(
    State(state): State<AppState>,
    Json(req): Json<KeysRequest>,
) -> StatusCode {
    if let Err(e) = save_mexc_keys(&req.mexc_api_key, &req.mexc_api_secret) {
        eprintln!("[keys] failed to save to telegram_config.json: {e:#}");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    let mut client = state.trade_client.write().await;
    if req.mexc_api_key.is_empty() || req.mexc_api_secret.is_empty() {
        *client = None;
        eprintln!("[trade] API keys cleared");
    } else {
        *client = Some(Arc::new(MexcTradeClient::new(req.mexc_api_key, req.mexc_api_secret, 1)));
        eprintln!("[trade] API keys updated, client active");
    }
    StatusCode::OK
}

fn save_mexc_keys(api_key: &str, api_secret: &str) -> Result<()> {
    const PATH: &str = "telegram_config.json";
    let mut obj: serde_json::Map<String, serde_json::Value> =
        std::fs::read_to_string(PATH)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();
    obj.insert("mexc_api_key".to_string(), serde_json::Value::String(api_key.to_string()));
    obj.insert("mexc_api_secret".to_string(), serde_json::Value::String(api_secret.to_string()));
    std::fs::write(PATH, serde_json::to_string_pretty(&obj)?)?;
    Ok(())
}

// ── CSV helpers ───────────────────────────────────────────────────────────────

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

// ── Quote handling ────────────────────────────────────────────────────────────

async fn handle_quote(
    update: QuoteUpdate,
    config: &Config,
    states: &mut HashMap<String, SymbolState>,
    stats: &mut Stats,
    csv: &mut File,
    slippage_csv: &mut File,
    trade_client: Option<Arc<MexcTradeClient>>,
    live_cfg: &SharedLiveConfig,
) -> Result<()> {
    let live = live_cfg.read().await.clone();
    let now = update.recv_ts_ms;
    let Some(state) = states.get_mut(&update.symbol) else {
        return Ok(());
    };

    match update.exchange {
        Exchange::Binance => {
            stats.record_quote(&update.symbol, Exchange::Binance);
            let prev = state.last_binance.clone();
            state.last_binance = Some(update.clone());

            'detect: {
                let Some(prev) = prev else { break 'detect; };
                let Some(mexc) = state.last_mexc.clone() else { break 'detect; };
                let elapsed = update.recv_ts_ms - prev.recv_ts_ms;
                if elapsed < 0 || elapsed > config.impulse_window_ms {
                    break 'detect;
                }
                if now - state.last_event_ts_ms < config.event_cooldown_ms {
                    break 'detect;
                }

                let prev_mid = prev.mid();
                let new_mid = update.mid();
                if prev_mid <= Decimal::ZERO || new_mid <= Decimal::ZERO {
                    break 'detect;
                }
                let move_bps = (new_mid - prev_mid) / prev_mid * Decimal::from(10_000);
                let abs_move_bps = move_bps.abs();
                if abs_move_bps < live.impulse_bps {
                    break 'detect;
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
        }
        Exchange::Mexc => {
            stats.record_quote(&update.symbol, Exchange::Mexc);
            state.last_mexc = Some(update.clone());

            'detect: {
                let Some(pending) = state.pending.clone() else { break 'detect; };

                if now - pending.created_recv_ts_ms > live.max_lag_ms {
                    state.pending = None;
                    stats.record_expired(&pending.symbol);
                    break 'detect;
                }

                let start = pending.mexc_start_mid;
                let current = update.mid();
                if start <= Decimal::ZERO || current <= Decimal::ZERO {
                    break 'detect;
                }
                let mexc_move_bps = (current - start) / start * Decimal::from(10_000);
                let confirmed = if pending.direction > 0 {
                    mexc_move_bps >= live.confirm_bps
                } else {
                    mexc_move_bps <= -live.confirm_bps
                };
                if !confirmed {
                    break 'detect;
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
                let estimate = write_slippage_record(slippage_csv, config, &live, &record, &pending, &update)?;
                stats.set_trade_estimate(&record.symbol, estimate);
                stats.add_lag(&record.symbol, record.lag_ms);
                state.pending = None;
            }
        }
    }

    check_price_diff_alert(&update.symbol, config, &live, state, trade_client).await;

    Ok(())
}

async fn check_price_diff_alert(symbol: &str, config: &Config, live: &LiveConfig, state: &mut SymbolState, trade_client: Option<Arc<MexcTradeClient>>) {
    let (Some(binance), Some(mexc)) = (state.last_binance.as_ref(), state.last_mexc.as_ref()) else {
        return;
    };
    if live.alert_diff_bps <= Decimal::ZERO {
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
    let edge = estimate_diff_edge(binance, mexc, diff_bps, live.trade_margin_usdt * Decimal::from(live.trade_leverage));
    let now = now_ms();
    let binance_age_ms = now - binance.recv_ts_ms;
    let mexc_age_ms = now - mexc.recv_ts_ms;
    if binance_age_ms < 0
        || mexc_age_ms < 0
        || binance_age_ms > config.max_quote_age_ms
        || mexc_age_ms > config.max_quote_age_ms
    {
        if let Some(tx) = state.active_trade_close.take() {
            let _ = tx.send(());
        }
        if state.active_diff_alert.take().is_some() {
            state.last_diff_alert_closed_ms = now;
            eprintln!(
                "[alert] stale quote close {symbol}: binance_age={binance_age_ms}ms mexc_age={mexc_age_ms}ms max={}ms",
                config.max_quote_age_ms
            );
        }
        return;
    }

    if abs_diff_bps >= live.alert_diff_bps {
        match state.active_diff_alert.as_mut() {
            Some(active) => {
                if abs_diff_bps > active.max_abs_diff_bps {
                    active.max_abs_diff_bps = abs_diff_bps;
                }
                if edge.edge_zero_fee_bps > active.max_edge_zero_fee_bps {
                    active.max_edge_zero_fee_bps = edge.edge_zero_fee_bps;
                }

                let duration_ms = now - active.started_ms;
                if !active.notified
                    && duration_ms >= config.alert_min_duration_ms
                    && edge.edge_zero_fee_bps >= config.alert_min_edge_bps
                    && active.max_edge_zero_fee_bps >= config.alert_min_edge_bps
                {
                    active.notified = true;
                    let diff_pct = diff_bps / Decimal::from(100);
                    let thr_pct = live.alert_diff_bps / Decimal::from(100);
                    let text = format!(
                        "💎 Ценная разница {symbol}\n{direction}\nДержится: {secs:.2} сек\nBinance mid: {b}\nMEXC mid: {m}\nMEXC bid/ask: {bid} / {ask}\nРазница сейчас: {du} USDT / {dp}% ({dbps} bps)\nПорог: {thr}%\nСпред MEXC: {spread} bps\n0 fee edge старт/макс/сейчас: {start_edge} / {max_edge} / {edge_bps} bps\nPnL 0 fee сейчас: {pnl} USDT",
                        symbol = symbol,
                        direction = active.direction,
                        secs = duration_ms as f64 / 1000.0,
                        b = b_mid.round_dp(6),
                        m = m_mid.round_dp(6),
                        bid = mexc.bid.round_dp(8),
                        ask = mexc.ask.round_dp(8),
                        du = diff_usdt.round_dp(6),
                        dp = diff_pct.round_dp(4),
                        dbps = diff_bps.round_dp(3),
                        thr = thr_pct.round_dp(4),
                        spread = edge.mexc_spread_bps.round_dp(3),
                        start_edge = active.start_edge_zero_fee_bps.round_dp(3),
                        max_edge = active.max_edge_zero_fee_bps.round_dp(3),
                        edge_bps = edge.edge_zero_fee_bps.round_dp(3),
                        pnl = edge.pnl_zero_fee_usdt.round_dp(4),
                    );
                    send_telegram_to_subscribers(config, &text);

                    if live.trade_enabled && state.active_trade_close.is_none() {
                        if let Some(client) = trade_client.clone() {
                            let trade_direction = if diff_usdt < Decimal::ZERO { 1 } else { -1 };
                            let (close_tx, close_rx) = tokio::sync::oneshot::channel();
                            state.active_trade_close = Some(close_tx);
                            tokio::spawn(client.run_trade(
                                symbol.to_string(),
                                trade_direction,
                                m_mid,
                                b_mid,
                                config.trade_vol.clone(),
                                live.trade_leverage,
                                config.trade_timeout_ms,
                                close_rx,
                            ));
                        }
                    }
                }
            }
            None => {
                if now - state.last_diff_alert_closed_ms < 60_000 {
                    return;
                }
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
                    start_edge_zero_fee_bps: edge.edge_zero_fee_bps,
                    max_edge_zero_fee_bps: edge.edge_zero_fee_bps,
                    notified: false,
                });
            }
        }
    } else if let Some(active) = state.active_diff_alert.take() {
        state.last_diff_alert_closed_ms = now;
        if let Some(tx) = state.active_trade_close.take() {
            let _ = tx.send(());
        }
        if !active.notified {
            return;
        }
        let duration_ms = now - active.started_ms;
        let start_pct = active.start_diff_bps / Decimal::from(100);
        let max_pct = active.max_abs_diff_bps / Decimal::from(100);
        let final_pct = diff_bps / Decimal::from(100);
        let text = format!(
            "Разница закрылась {symbol}\nНаправление: {direction}\nДержалась: {secs:.2} сек\nСтарт: {start_usdt} USDT / {start_pct}% ({start_bps} bps)\nМакс: {max_pct}% ({max_bps} bps)\nФинал: {final_usdt} USDT / {final_pct}% ({final_bps} bps)\nСпред MEXC сейчас: {spread} bps\nОценка 0 fee старт/макс/финал: {start_edge} / {max_edge} / {final_edge} bps\nPnL 0 fee финал: {final_pnl} USDT",
            symbol = symbol,
            direction = active.direction,
            secs = duration_ms as f64 / 1000.0,
            start_usdt = active.start_diff_usdt.round_dp(6),
            start_pct = start_pct.round_dp(4),
            start_bps = active.start_diff_bps.round_dp(3),
            max_pct = max_pct.round_dp(4),
            max_bps = active.max_abs_diff_bps.round_dp(3),
            final_usdt = diff_usdt.round_dp(6),
            final_pct = final_pct.round_dp(4),
            final_bps = diff_bps.round_dp(3),
            spread = edge.mexc_spread_bps.round_dp(3),
            start_edge = active.start_edge_zero_fee_bps.round_dp(3),
            max_edge = active.max_edge_zero_fee_bps.round_dp(3),
            final_edge = edge.edge_zero_fee_bps.round_dp(3),
            final_pnl = edge.pnl_zero_fee_usdt.round_dp(4),
        );
        send_telegram_to_subscribers(config, &text);
    }
}

fn estimate_diff_edge(binance: &QuoteUpdate, mexc: &QuoteUpdate, diff_bps: Decimal, notional_usdt: Decimal) -> DiffEdgeEstimate {
    let b_mid = binance.mid();
    let mexc_spread_bps = spread_bps(mexc.bid, mexc.ask);
    let edge_zero_fee_bps = if b_mid <= Decimal::ZERO {
        Decimal::ZERO
    } else if diff_bps < Decimal::ZERO {
        (b_mid - mexc.ask) / b_mid * Decimal::from(10_000)
    } else {
        (mexc.bid - b_mid) / b_mid * Decimal::from(10_000)
    };
    DiffEdgeEstimate {
        mexc_spread_bps,
        edge_zero_fee_bps,
        pnl_zero_fee_usdt: notional_usdt * edge_zero_fee_bps / Decimal::from(10_000),
    }
}

fn send_telegram_to_subscribers(config: &Config, text: &str) {
    if config.telegram_bot_token.is_empty() {
        return;
    }
    let subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
    if subscribers.is_empty() {
        return;
    }
    let token = config.telegram_bot_token.clone();
    let text = text.to_string();
    eprintln!("[telegram] sending alert to {} subscriber(s)", subscribers.len());
    tokio::spawn(async move {
        for chat_id in subscribers {
            if let Err(e) = telegram_send_raw(&token, &chat_id, &text).await {
                eprintln!("[telegram] send to {} failed: {:#}", chat_id, e);
            }
        }
    });
}

fn expire_old_pending(_config: &Config, live: &LiveConfig, states: &mut HashMap<String, SymbolState>, stats: &mut Stats) {
    let now = now_ms();
    for (symbol, state) in states.iter_mut() {
        if let Some(pending) = &state.pending {
            if now - pending.created_recv_ts_ms > live.max_lag_ms {
                let expired_symbol = symbol.clone();
                state.pending = None;
                stats.record_expired(&expired_symbol);
            }
        }
    }
}

fn print_stats(config: &Config, live: &LiveConfig, states: &HashMap<String, SymbolState>, stats: &Stats) {
    let mut stdout = io::stdout();
    let _ = execute!(stdout, Clear(ClearType::All), MoveTo(0, 0));
    println!("=== MEXC LAG MONITOR {} | symbols={} ===", Utc::now().format("%Y-%m-%d %H:%M:%S UTC"), config.symbols.len());
    println!("{}", stats.summary());
    println!(
        "thresholds: impulse>={}bps confirm>={}bps max_lag={}ms window={}ms",
        live.impulse_bps, live.confirm_bps, live.max_lag_ms, config.impulse_window_ms
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
    live: &LiveConfig,
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
    let position_usdt = live.trade_margin_usdt * Decimal::from(live.trade_leverage);
    let estimated_pnl_usdt = position_usdt * net_cross_bps / Decimal::from(10_000);
    let pnl_zero_fee_usdt = position_usdt * gross_cross_bps / Decimal::from(10_000);

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

// ── WebSocket feeds ───────────────────────────────────────────────────────────

async fn run_binance(config: Config, tx: mpsc::Sender<QuoteUpdate>) {
    let streams: Vec<String> = config.symbols.iter()
        .map(|s| format!("{}@bookTicker", binance_stream_symbol(s)))
        .collect();
    let url = format!("{}?streams={}", config.binance_ws, streams.join("/"));
    let symbols: HashSet<String> = config.symbols.iter().cloned().collect();

    loop {
        eprintln!("[binance] connecting combined stream ({} symbols)", symbols.len());
        if let Err(e) = binance_session(&url, &symbols, &tx).await {
            eprintln!("[binance] session error: {e:#}");
        }
        sleep(Duration::from_secs(2)).await;
    }
}

fn binance_stream_symbol(symbol: &str) -> String {
    match symbol {
        "PEPE_USDT" => "1000pepeusdt".to_string(),
        _ => symbol.replace('_', "").to_ascii_lowercase(),
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
                if symbols.contains(&update.symbol) {
                    if tx.send(update).await.is_err() {
                        return Ok(());
                    }
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

// ── Parsers ───────────────────────────────────────────────────────────────────

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
    let mut bid = Decimal::from_str(&data.bid).ok()?;
    let mut ask = Decimal::from_str(&data.ask).ok()?;
    if data.symbol == "1000PEPEUSDT" {
        bid /= Decimal::from(1000);
        ask /= Decimal::from(1000);
    }
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
            if is_bid { best.max(price) } else { best.min(price) }
        })
}

fn normalize_binance_symbol(symbol: &str) -> String {
    if symbol == "1000PEPEUSDT" {
        return "PEPE_USDT".to_string();
    }
    symbol
        .strip_suffix("USDT")
        .map(|base| format!("{}_USDT", base))
        .unwrap_or_else(|| symbol.to_string())
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

// ── Dashboard snapshot ────────────────────────────────────────────────────────

fn build_dashboard_snapshot(
    config: &Config,
    live: &LiveConfig,
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
        impulse_bps: live.impulse_bps.to_string(),
        confirm_bps: live.confirm_bps.to_string(),
        max_lag_ms: live.max_lag_ms,
        symbols,
    }
}

// ── HTTP server ───────────────────────────────────────────────────────────────

async fn run_dashboard(state: AppState) {
    let protected = Router::new()
        .route("/api/state", get(dashboard_state))
        .route("/api/config", get(config_get).post(config_post))
        .route("/api/keys", get(keys_get).post(keys_post))
        .route("/api/trade-stats", get(trade_stats_handler))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware));

    let app = Router::new()
        .route("/", get(dashboard_html))
        .route("/api/login", post(login_handler))
        .route("/api/logout", post(logout_handler))
        .merge(protected)
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8787));
    let Ok(listener) = tokio::net::TcpListener::bind(addr).await else {
        eprintln!("[dashboard] failed to bind {addr}");
        return;
    };
    eprintln!("[dashboard] listening on {addr}");
    let _ = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    ).await;
}

async fn dashboard_state(State(state): State<AppState>) -> Json<DashboardSnapshot> {
    Json(state.dashboard.read().await.clone())
}

async fn config_get(State(state): State<AppState>) -> Json<LiveConfig> {
    Json(state.live_cfg.read().await.clone())
}

async fn config_post(State(state): State<AppState>, Json(new_cfg): Json<LiveConfig>) -> impl IntoResponse {
    *state.live_cfg.write().await = new_cfg.clone();
    // Persist live fields back into config.json so they survive restart.
    if let Ok(text) = std::fs::read_to_string(state.config_path.as_str()) {
        if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(&text) {
            json["impulse_bps"] = new_cfg.impulse_bps.to_string().into();
            json["confirm_bps"] = new_cfg.confirm_bps.to_string().into();
            json["max_lag_ms"] = new_cfg.max_lag_ms.into();
            json["alert_diff_bps"] = new_cfg.alert_diff_bps.to_string().into();
            json["trade_notional_usdt"] = new_cfg.trade_margin_usdt.to_string().into();
            json["trade_leverage"] = new_cfg.trade_leverage.into();
            json["trade_enabled"] = new_cfg.trade_enabled.into();
            if let Ok(out) = serde_json::to_string_pretty(&json) {
                let _ = std::fs::write(state.config_path.as_str(), out);
            }
        }
    }
    Json(new_cfg)
}

// BTC/ETH/SOL have non-zero maker fees on MEXC futures; excluded from PnL simulation.
const MAJOR_SYMBOLS: &[&str] = &["BTC_USDT", "ETH_USDT", "SOL_USDT"];

#[derive(Serialize)]
struct TradeStats {
    total_detected: usize,
    trade_count: usize,
    avg_gross_bps: Option<f64>,
    avg_net_bps: Option<f64>,
    cumulative_pnl_usdt: f64,
    fee_bps: f64,
    min_lag_ms: i64,
}

#[derive(Deserialize, Default)]
struct TradeStatsQuery {
    min_lag_ms: Option<i64>,
}

async fn trade_stats_handler(
    State(state): State<AppState>,
    Query(query): Query<TradeStatsQuery>,
) -> impl IntoResponse {
    // CSV: utc,symbol,direction,lag_ms,entry_bid,entry_ask,exit_bid,exit_ask,
    //      entry_spread_bps,exit_spread_bps,gross_cross_bps,...
    //
    // Two filters for realism:
    // 1. lag_ms >= min_lag_ms: events where MEXC lagged long enough for your order to execute.
    //    If MEXC moves in 150ms but your order takes 150ms, you arrive after MEXC already moved.
    // 2. gross > fee: only count events that are actually profitable after 6 bps taker fee.
    const ALTCOIN_FEE_BPS: f64 = 6.0;
    let min_lag_ms = query.min_lag_ms.unwrap_or(200);
    let path = state.slippage_csv_path.as_str();
    let live = state.live_cfg.read().await;
    let margin = live.trade_margin_usdt.to_string().parse::<f64>().unwrap_or(300.0);
    let leverage = live.trade_leverage as f64;
    let notional = margin * leverage;
    drop(live);

    struct Row { gross: f64 }
    let (total, rows): (usize, Vec<Row>) = {
        let mut total = 0usize;
        let rows = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .skip(1)
            .filter_map(|line| {
                let cols: Vec<&str> = line.split(',').collect();
                if MAJOR_SYMBOLS.contains(cols.get(1)?) { return None; }
                let lag: i64 = cols.get(3)?.parse().ok()?;
                let gross: f64 = cols.get(10)?.parse().ok()?;
                total += 1;
                if lag >= min_lag_ms && gross > ALTCOIN_FEE_BPS { Some(Row { gross }) } else { None }
            })
            .collect();
        // total counts all altcoin rows regardless of filters
        let all_total = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .skip(1)
            .filter(|line| {
                let cols: Vec<&str> = line.split(',').collect();
                cols.get(1).map(|s| !MAJOR_SYMBOLS.contains(s)).unwrap_or(false)
            })
            .count();
        (all_total, rows)
    };

    let count = rows.len();
    if count == 0 {
        return Json(TradeStats { total_detected: total, trade_count: 0, avg_gross_bps: None, avg_net_bps: None, cumulative_pnl_usdt: 0.0, fee_bps: ALTCOIN_FEE_BPS, min_lag_ms });
    }

    let avg_gross = rows.iter().map(|r| r.gross).sum::<f64>() / count as f64;
    let avg_net = avg_gross - ALTCOIN_FEE_BPS;
    let cum_pnl: f64 = rows.iter().map(|r| notional * (r.gross - ALTCOIN_FEE_BPS) / 10_000.0).sum();

    Json(TradeStats {
        total_detected: total,
        trade_count: count,
        avg_gross_bps: Some((avg_gross * 1000.0).round() / 1000.0),
        avg_net_bps: Some((avg_net * 1000.0).round() / 1000.0),
        cumulative_pnl_usdt: (cum_pnl * 100.0).round() / 100.0,
        fee_bps: ALTCOIN_FEE_BPS,
        min_lag_ms,
    })
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

// ── Dashboard HTML ────────────────────────────────────────────────────────────

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Монитор задержки MEXC</title>
  <style>
    :root { color-scheme: dark; font-family: Segoe UI, Arial, sans-serif; background:#0f1115; color:#e7eaf0; }
    body { margin:0; background:#0f1115; }
    header { position:sticky; top:0; background:#151923; border-bottom:1px solid #2a3040; padding:14px 18px; z-index:2; display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
    .header-left h1 { margin:0 0 8px; font-size:20px; font-weight:650; }
    .meta { display:flex; flex-wrap:wrap; gap:10px 18px; color:#aab3c5; font-size:13px; }
    .settings { display:flex; flex-wrap:wrap; align-items:center; gap:10px 16px; padding:10px 18px; background:#111622; border-bottom:1px solid #2a3040; font-size:13px; }
    .settings label { display:flex; align-items:center; gap:6px; color:#aab3c5; }
    .settings input[type=number] { width:80px; background:#1a2030; border:1px solid #3a4558; border-radius:4px; color:#e7eaf0; padding:3px 6px; font-size:13px; }
    .settings input[type=checkbox] { accent-color:#4a90d9; width:15px; height:15px; }
    .settings button { background:#2a5298; border:none; border-radius:4px; color:#fff; padding:5px 14px; cursor:pointer; font-size:13px; }
    .settings button:hover { background:#3a6ab8; }
    .cfg-status { color:#6dbf6d; font-size:12px; min-width:50px; }
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
    .muted { color:#7d8799; }
    .btn-logout { background:transparent; border:1px solid #3a4558; border-radius:4px; color:#aab3c5; padding:5px 12px; cursor:pointer; font-size:12px; white-space:nowrap; }
    .btn-logout:hover { background:#1e2535; color:#e7eaf0; }
    /* Login overlay */
    .overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.85); z-index:200; align-items:center; justify-content:center; }
    .overlay.active { display:flex; }
    .login-card { background:#151923; border:1px solid #2a3040; border-radius:8px; padding:36px 40px; min-width:300px; max-width:380px; width:100%; }
    .login-card h2 { margin:0 0 24px; font-size:18px; font-weight:600; }
    .field { display:flex; flex-direction:column; gap:6px; margin-bottom:16px; }
    .field label { color:#aab3c5; font-size:13px; }
    .field input { background:#1a2030; border:1px solid #3a4558; border-radius:4px; color:#e7eaf0; padding:9px 11px; font-size:14px; outline:none; }
    .field input:focus { border-color:#4a90d9; }
    .btn-primary { width:100%; background:#2a5298; border:none; border-radius:4px; color:#fff; padding:10px; cursor:pointer; font-size:14px; margin-top:4px; }
    .btn-primary:hover { background:#3a6ab8; }
    .login-err { color:#e06060; font-size:13px; margin-top:10px; min-height:18px; }
  </style>
</head>
<body>

<div class="overlay" id="loginOverlay">
  <div class="login-card">
    <h2>Вход</h2>
    <div class="field">
      <label>Логин</label>
      <input type="text" id="loginUser" autocomplete="username" />
    </div>
    <div class="field">
      <label>Пароль</label>
      <input type="password" id="loginPass" autocomplete="current-password" />
    </div>
    <button class="btn-primary" onclick="doLogin()">Войти</button>
    <div class="login-err" id="loginErr"></div>
  </div>
</div>

<header>
  <div class="header-left">
    <h1>Монитор задержки Binance -> MEXC</h1>
    <div class="meta">
      <span id="updated">ожидание данных</span>
      <span id="thresholds"></span>
      <span>Файлы: lag_events.csv / slippage_events.csv / stats_snapshots.csv</span>
    </div>
  </div>
  <button class="btn-logout" onclick="doLogout()">Выйти</button>
</header>
<div class="settings">
  <label>Margin (USDT): <input id="cfgMargin" type="number" min="1" step="10"></label>
  <label>Leverage: <input id="cfgLeverage" type="number" min="1" max="200" step="1"></label>
  <label>Impulse (bps): <input id="cfgImpulse" type="number" min="0.1" step="0.1"></label>
  <label>Confirm (bps): <input id="cfgConfirm" type="number" min="0.1" step="0.1"></label>
  <label>Max lag (ms): <input id="cfgMaxLag" type="number" min="100" step="100"></label>
  <label>Alert diff (bps): <input id="cfgAlert" type="number" min="0.1" step="0.1"></label>
  <label>Trading: <input id="cfgTrade" type="checkbox"></label>
  <button onclick="saveConfig()">Apply</button>
  <span class="cfg-status" id="cfgStatus"></span>
  <span style="color:#3a4558;margin:0 4px">|</span>
  <label>API Key: <input id="cfgApiKey" type="password" placeholder="MEXC API key" style="width:160px"></label>
  <label>Secret: <input id="cfgApiSecret" type="password" placeholder="MEXC secret" style="width:160px"></label>
  <button onclick="saveKeys()">Set Keys</button>
  <span class="cfg-status" id="keysStatus"></span>
</div>
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
  <section class="summary" style="margin-top:16px" id="tradeStatsSection" hidden>
    <div class="box">Событий (altcoin)<b id="stTotal">-</b></div>
    <div class="box">Tradeable (lag≥<input id="minLagInput" type="number" value="200" min="0" step="50" style="width:52px;background:#1a2030;color:#c8d0e0;border:1px solid #2a3550;border-radius:3px;padding:1px 3px" onchange="loadTradeStats()">ms, gross&gt;6bps)<b id="stTrades">-</b></div>
    <div class="box">Avg net bps<b id="stNet">-</b></div>
    <div class="box">Cumulative PnL (sim)<b id="stCumPnl">-</b></div>
  </section>
</main>
<script>
const TOKEN_KEY = 'dashboard_token';
function tok() { return sessionStorage.getItem(TOKEN_KEY); }
function authH() { const t = tok(); return t ? {'Authorization': 'Bearer ' + t} : {}; }
function showLogin() {
  const overlay = document.getElementById('loginOverlay');
  const alreadyVisible = overlay.classList.contains('active');
  overlay.classList.add('active');
  if (!alreadyVisible) setTimeout(() => document.getElementById('loginUser').focus(), 50);
}
function hideLogin() { document.getElementById('loginOverlay').classList.remove('active'); }
function handle401() { sessionStorage.removeItem(TOKEN_KEY); showLogin(); }

async function doLogin() {
  const u = document.getElementById('loginUser').value.trim();
  const p = document.getElementById('loginPass').value;
  const err = document.getElementById('loginErr');
  err.textContent = '';
  if (!u || !p) { err.textContent = 'Заполните все поля'; return; }
  try {
    const r = await fetch('/api/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({username: u, password: p})
    });
    if (r.ok) {
      const d = await r.json();
      sessionStorage.setItem(TOKEN_KEY, d.token);
      document.getElementById('loginPass').value = '';
      hideLogin();
      loadConfig();
    } else if (r.status === 429) {
      err.textContent = 'Слишком много попыток. Подождите 15 минут.';
    } else {
      err.textContent = 'Неверный логин или пароль';
    }
  } catch { err.textContent = 'Ошибка соединения'; }
}

function doLogout() {
  const t = tok();
  if (t) fetch('/api/logout', {method: 'POST', headers: authH()}).catch(() => {});
  sessionStorage.removeItem(TOKEN_KEY);
  showLogin();
}

document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.getElementById('loginOverlay').classList.contains('active')) doLogin();
});

function fmt(v, suffix='') { return v === null || v === undefined ? '-' : `${v}${suffix}`; }
function sum(rows, key) { return rows.reduce((a, r) => a + (r[key] || 0), 0); }

async function loadConfig() {
  const r = await fetch('/api/config', {headers: authH()}).catch(() => null);
  if (!r || r.status === 401) { handle401(); return; }
  const c = await r.json();
  document.getElementById('cfgMargin').value = c.trade_margin_usdt;
  document.getElementById('cfgLeverage').value = c.trade_leverage;
  document.getElementById('cfgImpulse').value = c.impulse_bps;
  document.getElementById('cfgConfirm').value = c.confirm_bps;
  document.getElementById('cfgMaxLag').value = c.max_lag_ms;
  document.getElementById('cfgAlert').value = c.alert_diff_bps;
  document.getElementById('cfgTrade').checked = c.trade_enabled;
  loadKeys();
}

async function loadKeys() {
  const r = await fetch('/api/keys', {headers: authH()}).catch(() => null);
  if (!r || r.status === 401) return;
  const d = await r.json();
  const st = document.getElementById('keysStatus');
  st.textContent = d.active ? 'Active ✓' : 'Not set';
  st.style.color = d.active ? '#6dbf6d' : '#e0a060';
}

async function saveKeys() {
  const key = document.getElementById('cfgApiKey').value.trim();
  const secret = document.getElementById('cfgApiSecret').value.trim();
  const st = document.getElementById('keysStatus');
  st.textContent = '';
  if (!key || !secret) { st.textContent = 'Enter both fields'; st.style.color = '#e06060'; return; }
  const r = await fetch('/api/keys', {
    method: 'POST',
    headers: Object.assign({'Content-Type': 'application/json'}, authH()),
    body: JSON.stringify({mexc_api_key: key, mexc_api_secret: secret})
  }).catch(() => null);
  if (!r || r.status === 401) { handle401(); return; }
  if (r.ok) {
    document.getElementById('cfgApiKey').value = '';
    document.getElementById('cfgApiSecret').value = '';
    st.textContent = 'Active ✓';
    st.style.color = '#6dbf6d';
  } else {
    st.textContent = 'Error ✗';
    st.style.color = '#e06060';
  }
}

async function saveConfig() {
  const body = {
    trade_margin_usdt: parseFloat(document.getElementById('cfgMargin').value),
    trade_leverage: parseInt(document.getElementById('cfgLeverage').value),
    impulse_bps: parseFloat(document.getElementById('cfgImpulse').value),
    confirm_bps: parseFloat(document.getElementById('cfgConfirm').value),
    max_lag_ms: parseInt(document.getElementById('cfgMaxLag').value),
    alert_diff_bps: parseFloat(document.getElementById('cfgAlert').value),
    trade_enabled: document.getElementById('cfgTrade').checked,
  };
  const r = await fetch('/api/config', {
    method: 'POST',
    headers: Object.assign({'Content-Type': 'application/json'}, authH()),
    body: JSON.stringify(body)
  }).catch(() => null);
  if (!r || r.status === 401) { handle401(); return; }
  const st = document.getElementById('cfgStatus');
  st.textContent = r.ok ? 'Saved ✓' : 'Error ✗';
  st.style.color = r.ok ? '#6dbf6d' : '#e06060';
  setTimeout(() => { st.textContent = ''; }, 2500);
}

async function refresh() {
  const r = await fetch('/api/state', {cache: 'no-store', headers: authH()}).catch(() => null);
  if (!r) return;
  if (r.status === 401) { handle401(); return; }
  const data = await r.json();
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

async function loadTradeStats() {
  const minLag = document.getElementById('minLagInput')?.value || 200;
  const r = await fetch(`/api/trade-stats?min_lag_ms=${minLag}`, {headers: authH()}).catch(() => null);
  if (!r || r.status === 401) return;
  const d = await r.json();
  const sec = document.getElementById('tradeStatsSection');
  if (d.total_detected === 0) { sec.hidden = true; return; }
  sec.hidden = false;
  document.getElementById('stTotal').textContent = d.total_detected;
  const pct = d.total_detected > 0 ? Math.round(d.trade_count / d.total_detected * 100) : 0;
  document.getElementById('stTrades').textContent = `${d.trade_count} (${pct}%)`;
  const net = d.avg_net_bps;
  const netEl = document.getElementById('stNet');
  netEl.textContent = net !== null ? net.toFixed(3) + ' bps' : 'нет данных';
  netEl.style.color = net !== null ? (net > 0 ? '#6dbf6d' : '#e06060') : '#888';
  const cum = d.cumulative_pnl_usdt;
  const cumEl = document.getElementById('stCumPnl');
  cumEl.textContent = d.trade_count > 0 ? `$${cum.toFixed(2)}` : '-';
  cumEl.style.color = cum > 0 ? '#6dbf6d' : '#e06060';
}

if (tok()) { loadConfig(); loadTradeStats(); } else { showLogin(); }
setInterval(refresh, 1000);
setInterval(loadTradeStats, 30000);
refresh();
</script>
</body>
</html>"#;

// ── Telegram ──────────────────────────────────────────────────────────────────

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

async fn telegram_send_raw(token: &str, chat_id: &str, text: &str) -> Result<()> {
    if token.is_empty() || chat_id.is_empty() {
        return Ok(());
    }
    let blocked_until = TELEGRAM_BLOCKED_UNTIL_MS.load(Ordering::Relaxed);
    if now_ms() < blocked_until {
        anyhow::bail!("rate limited for {}ms", blocked_until - now_ms());
    }
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
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
        if status.as_u16() == 429 {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(retry_after) = v.pointer("/parameters/retry_after").and_then(|x| x.as_i64()) {
                    TELEGRAM_BLOCKED_UNTIL_MS.store(now_ms() + retry_after * 1000, Ordering::Relaxed);
                }
            }
        }
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
                            let _ = telegram_send_raw(&config.telegram_bot_token, &chat_id, "Вы вышли из рассылки отчетов.").await;
                            continue;
                        }

                        if text.trim() == "/status" {
                            let subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
                            let msg = if subscribers.contains(&chat_id) {
                                "Вы залогинены и будете получать отчеты."
                            } else {
                                "Вы не залогинены. Используйте /login password."
                            };
                            let _ = telegram_send_raw(&config.telegram_bot_token, &chat_id, msg).await;
                            continue;
                        }

                        if let Some(password) = text.trim().strip_prefix("/login ") {
                            if password.trim() == config.telegram_login_password {
                                let mut subscribers = load_telegram_subscribers(&config.telegram_subscribers_path);
                                if !subscribers.contains(&chat_id) {
                                    subscribers.push(chat_id.clone());
                                    let _ = save_telegram_subscribers(&config.telegram_subscribers_path, &subscribers);
                                }
                                let _ = telegram_send_raw(&config.telegram_bot_token, &chat_id, "Логин успешен. Вы будете получать отчеты каждые 5 часов.").await;
                            } else {
                                let _ = telegram_send_raw(&config.telegram_bot_token, &chat_id, "Неверный пароль.").await;
                            }
                        }
                    }
                }
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}
