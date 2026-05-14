use anyhow::Result;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use sha2::Sha256;
use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct MexcTradeClient {
    client: reqwest::Client,
    api_key: String,
    api_secret: String,
    pub open_positions: Arc<AtomicI32>,
    pub max_positions: i32,
}

impl MexcTradeClient {
    pub fn new(api_key: String, api_secret: String, max_positions: i32) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .unwrap(),
            api_key,
            api_secret,
            open_positions: Arc::new(AtomicI32::new(0)),
            max_positions,
        }
    }

    pub fn can_open(&self) -> bool {
        self.open_positions.load(Ordering::Relaxed) < self.max_positions
    }

    fn ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    fn sign(&self, ts: u64, body: &str) -> String {
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("hmac init");
        mac.update(format!("{}{}{}", self.api_key, ts, body).as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    async fn post(&self, path: &str, body: String) -> Result<serde_json::Value> {
        let ts = Self::ts();
        let sig = self.sign(ts, &body);
        let text = self
            .client
            .post(format!("https://contract.mexc.com{path}"))
            .header("ApiKey", &self.api_key)
            .header("Request-Time", ts.to_string())
            .header("Signature", sig)
            .header("Recv-Window", "5000")
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await?
            .text()
            .await?;
        eprintln!("[trade] POST {path}: {text}");
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
    }

    async fn get(&self, path: &str) -> Result<serde_json::Value> {
        let ts = Self::ts();
        let sig = self.sign(ts, "");
        let text = self
            .client
            .get(format!("https://contract.mexc.com{path}"))
            .header("ApiKey", &self.api_key)
            .header("Request-Time", ts.to_string())
            .header("Signature", sig)
            .header("Recv-Window", "5000")
            .send()
            .await?
            .text()
            .await?;
        eprintln!("[trade] GET {path}: {text}");
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
    }

    async fn submit_order(
        &self,
        symbol: &str,
        side: i32,
        order_type: i32,
        price: Decimal,
        vol: &str,
        leverage: i32,
    ) -> Result<String> {
        let price_dp = match symbol {
            s if s.starts_with("BTC") => 1u32,
            s if s.starts_with("ETH") => 2,
            _ => 2,
        };
        let body = serde_json::json!({
            "symbol": symbol,
            "price": price.round_dp(price_dp).to_string(),
            "vol": vol,
            "leverage": leverage,
            "side": side,
            "type": order_type,
            "openType": 1,  // isolated margin
        })
        .to_string();
        let v = self.post("/api/v1/private/order/submit", body).await?;
        if v["success"].as_bool() != Some(true) {
            anyhow::bail!("order rejected: {v}");
        }
        // data is returned as a number (order id)
        let id = if let Some(n) = v["data"].as_i64() {
            n.to_string()
        } else {
            v["data"].as_str().unwrap_or("").to_string()
        };
        Ok(id)
    }

    async fn order_filled(&self, order_id: &str) -> bool {
        match self
            .get(&format!("/api/v1/private/order/get/{order_id}"))
            .await
        {
            Ok(v) => v["data"]["state"].as_i64() == Some(3), // 3 = filled
            Err(_) => false,
        }
    }

    pub async fn run_trade(
        self: Arc<Self>,
        symbol: String,
        direction: i32,       // 1 = long (MEXC < Binance), -1 = short (MEXC > Binance)
        entry_price: Decimal, // current MEXC mid
        target_price: Decimal, // Binance mid (where MEXC should move)
        vol: String,
        leverage: i32,
        safety_timeout_ms: u64,
        close_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        if !self.can_open() {
            eprintln!("[trade] max positions ({}) reached, skipping {symbol}", self.max_positions);
            return;
        }
        self.open_positions.fetch_add(1, Ordering::Relaxed);

        // side: 1=open_long, 3=open_short, 4=close_long, 2=close_short
        let open_side = if direction > 0 { 1 } else { 3 };
        let close_side = if direction > 0 { 4 } else { 2 };
        let dir_str = if direction > 0 { "LONG" } else { "SHORT" };

        // 1. Market open for immediate fill (type=5)
        let entry_id = match self
            .submit_order(&symbol, open_side, 5, entry_price, &vol, leverage)
            .await
        {
            Ok(id) => {
                eprintln!("[trade] opened {dir_str} {symbol} vol={vol} entry={entry_price} target={target_price} id={id}");
                id
            }
            Err(e) => {
                eprintln!("[trade] open failed {symbol}: {e}");
                self.open_positions.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        };
        let _ = entry_id; // logged above

        // 2. Limit exit at target price (type=1, maker = 0% fee)
        let exit_id = match self
            .submit_order(&symbol, close_side, 1, target_price, &vol, 0)
            .await
        {
            Ok(id) => {
                eprintln!("[trade] exit limit placed {symbol} target={target_price} id={id}");
                id
            }
            Err(e) => {
                eprintln!("[trade] exit limit failed {symbol}: {e}");
                String::new()
            }
        };

        // 3. Wait for diff-collapsed signal or safety timeout
        tokio::select! {
            _ = close_rx => { eprintln!("[trade] close signal: diff collapsed for {symbol}"); }
            _ = sleep(Duration::from_millis(safety_timeout_ms)) => { eprintln!("[trade] safety timeout for {symbol}"); }
        }

        // 4. Check if limit exit already filled
        if !exit_id.is_empty() && self.order_filled(&exit_id).await {
            eprintln!("[trade] CLOSED {symbol} via limit at {target_price} ✓");
            self.open_positions.fetch_sub(1, Ordering::Relaxed);
            return;
        }

        // 5. Cancel limit exit
        if !exit_id.is_empty() {
            let cancel_body = serde_json::json!([exit_id]).to_string();
            let _ = self.post("/api/v1/private/order/cancel", cancel_body).await;
        }

        // 6. Force market close
        match self
            .submit_order(&symbol, close_side, 5, target_price, &vol, 0)
            .await
        {
            Ok(id) => eprintln!("[trade] CLOSED {symbol} via market (timeout) id={id}"),
            Err(e) => eprintln!("[trade] force close FAILED {symbol}: {e}"),
        }

        self.open_positions.fetch_sub(1, Ordering::Relaxed);
    }
}
