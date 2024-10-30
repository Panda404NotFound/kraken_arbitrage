use crate::config::Config;
use crate::tokens::{TokenLiquidity, get_mandatory_tokens};
use crate::data::{LockFreeTokenDataStore, TokenDataUpdate, 
    OrderBookUpdate, OrderBookEntry, update_token_data};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use url::Url;
use log::{info, debug, warn};
use hashbrown::HashSet;
use prometheus::IntGauge;
use anyhow::{Result, anyhow};
use serde_json::{json, Value};
use async_channel::{Sender, unbounded};

use futures_util::SinkExt;
use futures::stream::StreamExt;

use tokio::sync::mpsc;
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};


pub struct WebSocketManager {
    token_data_store: Arc<LockFreeTokenDataStore>,
    subscription_receiver: mpsc::Receiver<Vec<TokenLiquidity>>,
    current_subscriptions: HashSet<String>,
    updating_subscriptions: AtomicBool,
    unused_pairs_receiver: mpsc::Receiver<Vec<String>>,
    token_update_sender: mpsc::Sender<Vec<String>>,
    metrics: WebSocketMetrics,
    config: Arc<Config>,
}

struct WebSocketMetrics {
    active_connections: IntGauge,
}

impl WebSocketMetrics {
    fn new() -> Self {
        WebSocketMetrics {
            active_connections: IntGauge::new("websocket_active_connections", "Number of active WebSocket connections").unwrap(),
        }
    }
}

impl WebSocketManager {
    pub fn new(
        token_data_store: Arc<LockFreeTokenDataStore>,
        subscription_receiver: mpsc::Receiver<Vec<TokenLiquidity>>,
        unused_pairs_receiver: mpsc::Receiver<Vec<String>>,
        token_update_sender: mpsc::Sender<Vec<String>>,
        config: Arc<Config>,
    ) -> Self {
        Self {
            token_data_store,
            subscription_receiver,
            current_subscriptions: HashSet::new(),
            updating_subscriptions: AtomicBool::new(false),
            unused_pairs_receiver,
            token_update_sender,
            metrics: WebSocketMetrics::new(),
            config,
        }
    }

    pub async fn run(&mut self, initial_tokens: Vec<TokenLiquidity>) -> Result<()> {
        let url = Url::parse("wss://ws.kraken.com/v2")?;
        self.current_subscriptions = initial_tokens.iter().map(|t| t.symbol.clone()).collect();

        // Получаем обязательные токены
        let mandatory_tokens = get_mandatory_tokens(&self.config).await;
        
        // Объединяем начальные и обязательные токены
        let all_tokens = [initial_tokens, mandatory_tokens].concat();
    
        self.current_subscriptions = all_tokens.iter().map(|t| t.symbol.clone()).collect();

        let (tx, _rx) = unbounded::<(String, OrderBookUpdate)>();

        loop {
            match self.connect_and_handle(&url, tx.clone()).await {
                Ok(_) => info!("WebSocket connection closed normally. Reconnecting..."),
                Err(e) => info!("WebSocket error: {:?}. Attempting to reconnect...", e),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub fn remove_token_from_structures(
        symbol: &str, 
        token_data_store: &Arc<LockFreeTokenDataStore>,
        token_liquidity: &mut Vec<TokenLiquidity>
    ) {
        let formatted_symbol = symbol.replace("/", "");
        
        if token_data_store.get_token_data(&formatted_symbol).is_some() || token_liquidity.iter().any(|t| t.symbol == formatted_symbol) {
            info!("Найден символ в прямом порядке: {} -> {}", symbol, formatted_symbol);
        } else {
            // Инвертируем символ
            let parts: Vec<&str> = symbol.split('/').collect();
            if parts.len() == 2 {
                let inverted_symbol = format!("{}{}", parts[1], parts[0]);
                if token_data_store.get_token_data(&inverted_symbol).is_some() || token_liquidity.iter().any(|t| t.symbol == inverted_symbol) {
                    info!("Найден символ в инвертированном порядке: {} -> {}", symbol, inverted_symbol);
                } else {
                    info!("Символ не найден ни в прямом, ни в инвертированном порядке: {}", symbol);
                    return; // Выходим из функции, так как символ не найден
                }
            } else {
                info!("Неверный формат символа: {}", symbol);
                return; // Выходим из функции, так как формат символа неверный
            }
        }

        // Удаление данных из LockFreeTokenDataStore
        token_data_store.remove_token_data(&formatted_symbol);
        
        // Удаление токена из вектора TokenLiquidity
        token_liquidity.retain(|token| token.symbol != formatted_symbol);

        info!("Удалены данные для токена: {} (форматированный символ: {}) из token_data_store и TokenLiquidity", symbol, formatted_symbol);
    }

    async fn send_subscriptions_in_chunks(&self, write: &mut futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>, tokens: Vec<String>) -> Result<()> {
        let chunk_size = 50;
        for chunk in tokens.chunks(chunk_size) {
            let subscribe_message = self.create_subscribe_message(chunk);
            write.send(Message::Text(subscribe_message.to_string())).await?;
            info!("Отправлено сообщение подписки для чанка: {}", subscribe_message);
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(())
    }

    #[allow(unused_variables)]
    async fn connect_and_handle(&mut self, url: &Url, tx: Sender<(String, OrderBookUpdate)>) -> Result<()> {
        info!("Connecting to WebSocket...");
        let (ws_stream, _) = connect_async(url).await?;
        info!("WebSocket in websocket.rs connected successfully.");

        let (mut write, mut read) = ws_stream.split();

        self.metrics.active_connections.inc();

        let tokens_to_subscribe: Vec<String> = self.current_subscriptions.iter().cloned().collect();
        self.send_subscriptions_in_chunks(&mut write, tokens_to_subscribe).await?;
        info!("Отправлены все начальные сообщения подписки");

        let last_data_time = Instant::now();
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if last_data_time.elapsed() > Duration::from_secs(60 * 60 * 60) {
                        info!("No data received for 60 hours. Reconnecting...");
                        self.metrics.active_connections.dec();
                        return Ok(());
                    }
                }
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![])).await?;
                    info!("Ping sent");
                }
                Some(message) = read.next() => {
                    match message {
                        Ok(Message::Text(text)) => {
                            //info!("Received message: {}", text);
                            let start = Instant::now();
                            //message_count.fetch_add(1, Ordering::Relaxed);
                            
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(channel) = v.get("channel").and_then(Value::as_str) {
                                match channel {
                                    "book" => {
                                        if let Some(message_type) = v.get("type").and_then(Value::as_str) {
                                            match message_type {
                                                "snapshot" => {
                                                    self.process_snapshot(&v).await?;
                                                }
                                                "update" => {
                                                    self.process_l2update(&v, &tx).await?;
                                                }
                                                _ => {
                                                    info!("Получено неизвестное сообщение типа '{}' для канала 'book'", message_type);
                                                }
                                            }
                                        }
                                    }
                                    "status" => {
                                        if let Some(data) = v.get("data").and_then(Value::as_array) {
                                            if let Some(status_data) = data.first() {
                                                info!("Получено сообщение о статусе: {:?}", status_data);
                                            }
                                        }
                                    }
                                    "heartbeat" => {
                                        #[cfg(debug_assertions)]
                                        debug!("Получено сообщение heartbeat");
                                    }
                                    _ => {
                                        info!("Получено сообщение для неизвестного канала: {}", channel);
                                    }
                                }
                            } else if let Some(method) = v.get("method").and_then(Value::as_str) {
                                match method {
                                    "subscribe" => {
                                        if let Some(result) = v.get("result") {
                                            info!("Успешная подписка: {:?}", result);
                                        }
                                    }
                                    "unsubscribe" => {
                                        if let Some(result) = v.get("result") {
                                            info!("Успешная отписка: {:?}", result);
                                        }
                                    }
                                    _ => {
                                        info!("Получено сообщение с неизвестным методом: {}", method);
                                    }
                                }
                            } else {
                                info!("Получено неопознанное сообщение: {:?}", v);
                            }
                            let duration = start.elapsed();
                            //info!("Время обработки сообщения WebSocket: {:?}", duration);
                        }
                        
                        Ok(Message::Pong(_)) => {
                            info!("Pong received");
                        }
                        Ok(Message::Close(frame)) => {
                            warn!("WebSocket closed: {:?}", frame);
                            self.metrics.active_connections.dec();
                            return Ok(());
                        }
                        Err(e) => {
                            eprintln!("WebSocket error: {:?}", e);
                            self.metrics.active_connections.dec();
                            return Err(anyhow!("WebSocket error: {:?}", e));
                        }
                        _ => {}
                    }
                }
                result = self.subscription_receiver.recv() => {
                    if let Some(new_tokens) = result {
                        self.update_subscriptions(&mut write, new_tokens, Vec::new()).await?;
                    } else {
                        self.metrics.active_connections.dec();
                        return Ok(());
                    }
                }
                
                // Добавляем новую ветку для обработки неиспользуемых пар
                Some(unused_pairs) = self.unused_pairs_receiver.recv() => {
                    info!("Received unused pairs: {:?}", unused_pairs);
                    self.update_subscriptions(&mut write, Vec::new(), unused_pairs).await?;
                }
            }
        }
    }

    async fn update_subscriptions(
        &mut self,
        write: &mut futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut new_tokens: Vec<TokenLiquidity>,
        tokens_to_unsubscribe: Vec<String>
    ) -> Result<()> {
        
        if self.updating_subscriptions.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            info!("Subscription update already in progress, skipping...");
            return Ok(());
        }
    
        let new_subscriptions: HashSet<String> = new_tokens.iter().map(|t| t.symbol.clone()).collect();
        
        info!("Updating subscriptions. Current: {:?}, New: {:?}", self.current_subscriptions, new_subscriptions);

        let to_subscribe: Vec<String> = new_subscriptions.difference(&self.current_subscriptions).cloned().collect();
    
        let to_unsubscribe = tokens_to_unsubscribe
        .into_iter()
        .filter(|pair| self.current_subscriptions.contains(pair))
        .collect::<Vec<String>>();
    
        info!("Tokens to unsubscribe: {:?}", to_unsubscribe);
        info!("Tokens to subscribe: {:?}", to_subscribe);
    
        if !to_unsubscribe.is_empty() {
            let unsubscribe_message = self.create_unsubscribe_message(&to_unsubscribe);
            write.send(Message::Text(unsubscribe_message.to_string())).await?;
            info!("Successfully unsubscribed from: {:?}", to_unsubscribe);
            for symbol in &to_unsubscribe {
                self.current_subscriptions.remove(symbol);
                Self::remove_token_from_structures(symbol, &self.token_data_store, &mut new_tokens);
            }
        
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }

        if !to_unsubscribe.is_empty() {
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }
    
        if !to_subscribe.is_empty() {
            let subscribe_message = self.create_subscribe_message(&to_subscribe);
            write.send(Message::Text(subscribe_message.to_string())).await?;
            info!("Успешно подписались на: {:?}", to_subscribe);
            for symbol in &to_subscribe {
                self.current_subscriptions.insert(symbol.clone());
            }
        }
    
        if !to_subscribe.is_empty() || !to_unsubscribe.is_empty() {
            let updated_tokens: Vec<String> = self.current_subscriptions.iter().cloned().collect();
            if let Err(e) = self.token_update_sender.send(updated_tokens).await {
                info!("Failed to send token updates to Graph: {:?}", e);
            }
        }
    
        #[cfg(debug_assertions)]
        info!("Subscription update complete. Current subscriptions: {:?}", self.current_subscriptions);
        
        self.updating_subscriptions.store(false, Ordering::Release);
        
        Ok(())
    }

    fn create_subscribe_message(&self, symbols: &[String]) -> Value {
        info!("Создаём сообщение для подписки на {} символов", symbols.len());
    
        json!({
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": symbols
            }
        })
    }
    
    fn create_unsubscribe_message(&self, symbols: &[String]) -> Value {
        info!("Создаём сообщение для отписки от {} символов", symbols.len());
    
        json!({
            "method": "unsubscribe",
            "params": {
                "channel": "book",
                "symbol": symbols
            }
        })
    }

    // Метод для обработки сообщений типа "snapshot"
    async fn process_snapshot(&self, message: &Value) -> Result<()> {
        let _start = Instant::now();
        let data = message.get("data").and_then(|d| d.as_array()).and_then(|arr| arr.first())
            .ok_or_else(|| anyhow!("Отсутствуют данные в сообщении snapshot"))?;
        let symbol = data.get("symbol").and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Отсутствует 'symbol' в сообщении snapshot"))?;
        if !self.current_subscriptions.contains(symbol) {
            return Ok(());
        }
    
        let token_data_update = self.parse_snapshot(data, symbol)?;
        update_token_data(&self.token_data_store, token_data_update);

        //info!("Обновлён начальный слепок ордербука для {} за {:?} времени", symbol, _start.elapsed());

        Ok(())
    }

    // Метод для обработки сообщений типа "l2update"
    async fn process_l2update(&self, message: &Value, tx: &Sender<(String, OrderBookUpdate)>) -> Result<()> {
        let _start = Instant::now();
        let data = message.get("data").and_then(|d| d.as_array()).and_then(|arr| arr.first())
            .ok_or_else(|| anyhow!("Отсутствуют данные в сообщении l2update"))?;
        let symbol = data.get("symbol").and_then(Value::as_str)
            .ok_or_else(|| anyhow!("Отсутствует 'symbol' в сообщении l2update"))?;
        if !self.current_subscriptions.contains(symbol) {
            return Ok(());
        }
    
        let order_book_update = self.parse_order_book_update(data, symbol)?;
    
        self.token_data_store.update_order_book(symbol, order_book_update.clone());
    
        tx.send((symbol.to_string(), order_book_update)).await?;
    
        //info!("Обработано обновление для символа: {} за {:?} времени", symbol, _start.elapsed());

        Ok(())
    }

    fn parse_snapshot(&self, data: &Value, symbol: &str) -> Result<TokenDataUpdate> {
        let parse_entries = |entries: &Value| -> Result<Vec<OrderBookEntry>> {
            entries.as_array()
                .ok_or_else(|| anyhow!("Некорректные данные ордербука"))?
                .iter()
                .map(|entry| {
                    let price = entry.get("price").and_then(Value::as_f64)
                        .ok_or_else(|| anyhow!("Некорректная цена"))?;
                    let volume = entry.get("qty").and_then(Value::as_f64)
                        .ok_or_else(|| anyhow!("Некорректный объём"))?;
                    Ok(OrderBookEntry { price, volume, version: 0 })
                })
                .collect()
        };
    
        let bids = parse_entries(data.get("bids").unwrap_or(&Value::Null))?;
        let asks = parse_entries(data.get("asks").unwrap_or(&Value::Null))?;
    
        Ok(TokenDataUpdate {
            symbol: symbol.replace("/", ""),
            order_book_update: OrderBookUpdate {
                symbol: symbol.replace("/", ""),
                bids_updates: bids,
                asks_updates: asks,
            },
        })
    }
    
    fn parse_order_book_update(&self, data: &Value, symbol: &str) -> Result<OrderBookUpdate> {
        let parse_entries = |entries: &Value| -> Result<Vec<OrderBookEntry>> {
            entries.as_array()
                .ok_or_else(|| anyhow!("Некорректные данные обновления"))?
                .iter()
                .map(|entry| {
                    let price = entry.get("price").and_then(Value::as_f64)
                        .ok_or_else(|| anyhow!("Некорректная цена"))?;
                    let volume = entry.get("qty").and_then(Value::as_f64)
                        .ok_or_else(|| anyhow!("Некорректный объём"))?;
                    Ok(OrderBookEntry { price, volume, version: 0 })
                })
                .collect()
        };
    
        let bids_updates = parse_entries(data.get("bids").unwrap_or(&Value::Null))?;
        let asks_updates = parse_entries(data.get("asks").unwrap_or(&Value::Null))?;
    
        Ok(OrderBookUpdate {
            symbol: symbol.replace("/", ""),
            bids_updates,
            asks_updates,
        })
    }
}

