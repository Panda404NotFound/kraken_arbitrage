// Статусы кодов: 50, 52, 53, 54
// 50 не критический 
// 52, 53, 54 критические 
// Реализовать валидную обработку каждого кода 
// И дополнительную логику 400/401 ошибок с проверкой баланса 

// Можно и нужно добавить RateLimits по операциям
// создавая новую структуру данных для лимитов 
// которая будет парсится вместе с handle_message 
// 
// Относительно лимитов корректировать логику действий бота  

use url::Url;
use reqwest::Client;
use log::{info, error, debug, warn};
use serde_json::{json, Value};

use dashmap::DashMap;
use hashbrown::HashMap;
use once_cell::sync::Lazy;

use base64;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha512, Digest};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{interval, sleep};

use std::sync::Arc;
use std::collections::VecDeque;
use std::sync::atomic::{Ordering, AtomicBool};
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};

use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream, MaybeTlsStream};

use crate::config::Config;
use crate::tokenfilter;
use crate::tokenfilter::IncrementStore;
use crate::data::LockFreeTokenDataStore;
// use crate::tokenfilter::{fetch_product_data, IncrementStore};
use crate::math_graph::{MathGraph, ChainResult, OperationInfo, Operation};


#[derive(Clone, Debug)]
struct WebSocketMessage {
    message: String,
    response_sender: mpsc::Sender<Result<(f64, bool), Box<dyn std::error::Error + Send + Sync>>>,
}

#[derive(Clone, Debug)]
struct ChainExecutionInfo {
    chain: Vec<String>,
    profit_percentage: f64,
    count: f64,
    timestamp: Instant,
    is_ready: bool,
    chain_result: Option<ChainResult>,
}

impl ChainExecutionInfo {
    fn new(chain: Vec<String>, profit_percentage: f64) -> Self {
        ChainExecutionInfo {
            chain,
            profit_percentage,
            count: 0.0,
            timestamp: Instant::now(),
            is_ready: false,
            chain_result: None,
        }
    }
}

// Новый блок для kraken документации

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct WebSocketToken {
    token: String,
    expires: u64,
}

impl WebSocketToken {
    fn new(token: String, expires: u64) -> Self {
        WebSocketToken { token, expires }
    }
}

// Структура для хранения текущего ордера
#[allow(dead_code)]
#[derive(Clone, Debug)]
struct PendingOrder {
    message: WebSocketMessage,
    start_time: Instant,
}

/*
#[derive(Clone, Debug)]
struct AssetBalance {
    balance: f64,
    wallets: Vec<WalletInfo>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct WalletInfo {
    wallet_type: String,
    id: String,
    balance: f64,
}

#[derive(Clone, Debug)]
struct BalanceStore {
    balances: HashMap<String, AssetBalance>,
}

impl BalanceStore {
    fn new() -> Self {
        BalanceStore {
            balances: HashMap::new(),
        }
    }

    fn update(&mut self, asset: String, balance: f64, wallet_info: WalletInfo) {
        let asset_balance = self.balances.entry(asset).or_insert(AssetBalance {
            balance,
            wallets: Vec::new(),
        });
        asset_balance.balance = balance;
        if let Some(existing_wallet) = asset_balance.wallets.iter_mut().find(|w| w.id == wallet_info.id) {
            *existing_wallet = wallet_info;
        } else {
            asset_balance.wallets.push(wallet_info);
        }
    }
}
*/
// Новый блок для kraken документации

#[derive(Debug, Clone, Default)]
pub struct BotErrors {
    pub order_execution_error: Option<String>,
    pub websocket_error: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum BotAction {
    CycleStart,
    ChainSelected { chain: Vec<String>, expected_profit: f64 },
    OperationExecuted { 
        from_token: String, 
        to_token: String,
        symbol: String,
        operation: String,
        amount: f64,
        previous_balance: f64,
        new_balance: f64
    },
    CycleCompleted { 
        initial_balance: f64, 
        final_balance: f64, 
        profit_percentage: f64,
        intermediate_results: Vec<BotAction>,
    },
}

pub struct ProfitableChainStore {
    chains: DashMap<Vec<String>, ChainExecutionInfo>,
    config: Arc<Config>,
    math_graph: Arc<MathGraph>,
}

impl ProfitableChainStore {
    pub fn new(config: Arc<Config>, math_graph: Arc<MathGraph>) -> Self {
        ProfitableChainStore {
            chains: DashMap::new(),
            config,
            math_graph,
        }
    }

    fn verify_chain_exists(&self, chain: &[String]) -> Option<ChainResult> {
        let chain_key = chain.join("->");
        let result = self.math_graph.profitable_chains
            .get(&chain_key)
            .map(|entry| entry.value().clone());

        if result.is_some() {
            #[cfg(debug_assertions)]
            info!("Цепочка найдена: ключ = {}", chain_key);
        } else {
            #[cfg(debug_assertions)]
            info!("Цепочка не найдена: ключ = {}", chain_key);
        }

        result
    }

    fn add_or_update_chain(&self, chain_result: ChainResult, basic_threshold: f64) {
        let chain = chain_result.chain.clone();
        let profit_percentage = chain_result.profit_percentage;
        
        match self.verify_chain_exists(&chain) {
            Some(current_chain_result) => {
                if profit_percentage >= basic_threshold {
                    // Клонируем current_chain_result перед использованием в замыканиях
                    let chain_result_for_modify = current_chain_result.clone();
                    let chain_result_for_insert = current_chain_result.clone();
                    
                    self.chains.entry(chain.clone())
                        .and_modify(|info| {
                            let significant_change = (info.profit_percentage - profit_percentage).abs() > 0.01;
                            
                            #[cfg(debug_assertions)]
                            info!("Обновление цепочки {:?}: Старая прибыль: {:.4}%, Новая прибыль: {:.4}%", 
                                chain, info.profit_percentage * 100.0, profit_percentage * 100.0);
                            
                            info.profit_percentage = profit_percentage;
                            info.chain_result = Some(chain_result_for_modify);
                            
                            if significant_change {
                                info.is_ready = false;
                                // info.timestamp = Instant::now();
                                info!("Существенное изменение прибыли, сброс готовности цепочки {:?}", chain);
                            }
                        })
                        .or_insert_with(|| {
                            info!("Добавление новой цепочки {:?} с прибылью: {:.4}%", 
                                chain, profit_percentage * 100.0);
                            let mut info = ChainExecutionInfo::new(chain, profit_percentage);
                            info.chain_result = Some(chain_result_for_insert);
                            info
                        });
                } else {
                    if let Some(_removed) = self.chains.remove(&chain) {
                        info!("Удалена цепочка {:?} из-за низкой прибыли: {:.4}%", 
                            chain, profit_percentage * 100.0);
                    }
                }
            },
            None => {
                if let Some(_removed) = self.chains.remove(&chain) {
                    info!("Удалена цепочка {:?}, так как она больше не существует в MathGraph", chain);
                }
            }
        }
    }

    fn get_valid_chains(&self, basic_threshold: f64, profit_threshold: f64) -> Vec<ChainExecutionInfo> {
        self.chains
            .iter()
            .filter(|entry| {
                if let Some(_current_chain) = self.verify_chain_exists(entry.key()) {
                    let info = entry.value();
                    let meets_basic = info.profit_percentage >= basic_threshold;
                    let meets_profit = info.profit_percentage >= profit_threshold;
                    let is_ready = info.is_ready;

                    #[cfg(debug_assertions)]
                    info!("Проверка цепочки {:?}: meets_basic = {}, meets_profit = {}, is_ready = {}", 
                        entry.key(), meets_basic, meets_profit, is_ready);

                    meets_basic && meets_profit && is_ready
                } else {
                    false
                }
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn update_chain_validity(&self, basic_threshold: f64) {
        let now = Instant::now();
        let mut keys_to_remove = Vec::new();
    
        for mut entry in self.chains.iter_mut() {
            // Проверяем существование в MathGraph
            match self.verify_chain_exists(entry.key()) {
                Some(_current_chain) => {
                    let info = entry.value_mut();
                    let elapsed = now.duration_since(info.timestamp);
                    
                    if info.profit_percentage < basic_threshold {
                        info!("Цепочка {:?} будет удалена из-за низкой прибыльности: {:.4}%", 
                            info.chain, info.profit_percentage * 100.0);
                        keys_to_remove.push(entry.key().clone());
                    } 
                    else if info.profit_percentage >= self.config.profit_threshold + 0.04 {
                        if elapsed >= Duration::from_secs_f64(2.4) && !info.is_ready {
                            info.is_ready = true;
                            info!("Цепочка {:?} теперь готова к использованию после превышения profit_threshold + 4% (прошло {:.2} сек), прибыльность: {:.4}%", 
                                info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                        } else {
                            info!("Цепочка {:?} еще не готова после превышения profit_threshold + 4% (прошло {:.2} сек), прибыльность: {:.4}%", 
                                info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                        }
                    }
                    else if elapsed >= Duration::from_secs_f64(1.4) && !info.is_ready {
                        info.is_ready = true;
                        info!("Цепочка {:?} теперь готова к использованию (прошло {:.2} сек), прибыльность: {:.4}%", 
                            info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                    } 
                    else {
                        info!("Цепочка {:?} еще не готова (прошло {:.2} сек), прибыльность: {:.4}%", 
                            info.chain, elapsed.as_secs_f64(), info.profit_percentage * 100.0);
                    }
                },
                None => {
                    info!("Цепочка {:?} будет удалена, так как больше не существует в MathGraph", 
                        entry.key());
                    keys_to_remove.push(entry.key().clone());
                }
            }
        }
    
        // Удаляем помеченные цепочки
        for key in keys_to_remove {
            self.chains.remove(&key);
            info!("Удалена цепочка {:?}", key);
        }
    }
}

pub struct Bot {
    config: Arc<Config>,
    current_balance: f64,
    current_token: String,
    time_offset: i64,
    math_graph: Arc<MathGraph>,
    actions: Arc<RwLock<Vec<BotAction>>>,
    current_operation_index: usize,
    intermediate_results: Vec<BotAction>,
    current_chain: Option<ChainResult>,
    last_executed_chains: VecDeque<ChainExecutionInfo>,
    chain_execution_info: HashMap<Vec<String>, ChainExecutionInfo>,
    last_clean_time: Instant,
    write: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    message_sender: mpsc::Sender<WebSocketMessage>,
    message_receiver: mpsc::Receiver<WebSocketMessage>,
    token_data_store: Arc<LockFreeTokenDataStore>,
    step_size_store: Arc<RwLock<IncrementStore>>,
    pub errors: Arc<RwLock<BotErrors>>,
    profitable_chain_store: Arc<ProfitableChainStore>,
    bot_action_sender: broadcast::Sender<BotAction>,
    websocket_token: Arc<RwLock<Option<WebSocketToken>>>,
    pending_order: Option<PendingOrder>,
    //balance_store: Arc<RwLock<BalanceStore>>,
}

impl Clone for Bot {
    fn clone(&self) -> Self {
        let (_new_sender, new_receiver) = mpsc::channel(100);
        Bot {
            config: Arc::clone(&self.config),
            current_balance: self.current_balance,
            current_token: self.current_token.clone(),
            time_offset: self.time_offset,
            math_graph: Arc::clone(&self.math_graph),
            actions: Arc::clone(&self.actions),
            current_operation_index: self.current_operation_index,
            intermediate_results: self.intermediate_results.clone(),
            current_chain: self.current_chain.clone(),
            last_executed_chains: self.last_executed_chains.clone(),
            chain_execution_info: self.chain_execution_info.clone(),
            last_clean_time: self.last_clean_time,
            write: None,
            message_sender: self.message_sender.clone(),
            message_receiver: new_receiver,
            token_data_store: Arc::clone(&self.token_data_store),
            step_size_store: Arc::clone(&self.step_size_store),
            errors: Arc::clone(&self.errors),
            profitable_chain_store: Arc::new(ProfitableChainStore::new(
                Arc::clone(&self.config),
                Arc::clone(&self.math_graph)
            )),
            bot_action_sender: self.bot_action_sender.clone(),
            websocket_token: Arc::clone(&self.websocket_token),
            pending_order: self.pending_order.clone(),
            //balance_store: Arc::clone(&self.balance_store),
        }
    }
}

impl Bot {
    pub fn new(
        config: Arc<Config>,
        math_graph: Arc<MathGraph>,
        token_data_store: Arc<LockFreeTokenDataStore>,
        bot_action_sender: broadcast::Sender<BotAction>,
    ) -> Self {
        let (message_sender, message_receiver) = mpsc::channel(100);
        // Клонируем Arc перед использованием
        let config_clone = Arc::clone(&config);
        let math_graph_clone = Arc::clone(&math_graph);
        Bot {
            current_balance: config.wallet_balance,
            current_token: config.start_end_token.to_string(),
            config,
            time_offset: 0,
            math_graph,
            actions: Arc::new(RwLock::new(Vec::new())),
            current_operation_index: 0,
            intermediate_results: Vec::new(),
            current_chain: None,
            last_executed_chains: VecDeque::with_capacity(10),
            chain_execution_info: HashMap::new(),
            last_clean_time: Instant::now(),
            write: None,
            message_sender,
            message_receiver,
            token_data_store,
            step_size_store: Arc::new(RwLock::new(IncrementStore::new())),
            errors: Arc::new(RwLock::new(BotErrors::default())),
            profitable_chain_store: Arc::new(ProfitableChainStore::new(
                config_clone,
                math_graph_clone
            )),
            bot_action_sender,
            websocket_token: Arc::new(RwLock::new(None)),
            pending_order: None,
            //balance_store: Arc::new(RwLock::new(BalanceStore::new())),
        }
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.engine().await
    }

    pub async fn engine(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting Brain Bot");
        info!("Ждем 120 секунд в brain_bot.rs");

        tokio::time::sleep(Duration::from_secs(120)).await;

        let step_sizes = tokenfilter::fetch_symbols_data().await?;
        let store = Arc::clone(&self.step_size_store);
        tokio::spawn(async move {
            let mut guard = store.write().await;
            *guard = tokenfilter::IncrementStore { 
                symbols: step_sizes,
                tokens: Vec::new()
            };
            info!("Step_size_store успешно обновлен");
        });

        tokio::time::sleep(Duration::from_secs(5)).await;

        let server_time = self.get_server_time().await?;
        let local_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        self.time_offset = server_time - local_time;

        let url = Url::parse("wss://ws-auth.kraken.com/v2")?;

        loop {
            match self.connect_and_handle(&url).await {
                Ok(_) => info!("WebSocket connection closed normally. Reconnecting..."),
                Err(e) => {
                    error!("WebSocket error: {:?}. Closed connection", e);
                    if e.to_string().contains("Error 52") || e.to_string().contains("Error 53") || e.to_string().contains("Error 54") {

                        return Err(e);
                    } else {
                        error!("WebSocket error: {:?}. Closed connection", e);
                    }
                }
            }
            error!("Вызвано ожидание 1 секунд в цикле run. Перезапуск подключения...");
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn get_server_time(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let resp: Value = client.get("https://api.kraken.com/0/public/Time")
            .send()
            .await?
            .json()
            .await?;
        
        // Извлекаем значение "unixtime" из ответа
        let unixtime = resp["result"]["unixtime"].as_i64()
            .ok_or("Поле 'unixtime' отсутствует или не является целым числом")?;
        
        Ok(unixtime)
    }

    async fn send_message(&mut self, message: String, _is_buy: bool) -> Result<(f64, bool), Box<dyn std::error::Error + Send + Sync>> {
        let (response_sender, mut response_receiver) = mpsc::channel(100);
        
        let websocket_message = WebSocketMessage {
            message: message.clone(),
            response_sender,
        };
        
        self.message_sender.send(websocket_message).await?;
        info!("Отправленное сообщение из send_message {:.?}", message);
    
        info!("Waiting for response with 3 seconds timeout...");
        tokio::time::timeout(Duration::from_secs(3), response_receiver.recv()).await?
            .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "Error 54: Timed out waiting for response")) as Box<dyn std::error::Error + Send + Sync>)?
    }

    fn check_clean_struct(&mut self, force_clean: bool) {
        static IS_FIRST_CLEAN_CHECK: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));
        
        let now = Instant::now();
        
        if IS_FIRST_CLEAN_CHECK.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            info!("First call to check_clean_struct. Initializing timer.");
            self.last_clean_time = now;
            return;
        }
        
        let elapsed = now.duration_since(self.last_clean_time);
        
        info!("check_clean_struct called. Force clean: {}", force_clean);
        info!("Time since last clean: {:?}", elapsed);

        if force_clean && elapsed >= Duration::from_millis(1400) {
            info!("Cleaning chain_execution_info structure");
            self.chain_execution_info.clear();
            self.last_clean_time = now;
            info!("Structure cleaned. New last_clean_time set");
        } else if force_clean {
            info!("Force clean requested, but time haven't passed yet");
        }
    }

    fn format_float_truncate(value: f64, decimal_places: usize) -> f64 {
        if decimal_places == 0 {
            value.trunc()
        } else {
            let multiplier = 10f64.powi(decimal_places as i32);
            (value * multiplier).trunc() / multiplier
        }
    }

    async fn connect_and_handle(&mut self, url: &Url) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to WebSocket...");
        let (ws_stream, _) = connect_async(url).await?;
        info!("WebSocket connected successfully.");
        let (write, mut read) = ws_stream.split();
        self.write = Some(write);
        let mut write = self.write.take().unwrap();

        // Добавим новую функцию kraken_prepare
        self.kraken_prepare(&mut write).await?;
        self.write = Some(write);
    
        let mut ping_interval = interval(Duration::from_secs(30));
        let duration = Duration::from_secs(81420); // 22 часа 37 минут
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let timer_flag = Arc::new(AtomicBool::new(false));
    
        // Клонируем переменные для использования в задачах
        let shutdown_flag_clone1 = Arc::clone(&shutdown_flag);
        let shutdown_flag_clone2 = Arc::clone(&shutdown_flag);
        let timer_flag_clone1 = Arc::clone(&timer_flag);
        let timer_flag_clone2 = Arc::clone(&timer_flag);

        // Запускаем задачу для обработки Ctrl+C
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
            info!("Received Ctrl+C signal in Brain Bot. Preparing to stop bot.");
            shutdown_flag_clone1.store(true, Ordering::SeqCst);
        });
    
        // Запускаем задачу для отслеживания времени работы бота
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            info!("Bot runtime exceeded. Preparing to stop bot.");
            timer_flag_clone1.store(true, Ordering::SeqCst);
        });

        // Клонируем self для использования в trade_handle
        let mut self_clone = self.clone();
    
        // Запускаем задачу для выполнения торговых циклов
        let mut trade_handle = tokio::spawn(async move {
            loop {
                let cycle_start = Instant::now();

                if shutdown_flag_clone2.load(Ordering::SeqCst) {
                    info!("Shutdown signal received. Shutting down...");
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 54: Shutdown signal received")));
                }

                if timer_flag_clone2.load(Ordering::SeqCst) {
                    info!("Bot runtime exceeded. Shutting down...");
                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 55: Bot runtime exceeded or shutdown signal received")));
                }
        
                match self_clone.execute_trade_cycle().await {
                    Ok((action, is_completed)) => {
                        match action {
                            BotAction::OperationExecuted { .. } => {
                                // Обработка уже происходит внутри execute_trade_cycle
                            },
                            BotAction::CycleCompleted { .. } => {
                                info!("Trade cycle completed");
                                if is_completed {
                                    info!("All operations in the chain completed");
                                    info!("Основной цикл операций бота завершен за {:?}", cycle_start.elapsed());
                                    // Начинаем новый цикл
                                    //info!("Начинаем новый цикл");
                                    //tokio::time::sleep(Duration::from_secs(10)).await;
                                    //continue;
                                }
                            },
                            _ => {}
                        }
                    },
                    Err(e) => {
                        if e.to_string() == "Error Timestamp" {
                            let server_time = match self_clone.get_server_time().await {
                                Ok(time) => time,
                                Err(err) => {
                                    info!("Не удалось получить время сервера: {:?}", err);
                                    continue;
                                }
                            };
                            let local_time = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as i64;
                            self_clone.time_offset = server_time - local_time;
                            info!("Обновили смещение времени из-за ошибки таймстампа. Ошибка: {:?}", e);
                            continue;
                        } else if e.to_string() == "Restart cycle" {
                            info!("Перезапуск цикла из-за убыточной операции с начальным токеном USDT");
                            continue;
                        } else if e.to_string().contains("No profitable chains found") {
                            #[cfg(debug_assertions)]
                            debug!("Error: No profitable chains found: {:?}", e);
                            continue;
                        } else {
                            info!("Error in trade cycle: {:?}", e);
                            if e.to_string().contains("Error 52")
                                || e.to_string().contains("Error 53")
                                || e.to_string().contains("Error 54")
                            {
                                info!("Critical error occurred. Stopping trade cycles.");
                                return Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                )));
                            }
                        }
                    }                    
                }
                // В этой части происходит конец цикла
                // Для релиза убрать задержку
                //info!("Для loop ожидаем время следующего цикла");
                //tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    
        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Some(write) = &mut self.write {
                        write.send(Message::Ping(vec![])).await?;
                        info!("Ping sent");
                    }
                }
                Some(message) = read.next() => {
                    match message {
                        Ok(Message::Text(text)) => {
                            debug!("Получено сообщение: {}", text);
                            // Обработка системных сообщений
                            if text.contains("\"channel\":\"status\"") && text.contains("\"system\":\"online\"") {
                                info!("Успешное вебсокет подключение");
                                continue;
                            } 
                            if text.contains("{\"channel\":\"heartbeat\"}") {
                                info!("Получено сообщение heartbeat");
                                continue;
                            }
                            
                            // Проверяем подписку на канал executions
                            if text.contains("\"method\":\"subscribe\"") && text.contains("\"channel\":\"executions\"") {
                                let response: Value = serde_json::from_str(&text)?;
                                if let Some(success) = response["success"].as_bool() {
                                    if success {
                                        info!("Подписка на executions канал успешна: {}", text);
                                        continue;
                                    } else {
                                        let error_msg = format!("Error 52: Ошибка подписки на канал executions: {}", text);
                                        error!("{}", error_msg);
                                        return Err(Box::new(std::io::Error::new(
                                            std::io::ErrorKind::Other,
                                            error_msg
                                        )));
                                    }
                                }
                            }
            
                            // Если есть активный ордер - обрабатываем его статусы
                            if let Some(pending_order) = &self.pending_order {
                                let response: Value = serde_json::from_str(&text)?;
                                debug!("Получено сообщение с ордером: {}", text);
                                if let Some(error) = response["error"].as_str() {
                                    let error_result = self.handle_error_response(error).await;
                                    if let Err(e) = error_result {
                                        info!("Error 52: Error processing order: {:?}", e);
                                        let mut errors = self.errors.write().await;
                                        errors.order_execution_error = Some(format!("Error processing order: {:?}", e));
                                        let _ = pending_order.message.response_sender.send(Err(e)).await;
                                        self.pending_order = None;
                                    }
                                } else if response["method"] == "add_order" {
                                    if response["success"].as_bool() == Some(true) {
                                        debug!("Order successfully sent: {:?}", response["result"]);
                                    } else {
                                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Не удалось отправить ордер")));
                                    }
                                } else if let Some(data) = response["data"].as_array().and_then(|arr| arr.first()) {
                                    if let Some(status) = data["order_status"].as_str() {
                                        match status {
                                            "filled" => {
                                                // Парсим исходное сообщение для получения side
                                                let request_message: Value = serde_json::from_str(&pending_order.message.message)
                                                    .map_err(|e| Box::new(std::io::Error::new(
                                                        std::io::ErrorKind::Other,
                                                        format!("Error 52: Failed to parse order message: {}", e)
                                                    )))?;

                                                let is_buy = request_message["params"]["side"].as_str()
                                                    .map(|s| s == "buy")
                                                    .ok_or_else(|| Box::new(std::io::Error::new(
                                                        std::io::ErrorKind::Other,
                                                        "Error 52: Missing side in order request"
                                                    )))?;

                                                let (new_balance, success) = self.process_filled_order(data, is_buy).await?;
                                                debug!("Order filled: {}, is_buy: {}", new_balance, is_buy);
                                                let _ = pending_order.message.response_sender
                                                    .send(Ok((new_balance, success)))
                                                    .await;
                                                self.pending_order = None;
                                            },
                                            "canceled" | "expired" => {
                                                let err_msg = format!("Order {} - операция остановлена", status);
                                                error!("{}", err_msg);
                                                let _ = pending_order.message.response_sender
                                                    .send(Err(Box::new(
                                                        std::io::Error::new(std::io::ErrorKind::Other, 
                                                        format!("Error 52: {}", err_msg))
                                                    )))
                                                    .await;
                                                self.pending_order = None;
                                            },
                                            "pending_new" => {
                                                debug!("Order pending creation in engine");
                                            },
                                            "new" => {
                                                debug!("Order is live in engine");
                                            },
                                            "partially_filled" => {
                                                debug!("Order partially filled, execution continues");
                                            },
                                            _ => {
                                                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Неизвестный ответ от сервера")));
                                            }
                                        }
                                    } else {
                                        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Неизвестный ответ от сервера")));
                                    }
                                } else {
                                    return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Неизвестный ответ от сервера")));
                                }
                            }
                        }
                        Ok(Message::Pong(_)) => {
                            info!("Pong received");
                        },
                        Ok(Message::Close(frame)) => {
                            error!("WebSocket closed: {:?}", frame);
                            if let Some(pending_order) = &self.pending_order {
                                let _ = pending_order.message.response_sender.send(Err(Box::new(
                                    std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Error 52: WebSocket connection closed")
                                ))).await;
                            }
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: WebSocket connection closed")));
                        },
                        Err(e) => {
                            error!("WebSocket error: {:?}", e);
                            if let Some(pending_order) = &self.pending_order {
                                let _ = pending_order.message.response_sender.send(Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Error 52: WebSocket error: {:?}", e)
                                )))).await;
                            }
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Error 52: WebSocket error: {:?}", e)
                            )));
                        },
                        _ => {}
                    }
                }
                Some(websocket_message) = self.message_receiver.recv() => {
                    let cycle_start = Instant::now();
                    if let Some(write) = &mut self.write {
                        match write.send(Message::Text(websocket_message.message.clone())).await {
                            Ok(_) => {
                                info!("Order message sent successfully");
                                // Сохраняем информацию об отправленном ордере
                                self.pending_order = Some(PendingOrder {
                                    message: websocket_message,
                                    start_time: cycle_start,
                                });
                            },
                            Err(e) => {
                                error!("Error 54: Failed to send order message: {:?}", e);
                                let mut errors = self.errors.write().await;
                                errors.websocket_error = Some(format!("Failed to send order message: {:?}", e));
                                
                                // В случае ошибки отправки сразу уведомляем sender
                                let err = Box::new(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Error 54: Failed to send order: {}", e)
                                ));
                                let _ = websocket_message.response_sender.send(Err(err)).await;
                            },
                        }
                    } else {
                        error!("Error 54: WebSocket connection not established");
                        let err = Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Error 54: WebSocket connection not established"
                        ));
                        let _ = websocket_message.response_sender.send(Err(err)).await;
                    }
                }
                result = &mut trade_handle => {
                    match result {
                        Ok(Ok(())) => {
                            info!("Trade handle completed successfully");
                            return Ok(());
                        }
                        Ok(Err(e)) => {
                            error!("Trade handle completed with error: {:?}", e);
                            return Err(e);
                        }
                        Err(e) => {
                            error!("Trade handle panicked: {:?}", e);
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other, 
                                format!("Error 54: Trade handle panicked: {:?}", e)
                            )));
                        }
                    }
                }
            }
        }
    }

    async fn process_filled_order(&self, data: &Value, is_buy: bool) -> Result<(f64, bool), Box<dyn std::error::Error + Send + Sync>> {
        // Логируем входные данные для отладки
        info!("Processing filled order. Data: {:?}, is_buy: {}", data, is_buy);
    
        if is_buy {
            // Для buy операций используем cum_qty как есть
            let cum_qty = data["cum_qty"].as_f64()
                .ok_or("Error 54: Missing cum_qty")?;
            
            info!("Buy operation processed: cum_qty: {}", cum_qty);
            return Ok((cum_qty, true));
        }
    
        // Для sell операций используем cum_cost и вычитаем комиссию
        let cum_cost = data["cum_cost"].as_f64()
            .ok_or("Error 54: Missing cum_cost")?;
    
        // Получаем комиссию, учитывая разные возможные форматы
        let total_fee = match &data["fee_usd_equiv"] {
            // Если fee_usd_equiv это число
            fee if fee.is_number() => {
                let fee_value = fee.as_f64()
                    .ok_or("Error 54: Invalid fee_usd_equiv number format")?;
                debug!("Single fee found: {}", fee_value);
                fee_value
            },
            // Если fee_usd_equiv это массив
            fee if fee.is_array() => {
                let fees = fee.as_array()
                    .ok_or("Error 54: Invalid fee_usd_equiv array format")?;
                
                if fees.is_empty() {
                    warn!("Empty fee array found, using 0 as total fee");
                    0.0
                } else {
                    let sum: f64 = fees.iter()
                        .filter_map(|fee| fee.as_f64())
                        .sum();
                    info!("Multiple fees found, total: {}", sum);
                    sum
                }
            },
            // Если fee_usd_equiv отсутствует или имеет неверный формат
            _ => {
                warn!("No valid fee_usd_equiv found, using 0 as total fee");
                0.0
            }
        };
    
        let final_amount = cum_cost - total_fee;
        
        info!("Sell operation processed: cum_cost: {}, total_fee: {}, final_amount: {}", 
              cum_cost, total_fee, final_amount);
              
        Ok((final_amount, false))
    }

    async fn handle_error_response(&self, error: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // General errors
        if error.starts_with("EGeneral:") {
            if error.contains("Invalid arguments") {
                info!("Error: Invalid request parameters. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Invalid request parameters")));
            } else if error.contains("Permission denied") {
                info!("Error: Permission denied. API key doesn't have required permissions");
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Permission denied")));
            } else if error.contains("Internal error") {
                info!("Error: Internal error occurred. Please contact support. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 54: Internal error")));
            }
        }
        
        // Service errors
        else if error.starts_with("EService:") {
            if error.contains("Unavailable") {
                info!("Error: Service temporarily unavailable");
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Service unavailable")));
            } else if error.contains("Deadline elapsed") {
                info!("Error: Request timed out");
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 54: Request timeout")));
            } else if error.contains("cancel_only") || error.contains("post_only") {
                info!("Error: Market currently restricted. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Market restricted")));
            }
        }

        // API authentication errors
        else if error.starts_with("EAPI:") {
            info!("Error: Authentication failed. Details: {}", error);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Authentication failed")));
        }

        // Order-related errors
        else if error.starts_with("EOrder:") {
            if error.contains("Insufficient funds") || error.contains("Margin allowance exceeded") {
                info!("Error: Insufficient funds or margin. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Insufficient funds")));
            } else if error.contains("Rate limit") || error.contains("Orders limit exceeded") {
                info!("Error: Rate limit exceeded. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Rate limit exceeded")));
            } else if error.contains("minimum not met") || error.contains("Tick size") {
                info!("Error: Order parameters invalid. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Invalid order parameters")));
            } else if error.contains("Reduce only") {
                info!("Error: Reduce only condition not met. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Reduce only error")));
            }
        }

        // Account and Authentication errors
        else if error.starts_with("EAccount:") || error.starts_with("EAuth:") {
            if error.contains("Rate limit") || error.contains("Too many requests") {
                info!("Error: Too many requests. Please slow down. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 50: Too many requests")));
            } else {
                info!("Error: Account or authentication issue. Details: {}", error);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Account error")));
            }
        }

        // Trade and Funding errors
        else if error.starts_with("ETrade:") || error.starts_with("EFunding:") {
            info!("Error: Trade or funding error. Details: {}", error);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Error 52: Trade or funding error")));
        }

        // Default error handler
        else {
            info!("Unexpected error: {}.", error);
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Error 52: Unexpected error: {}", error))));
        }

        Ok(())
    }

    async fn execute_trade_cycle(&mut self) -> Result<(BotAction, bool), Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();
    
        //info!("Starting new trade cycle");

        let mut check_and_switch_counter = 0;
    
        // Обновляем валидность цепочек перед выбором
        //self.profitable_chain_store.update_chain_validity(self.config.basic_threshold);

        match self.get_most_profitable_chain().await {
            Ok(chain) => {
                info!("Successfully retrieved a profitable chain");
                
                let chain_key = chain.chain.clone();
                if let Some(mut chain_info) = self.profitable_chain_store.chains.get_mut(&chain_key) {
                    if chain_info.count < 2.0 {
                        // Существующий код для случая count < 2.0
                        chain_info.count += 1.0;
                        self.current_chain = Some(chain.clone());
                        
                        info!("Selected chain: {:?}", chain_key);
                        info!("Expected profit: {:.2}%", chain.profit_percentage * 100.0);
                        info!("Chain execution count: {}", chain_info.count);
                        
                        info!("Adding CycleStart action");
                        self.add_action(BotAction::CycleStart).await;
                        info!("Adding ChainSelected action");
                        self.add_action(BotAction::ChainSelected { 
                            chain: chain_key.clone(), 
                            expected_profit: chain.profit_percentage 
                        }).await;
                    } else {                        
                        info!("Цепочка уже выполнена дважды, поиск новой цепочки");
                        chain_info.count = 0.0;                        
                        
                        // Освобождаем мьютекс перед сканированием всех цепочек
                        drop(chain_info);
                        
                        // Создаем копию всех цепочек для безопасной работы
                        let chains_snapshot: Vec<(Vec<String>, ChainExecutionInfo)> = self.profitable_chain_store.chains
                            .iter()
                            .map(|entry| (entry.key().clone(), entry.value().clone()))
                            .collect();
                
                        // Фильтруем цепочки без блокировки DashMap
                        let filtered_chains: Vec<_> = chains_snapshot.into_iter()
                            .filter(|(key, info)| {
                                let meets_criteria = key != &chain_key && 
                                                   info.is_ready && 
                                                   info.count < 2.0 &&
                                                   info.profit_percentage >= self.config.profit_threshold;
                                
                                info!("Проверка цепочки для фильтрации {:?}: ready={}, count={}, profit={:.4}%, meets_criteria={}", 
                                    key, info.is_ready, info.count, info.profit_percentage * 100.0, meets_criteria);
                                
                                meets_criteria
                            })
                            .collect();
                
                        info!("Найдено {} подходящих цепочек", filtered_chains.len());
                
                        if filtered_chains.is_empty() {
                            info!("Нет доступных альтернативных цепочек");
                            return Err("No profitable chains found".into());
                        }
                
                        // Находим самую прибыльную цепочку
                        let best_chain = filtered_chains.into_iter()
                            .max_by(|(_, a), (_, b)| {
                                a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap()
                            })
                            .unwrap();
                
                        info!(
                            "Выбрана новая цепочка: {:?} с прибыльностью {:.4}%", 
                            best_chain.0, 
                            best_chain.1.profit_percentage * 100.0
                        );
                
                        // Теперь безопасно обновляем выбранную цепочку
                        if let Some(mut new_chain_entry) = self.profitable_chain_store.chains.get_mut(&best_chain.0) {
                            new_chain_entry.count += 1.0;
                            if let Some(chain_result) = &new_chain_entry.chain_result {
                                self.current_chain = Some(chain_result.clone());
                                
                                self.add_action(BotAction::ChainSelected { 
                                    chain: best_chain.0.clone(), 
                                    expected_profit: best_chain.1.profit_percentage 
                                }).await;
                
                                return Ok((BotAction::CycleStart, false));
                            }
                        }
                
                        info!("Не удалось получить доступ к выбранной цепочке");
                        return Err("No profitable chains found".into());
                    }
                } else {
                    info!("Chain not found in profitable_chain_store");
                    return Err("No profitable chains found".into());
                }
            },
            Err(e) if e.to_string() == "Restart cycle" => {
                info!("Перезапуск цикла из-за убыточной операции с начальным токеном USDT");
                return Ok((BotAction::CycleStart, false));
            },
            #[allow(unused_variables)]
            Err(e) => {
                #[cfg(debug_assertions)]
                warn!("Error: No profitable chains found: {:?}", e);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "No profitable chains found")));
            }
        }
    

        let mut chain_result = self.current_chain.as_ref().unwrap().clone();
        let mut index = 0;
    
        while index < chain_result.operations.len() {
            let operation_info = &chain_result.operations[index];
            let operation_start = Instant::now();
            self.current_operation_index = index;
            
            // Пересчитываем прибыльность цепочки с текущего токена
            let updated_chain = self.calculate_chain(&self.current_token.clone(), self.current_balance, &chain_result.chain).await?;
            info!("Updated chain profitability: {:.4}%", updated_chain.profit_percentage);
    
            // Проверяем прибыльность с учетом 15% погрешности
            if updated_chain.profit_percentage * 100.0 < self.config.profit_threshold * 85.0 {                
                info!("Chain profitability dropped below threshold. Current: {:.4}%, Threshold: {:.4}%", 
                    updated_chain.profit_percentage, self.config.profit_threshold);
                
                    if check_and_switch_counter < 2 {
                        match self.check_and_switch_chain(self.current_token.clone(), self.current_balance, updated_chain.profit_percentage).await {
                            Ok(new_chain) => {
                                // Проверяем, отличается ли новая цепочка от текущей
                                if new_chain.chain != chain_result.chain {
                                    if new_chain.profit_percentage * 100.0 > updated_chain.profit_percentage * 110.0 {
                                        info!("Switching to a more profitable chain. New profit: {:.4}%, previous profit: {:.4}%", new_chain.profit_percentage * 100.0, updated_chain.profit_percentage);
        
                                        // Обновляем текущую цепочку
                                        self.current_chain = Some(new_chain.clone());
        
                                        // Обновляем chain_result
                                        chain_result = new_chain.clone();
        
                                        // Добавляем соответствующие действия
                                        info!("Adding ChainSelected action for new chain");
                                        self.add_action(BotAction::ChainSelected { 
                                            chain: new_chain.chain.clone(), 
                                            expected_profit: new_chain.profit_percentage 
                                        }).await;
        
                                        // Сбрасываем индекс, чтобы начать с новой цепочки
                                        index = 0;
        
                                        // Продолжаем выполнение с новой цепочкой
                                        continue;
                                    } else {
                                        info!("Found a new chain, but its profitability ({:.4}%) is not better than current ({:.4}%). Not switching.", new_chain.profit_percentage, updated_chain.profit_percentage);
                                    }
                                } else {
                                    info!("New chain is identical to the current chain. Continuing with the current chain.");
                                }
                            },
                            Err(e) => {
                                info!("Error while checking for alternative chains: {:?}. Continuing with the current chain.", e);
                            }
                        }
                        check_and_switch_counter += 1;
                    } else {
                        info!("Limit of check_and_switch_chain calls reached. Continuing with the current chain.");
                    }
                }
    
            info!("Executing trade cycle operation {}", index + 1);
            info!("  From: {} -> To: {}", operation_info.from_token, operation_info.to_token);
            info!("  Symbol: {}", operation_info.symbol);
            info!("  Operation: {:?}", operation_info.operation);
            
            let (symbol, side, amount, is_quote_order) = self.prepare_order_params(operation_info, &self.current_balance.to_string(), self.current_balance);
    
            info!("Prepared order: Symbol: {}, Side: {}, Amount: {}, Is Quote Order: {}", symbol, side, amount, is_quote_order);
            
            let previous_balance = self.current_balance;
            let previous_token = self.current_token.clone();
    
            info!("  Previous balance: {} {}", previous_balance, operation_info.from_token);
            info!("  Previous token: {} {}", previous_token, operation_info.from_token);
    
            let order_start = Instant::now();
    
            let (new_balance, received_token) = self.place_market_order(
                &symbol, 
                &side, 
                amount, 
                is_quote_order,
                operation_info
            ).await?;

            self.current_balance = new_balance;
            self.current_token = received_token.clone();
    
            info!("Одна операция ордера {:?}", order_start.elapsed());
    
            info!("Operation completed. New balance: {} {}", self.current_balance, received_token);
            
            let action = BotAction::OperationExecuted {
                from_token: operation_info.from_token.clone(),
                to_token: operation_info.to_token.clone(),
                symbol: symbol.clone(),
                operation: format!("{:?}", operation_info.operation),
                amount,
                previous_balance,
                new_balance: self.current_balance,
            };
            
            self.intermediate_results.push(action.clone());
            self.add_action(action).await;
    
            info!("Одна операция сделки {:?}", operation_start.elapsed());
    
            if received_token == self.config.start_end_token && index > 0 {
                info!("Цикл внутри execute_trade_cycle завершен за {:?}", cycle_start.elapsed());
                break;
            }
    
            index += 1;
        }
    
        let is_completed = self.current_operation_index >= chain_result.operations.len() - 1 || 
            self.current_balance.to_string() == self.config.start_end_token;
    
        if is_completed {
            info!("Chain execution completed");
            self.current_operation_index = 0;
            let final_balance = self.current_balance;
            let initial_balance = self.config.wallet_balance;
            let profit_percentage = (final_balance / initial_balance - 1.0) * 100.0;
            
            let completed_action = BotAction::CycleCompleted {
                initial_balance,
                final_balance,
                profit_percentage,
                intermediate_results: self.intermediate_results.clone(),
            };

            self.add_action(completed_action.clone()).await;

            info!("Trade cycle completed.");
            info!("Initial balance: {} {}", initial_balance, self.config.start_end_token);
            info!("Final balance: {} {}", final_balance, self.config.start_end_token);
            info!("Profit: {:.2}%", profit_percentage);
            info!("Executed operations:");
            for (i, op) in chain_result.operations.iter().enumerate() {
                info!("  {}. {} -> {}: {}", i+1, op.from_token, op.to_token, op.symbol);
            }

            self.intermediate_results.clear();
            self.current_chain = None;
            self.check_clean_struct(true);

            self.current_balance = self.config.wallet_balance;
            self.current_token = self.config.start_end_token.to_string();

            return Ok((completed_action, true));
        }
        
        Ok((BotAction::CycleStart, false))
    }

    async fn calculate_chain(&mut self, start_token: &str, start_balance: f64, full_chain: &[String]) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();
    
        info!("Начало расчета прибыльности цепочки");
        info!("Начальный токен: {}", start_token);
        info!("Начальный баланс: {}", start_balance);
        info!("Полная цепочка: {:?}", full_chain);
    
        // Находим индекс start_token в полной цепочке
        let start_index = full_chain.iter().position(|token| token == start_token)
            .ok_or_else(|| Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Начальный токен не найден в цепочке")))?;
    
        info!("Индекс начального токена в цепочке: {}", start_index);

        // роверяем, не является ли текущий токен предпоследним в цепочке,
        // а следующий за ним - конечным токеном USDT
        if start_index == full_chain.len() - 2 && full_chain.last() == Some(&"USDT".to_string()) {
            info!("Обнаружена конечная токен пара - пропускаем операцию");
            return Ok(ChainResult {
                chain: full_chain.to_vec(),
                profit_percentage: self.config.profit_threshold,
                final_amount: start_balance,
                duration: cycle_start.elapsed(),
                operation_time: Duration::new(0, 0),
                operations: vec![]
            });
        }
    
        // Обрезаем цепочку, начиная с найденного индекса
        let remaining_chain = &full_chain[start_index..];
    
        info!("Расчет прибыльности цепочки от {} с балансом {}", start_token, start_balance);
        info!("Оставшаяся цепочка: {:?}", remaining_chain);
    
        let calc_start = Instant::now();
        let mut result = self.math_graph.calculate_chain_profit(remaining_chain.to_vec(), start_balance).await?;
        info!("Время расчета прибыльности: {:?}", calc_start.elapsed());
    
        info!("Результат расчета прибыльности:");
        info!("  Конечная сумма: {}", result.final_amount);
        info!("  Длительность: {:?}", result.duration);
        info!("  Время операции: {:?}", result.operation_time);

        // Добавляем логирование данных о цепочке
        #[cfg(debug_assertions)]
        for (i, operation) in result.operations.iter().enumerate() {
            info!("Операция {}: {} -> {}", i + 1, operation.from_token, operation.to_token);
            info!("  Символ: {}", operation.symbol);
            info!("  Операция: {:?}", operation.operation);
            for (j, entry) in operation.entries.iter().enumerate().take(5) {
                info!("    Уровень {}: Цена = {:.8}, Объем = {:.8}", j + 1, entry.price, entry.volume);
            }
        }
    
        // Проверка на недостаточную ликвидность и повторный расчет
        if result.final_amount == 0.0 {
            info!("Обнаружена нулевая конечная сумма. Выполняем повторный расчет.");
            result = self.math_graph.calculate_chain_profit(remaining_chain.to_vec(), start_balance).await?;
            
            tokio::time::sleep(Duration::from_millis(350)).await;

            if result.final_amount == 0.0 {
                info!("ВНИМАНИЕ: Недостаточно ликвидности для выполнения цепочки");
                result.profit_percentage = -100.0;
            }
        }
    
        // Корректируем результат с учетом начального баланса конфигурации
        let adjusted_profit_percentage = result.final_amount / self.config.wallet_balance - 1.0;

        // Добавляем проверку для начального токена USDT и убыточной прибыльности
        if start_index == 0 && start_token == "USDT" {
            if adjusted_profit_percentage * 100.0 < self.config.profit_threshold * 100.0 {
                info!("Обнаружен начальный токен USDT с убыточной прибыльностью");
                // Помечаем цепочку как выполненную дважды
                let chain_key = full_chain.to_vec();
                let chain_info = self.chain_execution_info
                    .entry(chain_key.clone())
                    .or_insert(ChainExecutionInfo {
                        chain: chain_key.clone(),
                        profit_percentage: adjusted_profit_percentage * 100.0,
                        count: 0.0,
                        timestamp: Instant::now(),
                        is_ready: false,
                        chain_result: None,
                    });
                chain_info.count = 2.0;

                // Добавляем цепочку в count 2.0 чтобы сразу запустить таймер
                // и регулировать случай изменения этой цепочки через время
                self.check_clean_struct(false);

                return Err("Restart cycle".into());
            }
        }
    
        info!("Корректировка результата:");
        info!("  Начальный баланс конфигурации: {}", self.config.wallet_balance);
        info!("  Скорректированный процент прибыли: {:.4}%", adjusted_profit_percentage * 100.0);
    
        let adjusted_result = ChainResult {
            chain: remaining_chain.to_vec(),
            profit_percentage: adjusted_profit_percentage * 100.0,
            final_amount: result.final_amount,
            duration: result.duration,
            operation_time: result.operation_time,
            operations: result.operations,
        };
        
        info!("Вычисление calculate_chain завершено за: {:?}", cycle_start.elapsed());
    
        Ok(adjusted_result)
    }

    async fn check_and_switch_chain(&self, current_token: String, current_balance: f64, updated_profit_percentage: f64) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        let cycle_start = Instant::now();

        info!("Starting check_and_switch_chain for token: {}", current_token);
        
        let alternative_chains = self.math_graph.find_chains_with_token(&current_token).await;
        
        info!("Found {} alternative chains", alternative_chains.len());
    
        let chunk_size = 1000;
        let mut all_chain_results: Vec<ChainResult> = Vec::new();

        info!("Initial data:");
        info!("Current chain profitability: {:.4}%", updated_profit_percentage);
    
        for (_chunk_index, chunk) in alternative_chains.chunks(chunk_size).enumerate() {
            info!("Количество цепочек в чанке: {}", chunk.len());
        
            let results = futures::stream::iter(chunk.to_vec())
                .map(|chain| {
                    let math_graph = self.math_graph.clone();
                    let start_token_balance = current_balance; // self.current_balance;
                    async move {
                        let result = math_graph.calculate_chain_profit(chain.clone(), start_token_balance).await;
                        result
                    }
                })
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await;
        
            for result in results {
                match result {
                    Ok(chain_result) => {
                        // Расчет скорректированного profit_percentage
                        let adjusted_profit_percentage = chain_result.final_amount / self.config.wallet_balance - 1.0;
        
                        // Создание нового ChainResult с скорректированным profit_percentage
                        let adjusted_chain_result = ChainResult {
                            profit_percentage: adjusted_profit_percentage,
                            ..chain_result.clone()
                        };
        
                        // Добавление скорректированного результата в all_chain_results
                        all_chain_results.push(adjusted_chain_result.clone());
                    },
                    Err(e) => {
                        info!("Ошибка при расчете прибыли для цепочки: {:?}", e);
                    }
                }
            }
        }

        info!("Вычисление check_and_switch_chain завершено за: {:?}", cycle_start.elapsed());

        let mut profit_percentages: Vec<f64> = all_chain_results.iter().map(|chain| chain.profit_percentage).collect();
        profit_percentages.sort_by(|a, b| b.partial_cmp(a).unwrap());

        // Удаляем цепочки с некорректной прибыльностью
        all_chain_results.retain(|chain| chain.profit_percentage.is_finite());
        
        if all_chain_results.is_empty() {
            info!("No valid chain results after filtering. Exiting check_and_switch_chain.");
            return Err("No alternative chains found".into());
        }

        // Сортируем цепочки по прибыльности (по убыванию)
        let mut chains_by_profit = all_chain_results.clone();
        chains_by_profit.sort_by(|a, b| b.profit_percentage.partial_cmp(&a.profit_percentage).unwrap());

        // Сортируем цепочки по длине (по возрастанию), затем по прибыльности (по убыванию)
        let mut chains_by_length = all_chain_results.clone();
        chains_by_length.sort_by(|a, b| {
            let len_cmp = a.operations.len().cmp(&b.operations.len());
            if len_cmp == std::cmp::Ordering::Equal {
                b.profit_percentage.partial_cmp(&a.profit_percentage).unwrap()
            } else {
                len_cmp
            }
        });

        let top_profitable_chain = chains_by_profit[0].clone();
        let top_shortest_chain = chains_by_length[0].clone();

        info!("Top profitable chain selected: {:?}", top_profitable_chain.chain);
        info!("Top shortest chain selected: {:?}", top_shortest_chain.chain);

        // Вычисляем, все ли цепочки убыточные
        let all_chains_unprofitable = all_chain_results.iter().all(|chain| chain.profit_percentage < 0.0);

        let selected_chain = if all_chains_unprofitable {
            info!("Все цепочки убыточные. Выполняем проверку относительно порогов -0.1% и -1.0%.");

            if top_shortest_chain.profit_percentage >= -0.1 {
                info!(
                    "Самая короткая цепочка прибыльна или близка к нулю ({}%). Выбираем самую короткую цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_shortest_chain.clone()
            } else if top_shortest_chain.profit_percentage > -1.0 {
                info!(
                    "Самая короткая цепочка убыточна, но превышает -1.0% ({}%). Выбираем самую прибыльную цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_profitable_chain.clone()
            } else {
                info!(
                    "Самая короткая цепочка менее -1.0% ({}%). Выбираем самую короткую цепочку.",
                    top_shortest_chain.profit_percentage
                );
                top_shortest_chain.clone()
            }
        } else {
            // Определяем количество прибыльных цепочек
            let profitable_chains_count = all_chain_results.iter().filter(|chain| chain.profit_percentage > 0.0).count();

            if profitable_chains_count == 1 {
                // Выбираем единственную прибыльную цепочку
                let profitable_chain = all_chain_results.iter().find(|chain| chain.profit_percentage > 0.0).unwrap().clone();
                info!(
                    "Найдена одна прибыльная цепочка: {:?}. Выбираем её.",
                    profitable_chain.chain
                );
                profitable_chain
            } else {
                // Есть две или более прибыльных цепочек
                let shortest_profitable_chain = all_chain_results.iter()
                    .filter(|chain| chain.profit_percentage > 0.0)
                    .min_by_key(|chain| chain.operations.len())
                    .unwrap()
                    .clone();

                let most_profitable_chain = all_chain_results.iter()
                    .filter(|chain| chain.profit_percentage > 0.0)
                    .max_by(|a, b| a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap())
                    .unwrap()
                    .clone();

                info!(
                    "Найдены две или более прибыльных цепочек:\n\
                    - Самая короткая прибыльная цепочка: {:?} с прибылью {:.4}%\n\
                    - Самая прибыльная цепочка: {:?} с прибылью {:.4}%",
                    shortest_profitable_chain.chain,
                    shortest_profitable_chain.profit_percentage,
                    most_profitable_chain.chain,
                    most_profitable_chain.profit_percentage
                );

                if shortest_profitable_chain.profit_percentage > 1.0 {
                    if most_profitable_chain.profit_percentage > 1.5 {
                        info!(
                            "Самая короткая цепочка прибыльна более чем на 1.0% ({:.2}%), \
                            а самая прибыльная цепочка превышает 1.5% ({:.2}%). \
                            Выбираем самую прибыльную цепочку.",
                            shortest_profitable_chain.profit_percentage,
                            most_profitable_chain.profit_percentage
                        );
                        most_profitable_chain
                    } else {
                        info!(
                            "Самая короткая цепочка прибыльна более чем на 1.0% ({:.2}%), \
                            а самая прибыльная цепочка НЕ превышает 1.5% ({:.2}%). \
                            Выбираем самую короткую цепочку.",
                            shortest_profitable_chain.profit_percentage,
                            most_profitable_chain.profit_percentage
                        );
                        shortest_profitable_chain
                    }
                } else if shortest_profitable_chain.profit_percentage <= 1.0 && most_profitable_chain.profit_percentage > 1.5 {
                    info!(
                        "Самая короткая цепочка НЕ превышает 1.0% ({:.2}%), \
                        а самая прибыльная цепочка превышает 1.5% ({:.2}%). \
                        Выбираем самую прибыльную цепочку.",
                        shortest_profitable_chain.profit_percentage,
                        most_profitable_chain.profit_percentage
                    );
                    most_profitable_chain
                } else {
                    info!(
                        "В остальных случаях выбираем самую короткую цепочку: {:?} с прибылью {:.4}%",
                        shortest_profitable_chain.chain,
                        shortest_profitable_chain.profit_percentage
                    );
                    shortest_profitable_chain
                }
            }
        };

        info!("Используем цепочку: {:?}", selected_chain.chain);
        
        Ok(selected_chain)
    }

    async fn get_most_profitable_chain(&mut self) -> Result<ChainResult, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(debug_assertions)]
        info!("Начало поиска наиболее прибыльной цепочки");
        let profitable_chains = self.math_graph.profitable_chains.clone();
        
        #[cfg(debug_assertions)]
        info!("Обновление profitable_chain_store с {} цепочками", profitable_chains.len());
        for chain_result in profitable_chains.iter() {
            self.profitable_chain_store.add_or_update_chain(
                chain_result.clone(),
                self.config.basic_threshold,
            );
        }

        // Вывод всех цепочек в profitable_chain_store
        #[cfg(debug_assertions)]
        info!("Текущие цепочки в profitable_chain_store:");
        #[cfg(debug_assertions)]
        for entry in self.profitable_chain_store.chains.iter() {
            info!("Цепочка: {:?}, Прибыль: {:.4}%", entry.key(), entry.value().profit_percentage * 100.0);
        }  
        
        #[cfg(debug_assertions)]
        info!("Обновление валидности цепочек");
        self.profitable_chain_store.update_chain_validity(self.config.basic_threshold);
        
        #[cfg(debug_assertions)]
        info!("Получение валидных цепочек");
        let valid_chains = self.profitable_chain_store.get_valid_chains(
            self.config.basic_threshold,
            self.config.profit_threshold,
        );
        
        if valid_chains.is_empty() {
            #[cfg(debug_assertions)]
            info!("Не найдено валидных прибыльных цепочек");
            return Err("No profitable chains found".into());
        }
        
        #[cfg(debug_assertions)]
        info!("Найдено {} валидных цепочек", valid_chains.len());
        let best_chain_info = valid_chains
            .into_iter()
            .max_by(|a, b| a.profit_percentage.partial_cmp(&b.profit_percentage).unwrap())
            .unwrap();

        #[cfg(debug_assertions)]
        info!("Best chain info: {:?}", best_chain_info.chain);
        
        if let Some(chain_result) = &best_chain_info.chain_result {
            info!("Выбрана цепочка: {:?}", chain_result.chain);
            info!("Ожидаемая прибыль: {:.2}%", chain_result.profit_percentage * 100.0);
            info!("Количество выполнений цепочки: {}", best_chain_info.count);
            
            Ok(chain_result.clone())
        } else {
            // Пересчитываем ChainResult, если он не сохранен
            let result = self.math_graph.calculate_chain_profit(best_chain_info.chain.clone(), self.config.wallet_balance).await?;
            info!("ChainResult пересчитан для цепочки: {:?}", result.chain);
            Ok(result)
        }
    }      

    fn prepare_order_params(&self, operation_info: &OperationInfo, current_token: &str, balance: f64) -> (String, String, f64, bool) {
        let symbol = operation_info.symbol.clone();
        let side = match operation_info.operation {
            Operation::Multiply => "SELL",
            Operation::Divide => "BUY",
        };
        let is_quote_order = current_token == operation_info.from_token;
        let amount = balance;

        (symbol, side.to_string(), amount, is_quote_order)
    }

    async fn format_trading_symbol(&self, symbol: &str) -> String {
        // Если символ уже содержит разделитель, возвращаем как есть
        if symbol.contains('/') {
            info!("Символ уже содержит разделитель: {}", symbol);
            return symbol.to_string();
        }
    
        info!("Получен символ для форматирования: {}", symbol);
    
        let store = self.step_size_store.read().await;
    
        // Функция для нормализации символа (удаление разделителя)
        let normalize_symbol = |s: &str| s.replace("/", "");
    
        // Ищем соответствие в токенах, сравнивая нормализованные версии
        for token in &store.tokens {
            let normalized_stored = normalize_symbol(&token.symbol);
            if normalized_stored == symbol {
                info!("Найдено точное соответствие: {} -> {}", symbol, token.format);
                return token.format.clone();
            }
        }
    
        // Если точное соответствие не найдено, проверяем все возможные комбинации с quote_currencies
        for &quote_currency in &self.config.quote_currencies {
            // Проверяем комбинации в обоих направлениях
            let combinations = vec![
                (symbol.starts_with(quote_currency), quote_currency, &symbol[quote_currency.len()..]),
                (symbol.ends_with(quote_currency), &symbol[..symbol.len() - quote_currency.len()], quote_currency)
            ];
    
            for (matches, first, second) in combinations {
                if !matches {
                    continue;
                }
    
                // Формируем возможные варианты форматирования
                let format1 = format!("{}/{}", first, second);
                let format2 = format!("{}{}", first, second);
    
                // Проверяем существование таких форматов в store.tokens
                for token in &store.tokens {
                    let normalized_token = normalize_symbol(&token.format);
                    if normalized_token == format2 || normalized_token == normalize_symbol(&format1) {
                        info!("Найдено соответствие через quote_currency: {} -> {}", symbol, token.format);
                        return token.format.clone();
                    }
                }
    
                // Если нашли совпадение по паттерну, но нет в tokens, возвращаем стандартный формат
                info!("Сформирован стандартный формат: {} -> {}", symbol, format1);
                return format1;
            }
        }
    
        // Последняя попытка - поиск частичных совпадений
        for token in &store.tokens {
            let normalized_stored = normalize_symbol(&token.format);
            if normalized_stored.contains(symbol) || symbol.contains(&normalized_stored) {
                info!("Найдено частичное совпадение: {} -> {}", symbol, token.format);
                return token.format.clone();
            }
        }
    
        info!("Не удалось найти соответствие для символа: {}. Возвращаем исходный формат.", symbol);
        symbol.to_string()
    }

    async fn place_market_order(&mut self, symbol: &str, side: &str, amount: f64, is_quote_order: bool, operation_info: &OperationInfo) -> Result<(f64, String), Box<dyn std::error::Error + Send + Sync>> {

        // Получаем актуальный websocket токен
        let token = self.websocket_token.read().await
            .as_ref()
            .ok_or("No WebSocket token available")?
            .token
            .clone();

        // Получаем base_increment и quote_increment для символа
        let base_increment = self.step_size_store.read().await.symbols.get(symbol)
            .map(|product_data| product_data.base_increment.clone())
            .unwrap_or_else(|| "0.00000001".to_string());

        let quote_increment = self.step_size_store.read().await.symbols.get(symbol)
            .map(|product_data| product_data.quote_increment.clone())
            .unwrap_or_else(|| "0.00000001".to_string());

        info!("Base increment for symbol {}: {}", symbol, base_increment);
        info!("Quote increment for symbol {}: {}", symbol, quote_increment);

        // Определяем формат для баланса на основе increment
        let base_precision = if base_increment == "1" {
            0
        } else {
            base_increment.chars()
                .skip_while(|&c| c != '.')
                .skip(1)
                .take_while(|&c| c == '0')
                .count() + 1
        };

        let quote_precision = if quote_increment == "1" {
            0
        } else {
            quote_increment.chars()
                .skip_while(|&c| c != '.')
                .skip(1)
                .take_while(|&c| c == '0')
                .count() + 1
        };

        // Форматируем количество в зависимости от типа ордера
        let formatted_amount = if is_quote_order {
            Self::format_float_truncate(amount, quote_precision)
        } else {
            Self::format_float_truncate(amount, base_precision)
        };

        let is_buy = side.to_lowercase() == "buy";

        let format_symbol = Instant::now();
        let formatted_symbol = self.format_trading_symbol(symbol).await;
        info!("Форматирование символа завершено за: {:?}", format_symbol.elapsed());

        let order_message = if side.to_lowercase() == "buy" {
            json!({
                "method": "add_order",
                "params": {
                    "order_type": "market",
                    "side": side.to_lowercase(),
                    "cash_order_qty": formatted_amount,
                    "symbol": formatted_symbol,
                    "token": token,
                }
            })
        } else {
            json!({
                "method": "add_order",
                "params": {
                    "order_type": "market",
                    "side": side.to_lowercase(),
                    "order_qty": formatted_amount,
                    "symbol": formatted_symbol,
                    "token": token,
                }
            })
        };
        
        let operation_start = Instant::now();
        let result = self.order_send(order_message.to_string(), operation_info.to_token.clone(), is_buy).await?;
        info!("Отправка и получение данных сделки {:?}", operation_start.elapsed());
    
        Ok(result)
    }

    async fn order_send(&mut self, order_message: String, expected_token: String, is_buy: bool) -> Result<(f64, String), Box<dyn std::error::Error + Send + Sync>> {
        info!("Sending order message: {}", order_message);
        match self.send_message(order_message, is_buy).await {    
            Ok((new_balance, _)) => {
                info!("Order response: New balance: {}, Expected token: {}", new_balance, expected_token);
                Ok((new_balance, expected_token))
            },
            Err(e) => Err(e),
        }
    }
    
    async fn add_action(&self, action: BotAction) {
        let _ = self.bot_action_sender.send(action.clone());
        let mut actions = self.actions.write().await;
        actions.push(action);
    }

    // Новый блок для kraken документации 

    async fn authenticate(&self, endpoint: &str, params: &[(&str, String)]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        info!("Начало аутентификации для эндпоинта: {}", endpoint);
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let mut auth_params = vec![("nonce", nonce.to_string())];
        auth_params.extend_from_slice(params);
        
        let post_data = auth_params.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");

        // Шаг 1: Хеширование POST данных с помощью SHA256
        let sha256_digest = Sha256::new().chain_update(format!("{}{}", nonce, post_data)).finalize();
    
        // Шаг 2: Создание HMAC-SHA512 подписи с использованием относительного URI
        let api_secret_decoded = BASE64.decode(&self.config.api_secret)?;
        let mut mac = Hmac::<Sha512>::new_from_slice(&api_secret_decoded)
            .map_err(|_| "Invalid key length")?;
        mac.update(endpoint.as_bytes());
        mac.update(&sha256_digest);
        let signature = BASE64.encode(mac.finalize().into_bytes());
        
        // Шаг 3: Отправка запроса с использованием относительного URI
        let client = Client::new();
        let response = client.post(format!("https://api.kraken.com{}", endpoint))
            .header("API-Key", &self.config.api_key)
            .header("API-Sign", signature)
            .form(&auth_params)
            .send()
            .await?
            .text()
            .await?;
    
        info!("Ответ на запрос аутентификации получен");
        
        Ok(response)
    }

    async fn get_websocket_token(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Запрос WebSocket токена");
        let endpoint = "/0/private/GetWebSocketsToken";
        let response = self.authenticate(endpoint, &[]).await?;
        let json: Value = serde_json::from_str(&response)?;
    
        if let Some(error) = json["error"].as_array().and_then(|e| e.first()).and_then(|e| e.as_str()) {
            error!("Ошибка API Kraken: {}", error);
            return Err(format!("Kraken API error: {}", error).into());
        }
        
        let token = json["result"]["token"].as_str().ok_or("No token in response")?;
        let expires = json["result"]["expires"].as_u64().ok_or("No expiry in response")?;
        
        let mut ws_token = self.websocket_token.write().await;
        *ws_token = Some(WebSocketToken::new(token.to_string(), expires));
        
        info!("WebSocket токен успешно получен: {}", token);

        Ok(())
    }

    async fn subscribe_to_executions(&self, write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Подписка на канал executions");
        let token = self.websocket_token.read().await
            .as_ref()
            .ok_or("No WebSocket token")?
            .token.clone();
    
        let subscribe_message = json!({
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "token": token,
                "snap_orders": false,
                "order_status": false,
                "snap_trades": false
            }
        });
    
        write.send(Message::Text(subscribe_message.to_string())).await?;
        debug!("Отправлено сообщение подписки на executions: {}", subscribe_message);
        Ok(())
    }

    async fn kraken_prepare(&mut self, write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Начало подготовки Kraken");
        self.get_websocket_token().await?;
        self.subscribe_to_executions(write).await?;
        info!("Подготовка Kraken завершена");
        Ok(())
    }

    // Новый блок для kraken документации 
    
}