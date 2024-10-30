use std::sync::Arc;
use tokio::time::Instant;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use log::info;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookEntry {
    pub price: f64,
    pub volume: f64,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookUpdate {
    pub symbol: String,
    pub asks_updates: Vec<OrderBookEntry>,
    pub bids_updates: Vec<OrderBookEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDataUpdate {
    pub symbol: String,
    pub order_book_update: OrderBookUpdate,
}

#[derive(Clone)]
pub struct DataEntry {
    pub version: u64,
    pub data: TokenDataUpdate,
}

pub struct LockFreeTokenDataStore {
    pub data: DashMap<String, DataEntry>,
    pub update_queue: SegQueue<(String, DataEntry)>,
}

#[allow(dead_code)]
impl LockFreeTokenDataStore {
    pub fn new() -> Self {
        LockFreeTokenDataStore {
            data: DashMap::new(),
            update_queue: SegQueue::new(),
        }
    }

    pub fn get_token_data(&self, symbol: &str) -> Option<TokenDataUpdate> {
        self.data.get(symbol).map(|entry| entry.data.clone())
    }

    pub fn update_entry(&self, symbol: String, update: TokenDataUpdate) {
        let version = self.get_next_version(&symbol);
        let entry = DataEntry { version, data: update };
        self.update_queue.push((symbol.clone(), entry.clone()));
        self.data.insert(symbol, entry);
    }

    fn get_next_version(&self, symbol: &str) -> u64 {
        self.data.get(symbol)
            .map(|entry| entry.version + 1)
            .unwrap_or(1)
    }

    pub fn remove_token_data(&self, symbol: &str) {
        self.data.remove(symbol);
        let new_queue = SegQueue::new();
        while let Some((s, entry)) = self.update_queue.pop() {
            if s != symbol {
                new_queue.push((s, entry));
            }
        }
        while let Some((s, entry)) = new_queue.pop() {
            self.update_queue.push((s, entry));
        }
    }
    
    pub fn update_order_book(&self, symbol: &str, updates: OrderBookUpdate) {
        if let Some(mut entry) = self.data.get_mut(symbol) {
            let order_book = &mut entry.data.order_book_update;
            
            // Метрики для asks
            let _asks_start = Instant::now();

            let mut _asks_count = 0;
            
            // Обновляем asks
            for ask in updates.asks_updates {
                _asks_count += 1;
                if ask.volume == 0.0 {
                    order_book.asks_updates.retain(|a| a.price != ask.price);
                } else {
                    if let Some(existing_ask) = order_book.asks_updates.iter_mut().find(|a| a.price == ask.price) {
                        existing_ask.volume = ask.volume;
                    } else {
                        order_book.asks_updates.push(ask);
                    }
                }
            }
            order_book.asks_updates.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
            order_book.asks_updates.truncate(10);
            
            let _asks_duration = _asks_start.elapsed();
            
            // Метрики для bids
            let bids_start = Instant::now();
            let mut _bids_count = 0;
            
            // Обновляем bids
            for bid in updates.bids_updates {
                _bids_count += 1;
                if bid.volume == 0.0 {
                    order_book.bids_updates.retain(|b| b.price != bid.price);
                } else {
                    if let Some(existing_bid) = order_book.bids_updates.iter_mut().find(|b| b.price == bid.price) {
                        existing_bid.volume = bid.volume;
                    } else {
                        order_book.bids_updates.push(bid);
                    }
                }
            }
            order_book.bids_updates.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
            order_book.bids_updates.truncate(10);
            
            // Проверка и коррекция пересечения ask и bid
            if let (Some(lowest_ask), Some(highest_bid)) = (order_book.asks_updates.first(), order_book.bids_updates.first()) {
                if highest_bid.price >= lowest_ask.price {
                    let highest_bid_price = highest_bid.price;
                    let lowest_ask_price = lowest_ask.price;
                    // Удаляем пересекающиеся ордера
                    order_book.asks_updates.retain(|ask| ask.price > highest_bid_price);
                    order_book.bids_updates.retain(|bid| bid.price < lowest_ask_price);
                }
            }

            let _bids_duration = bids_start.elapsed();
            
            // Логирование метрик
            #[cfg(debug_assertions)]
            info!("Order book update for {}: Asks (count: {}, time: {:?}), Bids (count: {}, time: {:?})",
            symbol, _asks_count, _asks_duration, _bids_count, _bids_duration);
            
            // Вызов новой функции для вывода стакана
            #[cfg(debug_assertions)]
            self.log_order_book(symbol, order_book);
        }
    }

    #[cfg(debug_assertions)]
    fn log_order_book(&self, symbol: &str, order_book: &OrderBookUpdate) {
        info!("Order book for {}", symbol);
        info!("Asks (top 5):");
        for (i, ask) in order_book.asks_updates.iter().take(5).enumerate() {
            info!("  {}: Price: {}, Volume: {}", i + 1, ask.price, ask.volume);
        }
        info!("Bids (top 5):");
        for (i, bid) in order_book.bids_updates.iter().take(5).enumerate() {
            info!("  {}: Price: {}, Volume: {}", i + 1, bid.price, bid.volume);
        }
    }
}

pub type TokenDataStore = Arc<LockFreeTokenDataStore>;

pub fn update_token_data(store: &TokenDataStore, update: TokenDataUpdate) {
    store.update_entry(update.symbol.clone(), update);
}