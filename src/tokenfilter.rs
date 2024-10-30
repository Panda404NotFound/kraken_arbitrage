use reqwest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use anyhow::{Result, Context};
use hashbrown::HashMap;
use log::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    pub product_id: String,
    pub base_increment: String,
    pub quote_increment: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse {
    pub products: Vec<Value>,
}

#[derive(Debug)]
pub struct ProductData {
    pub base_increment: String,
    pub quote_increment: String,
}

#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub symbol: String,
    pub format: String,
}

#[derive(Debug)]
pub struct IncrementStore {
    pub symbols: HashMap<String, ProductData>,
    pub tokens: Vec<TokenInfo>,
}

impl IncrementStore {
    pub fn new() -> Self {
        IncrementStore {
            symbols: HashMap::new(),
            tokens: Vec::new(),
        }
    }

    /*
    pub fn add_token(&mut self, symbol: String, format: String) {
        self.tokens.push(TokenInfo { symbol, format });
    }
    */
}

// Создаем новую функцию для получения только HashMap с symbols
pub async fn fetch_symbols_data() -> Result<HashMap<String, ProductData>> {
    info!("Начало выполнения fetch_symbols_data");
    let client = reqwest::Client::new();
    let url = "https://api.kraken.com/0/public/AssetPairs";

    info!("Отправка GET-запроса к {}", url);
    let response = client.get(url)
        .send()
        .await
        .context("Failed to send request")?;
    
    let api_response: Value = response.json()
        .await
        .context("Failed to parse JSON response")?;
    
    let result = api_response["result"].as_object()
        .context("Failed to get 'result' object from response")?;

    let mut symbols_map = HashMap::new();

    for (_, pair_data) in result {
        if let (Some(wsname), Some(tick_size)) = (
            pair_data["wsname"].as_str(),
            pair_data["tick_size"].as_str(),
        ) {
            symbols_map.insert(
                wsname.to_string(),
                ProductData {
                    base_increment: tick_size.to_string(),
                    quote_increment: tick_size.to_string(),
                },
            );
        }
    }

    info!("Завершено выполнение fetch_symbols_data. Обработано пар: {}", symbols_map.len());
    Ok(symbols_map)
}

/*
// Оставляем оригинальную функцию для полной инициализации
pub async fn fetch_product_data() -> Result<IncrementStore> {
    let symbols = fetch_symbols_data().await?;
    let mut store = IncrementStore::new();
    store.symbols = symbols;
    
    // Создаем временный вектор символов
    let symbols: Vec<String> = store.symbols.keys().cloned().collect();
    
    // Теперь добавляем токены, используя временный вектор
    for symbol in symbols {
        store.add_token(symbol.clone(), symbol);
    }
    
    Ok(store)
}
*/