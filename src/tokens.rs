// KRAKEN tokens.rs
use crate::config::Config;

use serde::{Deserialize, Serialize};
use reqwest::Client;
use anyhow::Result;
use tokio::time::Duration;
use tokio::sync::mpsc;
use hashbrown::HashSet;
use log::info;

#[derive(Debug, Deserialize)]
struct AssetPairResponse {
    result: std::collections::HashMap<String, AssetPairInfo>,
}

#[derive(Debug, Deserialize)]
struct AssetPairInfo {
    wsname: String,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct TokenLiquidity {
    pub symbol: String,
    pub volume_24h: f64,
}

// Изменено на 60 часов вместо 1 часа, чтобы не менять логику проекта
pub async fn update_top_tokens_hourly(
    count: usize,
    quote_currencies: Vec<&str>,
    subscription_sender: mpsc::Sender<Vec<TokenLiquidity>>,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(60 * 60 * 60));
    let mut current_tokens = get_top_tokens(count, &quote_currencies).await?;
    
    loop {
        interval.tick().await;
        let new_tokens = get_top_tokens(count, &quote_currencies).await?;
        
        let changes = detect_changes(&current_tokens, &new_tokens);
        if !changes.is_empty() {
            info!("Changes detected: {:?}", changes);
            if let Err(e) = subscription_sender.send(new_tokens.clone()).await {
                eprintln!("Failed to send updated tokens: {}", e);
            } else {
                current_tokens = new_tokens;
                info!("Top tokens updated and sent for resubscription");
            }
        } else {
            info!("No changes in top tokens");
        }
    }
}

fn detect_changes(old_tokens: &[TokenLiquidity], new_tokens: &[TokenLiquidity]) -> Vec<String> {
    let old_set: HashSet<_> = old_tokens.iter().map(|t| &t.symbol).collect();
    let new_set: HashSet<_> = new_tokens.iter().map(|t| &t.symbol).collect();
    
    let mut changes = Vec::new();
    
    for symbol in old_set.difference(&new_set) {
        changes.push(format!("Removed: {}", symbol));
    }
    
    for symbol in new_set.difference(&old_set) {
        changes.push(format!("Added: {}", symbol));
    }
    
    changes
}

pub async fn get_top_tokens(count: usize, quote_currencies: &[impl AsRef<str>]) -> Result<Vec<TokenLiquidity>> {
    let client = Client::new();
    let url = "https://api.kraken.com/0/public/AssetPairs";

    let api_response: AssetPairResponse = client.get(url).send().await?.json().await?;

    let mut all_tokens = Vec::new();

    for (_, pair_info) in api_response.result {
        let parts: Vec<&str> = pair_info.wsname.split('/').collect();
        if parts.len() == 2 {
            let base = parts[0].to_uppercase();
            let quote = parts[1].to_uppercase();
            
            let is_valid = quote_currencies.iter().any(|qc| {
                let qc = qc.as_ref().to_uppercase();
                base == qc || quote == qc
            });

            if is_valid {
                all_tokens.push(TokenLiquidity {
                    symbol: pair_info.wsname,
                    volume_24h: 0.0,
                });
            }
        }
    }

    all_tokens.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    let top_tokens: Vec<TokenLiquidity> = all_tokens.into_iter().take(count).collect();

    info!("Общее количество токен-пар: {}", top_tokens.len());

    Ok(top_tokens)
}

pub async fn get_mandatory_tokens(config: &Config) -> Vec<TokenLiquidity> {
    config.mandatory_tokens
        .iter()
        .map(|&symbol| TokenLiquidity {
            symbol: symbol.to_string(),
            volume_24h: 0.0, // Для обязательных токенов объем не важен
        })
        .collect()
}