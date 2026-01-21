use std::sync::Arc;
use anyhow::Result;

use crate::application::traits::order_trait::TOrderRepository;

pub struct OrderService {
    repo: Arc<dyn TOrderRepository>,
}

impl OrderService {
    pub fn new(repo: Arc<dyn TOrderRepository>) -> Self {
        Self { repo }
    }

    pub async fn get_views(&self) -> Result<Vec<serde_json::Value>> {
        let rows = self.repo.get_views().await?;
        let mut result = Vec::new();

        for row in rows {
            let trader_id = row.get::<_, String>("trader_id");
            let json_value = serde_json::json!({"trader_id": trader_id});
            result.push(json_value);
        }

        Ok(result)
    }

    pub async fn add(&self) -> Result<()> {
        self.repo.add().await
    }
}
