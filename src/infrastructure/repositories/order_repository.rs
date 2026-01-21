use async_trait::async_trait;
use anyhow::Result;
use deadpool_postgres::Pool;
use tokio_postgres::Row;

use crate::application::traits::order_trait::TOrderRepository;

pub struct OrderRepository {
    conn: Pool
}

impl OrderRepository {
    pub fn new(conn: Pool) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl TOrderRepository for OrderRepository {
    async fn get_views(&self) -> Result<Vec<Row>> {
        let conn = self.conn.get().await?;

        let rows = conn
            .query("SELECT trader_id FROM trade LIMIT 10", &[])
            .await?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row);
        }

        Ok(result)
    }

    async fn add(&self) -> Result<()> {
        Ok(())
    }
}
