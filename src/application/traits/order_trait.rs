use async_trait::async_trait;
use anyhow::Result;
use tokio_postgres::Row;

#[async_trait]
pub trait TOrderRepository: Send + Sync {
    async fn get_views(&self) -> Result<Vec<Row>>;
    async fn add(&self) -> Result<()>;
}
