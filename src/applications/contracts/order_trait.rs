use async_trait::async_trait;
use anyhow::Result;

use crate::applications::services::order::domain::order_entity::{
    BrokerTrxEntity, BrokerTrxState, ClientTrxState, OrderEntity, ProcessingConfig
};

#[async_trait]
pub trait TOrderRepository: Send + Sync {
    async fn get_views(&self) -> Result<Vec<tokio_postgres::Row>>;
    async fn match_orders(&self, config: &ProcessingConfig, order: &OrderEntity, btx: &mut BrokerTrxState, ctrx: &mut ClientTrxState,  order_ids: &[i32]) -> Result<()>;
    async fn update_order_status_batch(&self, tx: &mut tokio_postgres::Transaction<'_>, order_ids: &[i32]) -> Result<()>;
    async fn update_order_status(&self, order_id: i32) -> Result<()>;
    async fn update_broker_trx(&self,tx: &mut tokio_postgres::Transaction<'_>,broker_trx_nid: i32) -> Result<()>;
    async fn create_or_update_broker_trx(&self,tx: &mut tokio_postgres::Transaction<'_> , broker_trx: &BrokerTrxEntity, config: &ProcessingConfig) -> Result<i32>;
    fn should_search_broker_trx(&self, order: &OrderEntity, state: &BrokerTrxState) -> bool;
    async fn match_orders_async(&self) -> Result<String>;
    async fn get_trade_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error>;
    async fn get_order_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error>;
}
