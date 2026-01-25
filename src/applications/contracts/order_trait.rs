use async_trait::async_trait;
use anyhow::Result;

use crate::{applications::services::order::domain::order_entity::{
    BrokerTrxEntity, BrokerTrxState, ClientTrxState, MatchResult, OrderDone, OrderEntity, PartialMatch, ProcessingConfig
}, infrastructure::database::snapshot::SnapshotDb};

#[async_trait]
pub trait TOrderRepository: Send + Sync {
    async fn get_views(&self) -> Result<Vec<tokio_postgres::Row>>;
    async fn match_orders(&self, config: &ProcessingConfig, order: &OrderEntity, btx: &mut BrokerTrxState, ctrx: &mut ClientTrxState,  order_ids: &[i32]) -> Result<()>;
    async fn update_order_status_batch(&self, tx: &mut tokio_postgres::Transaction<'_>, order_ids: &[i32]) -> Result<()>;
    async fn update_order_status(&self, order_id: i32) -> Result<()>;
    async fn update_broker_trx(&self,tx: &mut tokio_postgres::Transaction<'_>,broker_trx_nid: i32) -> Result<()>;
    async fn create_or_update_broker_trx(&self,tx: &mut tokio_postgres::Transaction<'_> , broker_trx: &BrokerTrxEntity, config: &ProcessingConfig) -> Result<i32>;
    fn should_search_broker_trx(&self, order: &OrderEntity, state: &BrokerTrxState) -> bool;
    async fn match_orders_async(&self, result: MatchResult) -> Result<()>;
    async fn get_trade_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error>;
    async fn get_order_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error>;
    fn create_btx_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error>;
    fn create_ctx_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error>;
    fn create_alloc_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error>;
    async fn update_trade_match(&self, tx: &mut tokio_postgres::Transaction<'_>,  trade_nids: &[i32], order_nids: &[i32]) -> Result<()>;
    async fn update_trade_partial(&self, tx: &mut tokio_postgres::Transaction<'_> , rows: &[PartialMatch]) -> Result<()>;
    async fn insert_trade_remainder(&self, tx: &mut tokio_postgres::Transaction<'_> , rows: &[PartialMatch]) -> Result<()>;
    fn update_ctx_temp(&self, snapshot: &SnapshotDb) -> Result<String>;
    async fn update_order_match(&self, tx: &mut tokio_postgres::Transaction<'_>, rows: &[OrderDone]) -> Result<()>;
    fn get_broker_trx_done(&self, snapshot: &SnapshotDb) -> Result<Vec<serde_json::Value>, anyhow::Error>;
}
