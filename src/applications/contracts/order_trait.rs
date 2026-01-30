use std::collections::HashMap;

use async_trait::async_trait;
use anyhow::Result;

use crate::{applications::services::order::domain::order_entity::{
    BrokerTrxEntity, BrokerTrxInsert, BrokerTrxInsertOld, BrokerTrxState, BrokerTrxTmp, ClientTrxInsert, ClientTrxState, ClientTrxTmp, MatchResult, OrderDone, OrderEntity, PartialMatch, ProcessingConfig, SalesPerson, TradeMatched
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
    fn get_broker_trx_done(&self, snapshot: &SnapshotDb) -> Result<Vec<BrokerTrxInsertOld>, anyhow::Error>;
    async fn get_trade_matched(&self, tx: &mut tokio_postgres::Transaction<'_>) -> Result<Vec<TradeMatched>>;
    async fn get_trade_matched_map(&self, tx: &tokio_postgres::Transaction<'_>) -> anyhow::Result<HashMap<i32, TradeMatched>>;
    fn build_insert_btx_rows(&self, tmp: Vec<BrokerTrxTmp>, trade_map: &HashMap<i32, TradeMatched>) -> Vec<BrokerTrxInsert>;
    async fn copy_btx_to_postgres(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[BrokerTrxInsert]) -> anyhow::Result<()>;
    fn build_insert_ctx_rows(&self, tmp: Vec<ClientTrxTmp>, trade_map: &HashMap<i32, TradeMatched>) -> Vec<ClientTrxInsert>;
    async fn copy_ctx_to_postgres(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[ClientTrxInsert]) -> anyhow::Result<()>;
    async fn insert_sales_person(&self, sales: &Vec<SalesPerson>) -> Result<()>;
    async fn insert_btx_fast(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[BrokerTrxInsert]) -> anyhow::Result<()>;
}
