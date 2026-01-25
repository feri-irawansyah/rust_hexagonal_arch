use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum OrderStatus {
    Open,
    Partial,
    Matched,
    Cancelled,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerTrxState {
    pub search_again: bool,
    pub broker_trx_nid: i32,
    pub prev_trx_n_type: i32,
    pub prev_broker_nid: i32,
    pub prev_buy_sell: String,
    pub prev_stock_nid: i32,
    pub broker_trx_mode: i32,
    pub broker_trx_net: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ClientTrxState {
    pub search_again: bool,
    pub client_trx_nid: i32,
    pub prev_trx_n_type: i32,
    pub prev_client_nid: i32,
    pub prev_day_trade: bool,
    pub prev_online_trading: bool,
    pub prev_ng_report: bool,
    pub prev_stock_nid: i32,
    pub prev_buy_sell: String,
    pub prev_order_commission_percent: Decimal,
    pub client_trx_mode: i32,
    pub client_trx_net: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderEntity {
    pub order_nid: i32,
    pub order_date: String,
    pub settlement_date: Option<String>,
    pub market_nid: i32,
    pub board_nid: i32,
    pub broker_nid: i32,
    pub buy_sell: String,
    pub stock_nid: i32,
    pub client_nid: i32,
    pub order_price: Decimal,
    pub order_volume: Decimal,
    pub done_volume: Decimal,
    pub order_status: String,
    pub market_order_id: Decimal,
    pub currency_nid: i32,
    pub commission_mode: Option<i32>,
    pub commission_percent: Option<Decimal>,
    pub settlement_mode: Option<i32>,
    pub day_trade: bool,
    pub online_trading: bool,
    pub ng_report: bool,
    pub force_buy_sell: bool,
    pub office_nid: Option<i32>,
    pub trx_n_type: i32,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerTrxEntity {
    pub broker_trx_nid: i32,
    pub date: chrono::NaiveDateTime,
    pub broker_nid: i32,
    pub broker_trx_n_type: i32,
    pub buy_sell: String,
    pub stock_nid: Option<i32>,
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,
    pub buy_amount: Decimal,
    pub sell_amount: Decimal,
    pub settlement_mode: Option<i32>,
    pub final_flag: bool,
}

#[derive(Debug)]
pub struct BrokerTrxConfig {
    pub broker_trx_n_type: i32,
    pub broker_trx_mode: i32,
    pub broker_trx_net: i32,
}

#[derive(Debug, Clone, Default)]
pub struct ProcessingConfig {
    pub base_currency_nid: i32,
    pub default_market_nid: i32,
    pub default_board_nid: i32,
    pub default_due_days: i32,
    pub processing_date: chrono::NaiveDateTime,
    pub user_nid: i32,
    pub ip_address: String,
    pub computer_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct PartialMatch {
    pub trade_nid: i32,
    pub order_nid: i32,
    pub alloc_volume: Decimal,
    pub remain_volume: Decimal,
}

#[derive(Debug, Clone, Default)]
pub struct MatchResult {
    pub full_trade: Vec<(i32, i32)>, // trade_nid, order_nid
    pub partial: Vec<PartialMatch>,
    pub order_done: Vec<OrderDone>,
}

#[derive(Debug, Clone, Default)]
pub struct OrderDone {
    pub order_nid: i32,
    pub done_volume: i32,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerTrxInsert {
    pub broker_nid: i32,
    pub broker_trx_n_type: i32,
    pub buy_sell: String,
    pub stock_nid: Option<i32>,
    pub trade_nid: Option<i32>,
    pub due_date: Option<chrono::NaiveDateTime>,
    pub settlement_mode: Option<i32>,
    pub commission_percent: Option<Decimal>,
    pub buy_commission_percent: Option<Decimal>,
    pub sell_commission_percent: Option<Decimal>,
    pub minimum_fee: Option<Decimal>,
    pub buy_minimum_fee: Option<Decimal>,
    pub sell_minimum_fee: Option<Decimal>,
    pub order_nid: Option<i32>,
    pub buy_amount: Option<Decimal>,
    pub sell_amount: Option<Decimal>,
}