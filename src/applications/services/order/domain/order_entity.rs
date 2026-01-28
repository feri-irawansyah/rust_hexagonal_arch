use chrono::NaiveDate;
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
pub struct TradeMatched {
    pub order_nid: i32,
    pub buy_amount: Decimal,
    pub sell_amount: Decimal,
}

#[derive(Debug, Clone, Default)]
// pub struct BrokerTrxTmp {
//     pub order_nid: i32,
//     pub broker_nid: i32,
//     pub broker_trx_n_type: i16,
//     pub b_buy_sell: char,
//     pub b_stock_nid: i32,
//     pub b_trade_nid: i32,
//     pub buy_volume: i64,
//     pub sell_volume: i64,
//     pub due_date: NaiveDate,
//     pub settlement_mode: i16,
//     pub commission_percent: Decimal,
//     pub buy_commission_percent: Decimal,
//     pub sell_commission_percent: Decimal,
//     pub minimum_fee: Decimal,
//     pub buy_minimum_fee: Decimal,
//     pub sell_minimum_fee: Decimal,
// }
pub struct BrokerTrxTmp {
    pub broker_nid: i32,
    pub broker_trx_n_type: i16,
    pub b_buy_sell: char,
    pub b_stock_nid: i32,
    pub b_trade_nid: i32,
    pub total_buy_volume: i64,
    pub total_sell_volume: i64,
    pub due_date: NaiveDate,
    pub settlement_mode: i16,
    pub commission_percent: Decimal,
    pub buy_commission_percent: Decimal,
    pub sell_commission_percent: Decimal,
    pub minimum_fee: Decimal,
    pub buy_minimum_fee: Decimal,
    pub sell_minimum_fee: Decimal,
    // Simpan semua order_nid dalam group ini
    pub order_nids: Vec<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct ClientTrxTmp {
    pub client_nid: i32,
    pub client_trx_n_type: i16,
    pub c_buy_sell: char,
    pub c_stock_nid: i32,
    pub c_order_nid: i32,
    pub c_buy_avg_price: Decimal,
    pub c_sell_avg_price: Decimal,
    pub buy_volume: i64,
    pub sell_volume: i64,
    pub levy_percent: Decimal,
    pub sinking_fund_percent: Decimal,
    pub income_tax_percent: Decimal,
    pub due_date: NaiveDate,
    pub c_settlement_mode: i16,
    pub sales_person_nid: i32,
    pub office_nid: i32,
    pub referral_nid: i32,
    pub commission_mode: i16,
    pub commission_percent: Decimal,
    pub buy_commission_percent: Decimal,
    pub sell_commission_percent: Decimal,
    pub vat_percent: Decimal,
    pub minimum_fee: Decimal,
    pub buy_minimum_fee: Decimal,
    pub sell_minimum_fee: Decimal,
    pub wht_percent: Decimal,
    pub referral_as_expense: bool,
    pub day_trade: bool,
    pub online_trading: bool,
    pub force_buy_sell: bool,
    pub stamp_duty_as_expense: bool,
    pub exclude_stamp_duty_from_proceed_amount: bool,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerTrxInsert {
    pub date: NaiveDate,
    pub broker_nid: i32,
    pub broker_trx_n_type: i16,
    pub buy_sell: String,
    pub stock_nid: i32,
    pub trade_nid: i32,
    pub buy_volume: i64,
    pub sell_volume: i64,
    pub buy_amount: Decimal,
    pub sell_amount: Decimal,
    pub ar_amount: Decimal,
    pub ap_amount: Decimal,
    pub net_amount: Decimal,
    pub due_date: NaiveDate,
    pub settlement_mode: i16,
    pub commission_percent: Decimal,
    pub buy_commission_percent: Decimal,
    pub sell_commission_percent: Decimal,
    pub minimum_fee: Decimal,
    pub buy_minimum_fee: Decimal,
    pub sell_minimum_fee: Decimal,
    pub entry_user_nid: i32,
    pub entry_ip_address: String,
    pub entry_computer_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct ClientTrxInsert {
    pub date: NaiveDate,
    pub client_nid: i32,
    pub client_trx_n_type: i16,
    pub buy_sell: String,
    pub stock_nid: i32,
    pub order_nid: i32,
    pub buy_avg_price: Decimal,
    pub sell_avg_price: Decimal,
    pub buy_volume: i64,
    pub sell_volume: i64,
    pub levy_percent: Decimal, 
    pub sinking_fund_percent: Decimal,
    pub income_tax_percent: Decimal, 
    pub buy_amount: Decimal,
    pub sell_amount: Decimal,
    pub net_amount: Decimal,
    pub due_date: NaiveDate,
    pub settlement_mode: i16,
    pub sales_person_nid: i32,
    pub office_nid: i32,
    pub referral_nid: i32,
    pub commission_mode: i16,
    pub commission_percent: Decimal,
    pub buy_commission_percent: Decimal,
    pub sell_commission_percent: Decimal,
    pub vat_percent: Decimal,
    pub minimum_fee: Decimal,
    pub buy_minimum_fee: Decimal,
    pub sell_minimum_fee: Decimal,
    pub wht_percent: Decimal,
    pub referral_as_expense: bool,
    pub day_trade: bool,
    pub online_trading: bool,
    pub force_buy_sell: bool,
    pub stamp_duty_as_expense: bool,
    pub exclude_stamp_duty_from_proceed_amount: bool,
    pub entry_user_nid: i32,
    pub entry_ip_address: String,
    pub entry_computer_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerTrxInsertOld {
    pub date: String,
    pub broker_nid: i32,
    pub broker_trx_n_type: i32,
    pub buy_sell: String,
    pub stock_nid: Option<i32>,
    pub trade_nid: Option<i32>,
    pub buy_avg_price: Decimal,
    pub sell_avg_price: usize,
    pub buy_volume: usize,
    pub sell_volume: usize,
    pub buy_amount: usize,
    pub sell_amount: Decimal,
    pub net_amount: usize,
    pub due_date: String,
    pub settlement_mode: Option<i32>,
    pub commission_percent: Option<usize>,
    pub buy_commission_percent: Option<usize>,
    pub sell_commission_percent: Option<usize>,
    pub minimum_fee: Option<usize>,
    pub buy_minimum_fee: Option<usize>,
    pub sell_minimum_fee: Option<usize>,
    pub entry_time: String,
}