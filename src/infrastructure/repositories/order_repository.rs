use async_trait::async_trait;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use std::{collections::HashMap, pin::pin};
use anyhow::Result;
use deadpool_postgres::Pool;
use rust_decimal::{Decimal, prelude::FromPrimitive};

use crate::{applications::{
    contracts::order_trait::TOrderRepository, services::order::domain::order_entity::{
        BrokerTrxConfig, BrokerTrxEntity, BrokerTrxInsert, BrokerTrxInsertOld, BrokerTrxState, BrokerTrxTmp, ClientTrxInsert, ClientTrxState, ClientTrxTmp, MatchResult, OrderDone, OrderEntity, PartialMatch, ProcessingConfig, SalesPerson, TradeMatched
    }
}, infrastructure::database::snapshot::SnapshotDb};

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
    async fn get_views(&self) -> Result<Vec<tokio_postgres::Row>> {
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

    async fn match_orders(&self, config: &ProcessingConfig, order: &OrderEntity, btx: &mut BrokerTrxState, _ctrx: & mut ClientTrxState,  order_ids: &[i32]) -> Result<()> {
        let mut conn = self.conn.get().await?;
        let mut tx = conn.transaction().await?;

        // Update orders batch
        self.update_order_status_batch(&mut tx, order_ids).await?;

        let should_search = self.should_search_broker_trx(order, btx);

        if !should_search && btx.broker_trx_nid > 0 {
            return Ok(());
        }

        // Update broker trx
        if btx.broker_trx_nid > 0 {
            self.update_broker_trx(&mut tx, btx.broker_trx_nid).await?;
        }

        let broker_trx_config = BrokerTrxConfig {
            broker_trx_n_type: 1,
            broker_trx_mode: 0,
            broker_trx_net: 0,
        };

        let broker_trx_result = BrokerTrxEntity {
            broker_trx_nid: order.broker_nid,
            date: config.processing_date,
            broker_nid: order.broker_nid,
            broker_trx_n_type: broker_trx_config.broker_trx_n_type,
            buy_sell: order.buy_sell.to_string(),
            stock_nid: order.stock_nid.into(),
            buy_volume: if order.buy_sell == 'B'.to_string() { order.order_volume } else { Decimal::ZERO },
            sell_volume: if order.buy_sell == 'S'.to_string() { order.order_volume } else { Decimal::ZERO },
            buy_amount: if order.buy_sell == 'B'.to_string() { order.order_price * order.order_volume } else { Decimal::ZERO },
            sell_amount: if order.buy_sell == 'S'.to_string() { order.order_price * order.order_volume } else { Decimal::ZERO },
            settlement_mode: order.settlement_mode,
            final_flag: false,
        };

        let broker_trx_nid = self.create_or_update_broker_trx(&mut tx, &broker_trx_result, &config).await?;
        btx.broker_trx_nid = broker_trx_nid;
        btx.broker_trx_mode = broker_trx_config.broker_trx_mode;
        btx.broker_trx_net = broker_trx_config.broker_trx_net;
        tx.commit().await?;
        Ok(())
    }

    async fn update_order_status_batch(&self, tx: &mut tokio_postgres::Transaction<'_>, order_ids: &[i32]) -> Result<()> {
        tx.execute(
            r#"UPDATE "order" SET order_status = 'M' WHERE order_nid = ANY($1)"#,
            &[&order_ids]
        ).await?;
        Ok(())
    }

    async fn update_order_status(&self, order_id: i32) -> Result<()> {
        let conn = self.conn.get().await?;

        let status = "M".to_string();

        conn.execute(
            "UPDATE \"order\" SET order_status = $1 WHERE order_nid = $2;",
            &[&status, &order_id],
        )
        .await?;

        Ok(())
    }
    
    async fn update_broker_trx(&self, tx: &mut tokio_postgres::Transaction<'_>, broker_trx_nid: i32) -> Result<()> {
        tx.execute(r#"
            UPDATE broker_trx
            SET 
                ap_amount = buy_amount + buy_levy + buy_sinking_fund + buy_fee + buy_vat +
                           buy_commission - buy_wht +
                           buy_charge_amount1 + buy_charge_amount2 + buy_charge_amount3 +
                           buy_charge_amount4 + buy_charge_amount5 + buy_charge_amount6 +
                           buy_charge_amount7 + buy_charge_amount8 + buy_charge_amount9,
                
                ar_amount = sell_amount - sell_levy - sell_sinking_fund - income_tax -
                           sell_fee - sell_vat - sell_commission + sell_wht -
                           sell_charge_amount1 - sell_charge_amount2 - sell_charge_amount3 -
                           sell_charge_amount4 - sell_charge_amount5 - sell_charge_amount6 -
                           sell_charge_amount7 - sell_charge_amount8 - sell_charge_amount9,
                
                net_amount = ar_amount - ap_amount,
                
                due_date = CASE
                    WHEN ar_amount > ap_amount
                        THEN COALESCE(
                            cash_receive_date,
                            entry_time,
                            NOW()
                        )
                    ELSE
                        COALESCE(
                            cash_payment_date,
                            entry_time,
                            NOW()
                        )
                END,
                final = true
            WHERE broker_trx_nid = $1
            "#, &[&broker_trx_nid])
        .await?;
        Ok(())
    }

    async fn create_or_update_broker_trx(&self,tx: &mut tokio_postgres::Transaction<'_> , broker_trx: &BrokerTrxEntity, config: &ProcessingConfig) -> Result<i32> {
        let buy_volume  = broker_trx.buy_volume;
        let sell_volume = broker_trx.sell_volume;
        let buy_amount  = broker_trx.buy_amount;
        let sell_amount = broker_trx.sell_amount;

        let existing = tx.query_opt(
            r#"
            SELECT broker_trx_nid
            FROM broker_trx
            WHERE broker_nid = $1
            AND broker_trx_n_type = $2
            AND buy_sell = $3
            AND stock_nid IS NOT DISTINCT FROM $4
            AND settlement_mode IS NOT DISTINCT FROM $5
            ORDER BY broker_trx_nid DESC
            LIMIT 1
            "#,
            &[
                &broker_trx.broker_nid,
                &broker_trx.broker_trx_n_type,
                &broker_trx.buy_sell.to_string(),
                &broker_trx.stock_nid,
                &broker_trx.settlement_mode,
            ],
        )
        .await?;

        if let Some(record) = existing {
            let broker_trx_nid: i32 = record.get("broker_trx_nid");

            tx.execute(
                r#"
                UPDATE broker_trx
                SET buy_volume  = buy_volume  + $1,
                    sell_volume = sell_volume + $2,
                    buy_amount  = buy_amount  + $3,
                    sell_amount = sell_amount + $4
                WHERE broker_trx_nid = $5
                "#,
                &[
                    &buy_amount,
                    &sell_volume,
                    &buy_amount,
                    &sell_amount,
                    &broker_trx_nid,
                ],
            )
            .await?;

            Ok(broker_trx_nid)
        }
        else {
            // Insert new
            let result = tx.query_opt(
                r#"
                INSERT INTO broker_trx (
                    date, broker_nid, broker_trx_n_type, buy_sell,
                    stock_nid, buy_volume, sell_volume, buy_amount, sell_amount,
                    settlement_mode, entry_user_nid, entry_ip_address,
                    entry_computer_name, entry_time, due_date, currency_nid
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW(), 1)
                RETURNING broker_trx_nid
                "#,
                &[
                    &config.processing_date,
                    &broker_trx.broker_nid,
                    &broker_trx.broker_trx_n_type,
                    &broker_trx.buy_sell.to_string(),
                    &broker_trx.stock_nid.unwrap_or_default(),
                    &buy_volume,
                    &sell_volume,
                    &buy_amount,
                    &sell_amount,
                    &broker_trx.settlement_mode.unwrap_or_default(),
                    &config.user_nid,
                    &config.ip_address,
                    &config.computer_name
                ],
            )
            .await?;

            if let Some(row) = result {
                let broker_trx_nid: i32 = row.get("broker_trx_nid");
                Ok(broker_trx_nid)
            } else {
                Err(anyhow::anyhow!("Failed to insert broker_trx"))
            }
        }
    }

    fn should_search_broker_trx(&self, order: &OrderEntity, state: &BrokerTrxState) -> bool {
        if state.broker_trx_nid == 0 {
            return true;
        }
        
        if order.trx_n_type != state.prev_trx_n_type {
            return true;
        }
        
        if order.broker_nid != state.prev_broker_nid {
            return true;
        }
        
        if state.broker_trx_net == 1 && order.buy_sell != state.prev_buy_sell {
            return true;
        }
        
        if state.broker_trx_mode == 1 && order.stock_nid != state.prev_stock_nid {
            return true;
        }
        
        if state.broker_trx_mode == 2 {
            return true;
        }
        
        false
    }

    async fn match_orders_async(&self, result: MatchResult) -> Result<()> {
        let mut conn = self.conn.get().await?;
        let mut tx = conn.transaction().await?;
        const CHUNK: usize = 50_000;

        // 1️⃣ FULL MATCH
        for chunk in result.full_trade.chunks(CHUNK) {
            let (trade_ids, order_ids): (Vec<_>, Vec<_>) =
                chunk.iter().cloned().unzip();

            self.update_trade_match(&mut tx, &trade_ids, &order_ids).await?;
        }

        println!("Matched trades success");

        // 2️⃣ PARTIAL MATCH
        for chunk in result.partial.chunks(CHUNK) {
            self.update_trade_partial(&mut tx, chunk).await?;
            self.insert_trade_remainder(&mut tx, chunk).await?;
        }

        println!("Matched partial trades success");

        // 3️⃣ ORDER MATCH
        for chunk in result.order_done.chunks(CHUNK) {
            self.update_order_match(&mut tx, chunk).await?;
        }

        println!("Matched order success");

        let snapshot_result = tokio::task::spawn_blocking(move || -> anyhow::Result<(Vec<BrokerTrxTmp>, Vec<ClientTrxTmp>)> {
            // let trade_matched = trade_matched;
            let snapshot = SnapshotDb::new().unwrap();

            let _ = snapshot.conn().execute(r#"
                CREATE OR REPLACE TABLE client_trx_temp AS
                SELECT * FROM read_parquet("snapshot_mv_temp_client_trx.parquet");
            "#, []);

            let _ = snapshot.conn().execute(r#"
                CREATE OR REPLACE TABLE broker_trx_temp AS
                SELECT * FROM read_parquet("snapshot_mv_temp_broker_trx.parquet");
            "#, []);

            let _ = snapshot.conn().execute(r#"
                CREATE OR REPLACE TABLE alloc_temp AS
                    select
                    c.order_nid,
                    t.trade_nid,
                    t.market_order_id,
                    t.client_nid,
                    t.stock_nid,
                    t.buy_sell,
                    t.trade_price,

                    case
                        when t.running_volume <= c.order_volume
                            then t.trade_volume
                        when t.running_volume - t.trade_volume < c.order_volume
                            then c.order_volume - (t.running_volume - t.trade_volume)
                        else 0
                    end as alloc_volume,

                    t.trade_volume
                from
                (
                    select
                        t.trade_nid,
                        t.market_order_id,
                        t.client_nid,
                        t.stock_nid,
                        t.buy_sell,
                        t.order_nid,
                        t.trade_price,
                        t.trade_volume,
                        sum(t.trade_volume) over
                        (
                            partition by
                                t.market_order_id,
                                t.client_nid,
                                t.buy_sell,
                                t.stock_nid
                            order by
                                t.trade_nid
                            rows unbounded preceding
                        ) as running_volume
                    from read_parquet('snapshot_trade.parquet') t
                    where
                        t.trade_date = '2026-01-15'
                        and t.matched = false
                ) t
                join client_trx_temp c
                    on c.market_order_id = t.market_order_id
                and c.buy_sell        = t.buy_sell
                and c.stock_nid       = t.stock_nid
                where
                    case
                        when t.running_volume <= c.order_volume
                            then t.trade_volume
                        when t.running_volume - t.trade_volume < c.order_volume
                            then c.order_volume - (t.running_volume - t.trade_volume)
                        else 0
                    end > 0;
            "#, []);
            
            let result = snapshot.conn().execute(r#"
                CREATE TEMP TABLE match_result AS
                SELECT
                    t.trade_nid,
                    a.order_nid,
                    a.alloc_volume,
                    t.trade_volume - a.alloc_volume AS remain_volume,
                    CASE
                        WHEN a.alloc_volume = t.trade_volume THEN 1
                        ELSE 2
                    END AS match_type
                FROM read_parquet('snapshot_trade.parquet') t
                JOIN alloc_temp a
                    ON a.trade_nid = t.trade_nid
                WHERE a.alloc_volume > 0
            "#, []).map_err(|err| println!("Err: {}", err));
            
            print!("Match result created");

            match result {
                Ok(_) => {
                    let _ = snapshot.conn().execute(r#"
                        UPDATE broker_trx_temp bt
                        SET b_trade_nid = m.trade_nid
                        FROM match_result m
                        WHERE m.order_nid = bt.order_nid
                        AND bt.trx_mode = 2
                    "#, []).map_err(|err| println!("Err: {}", err));
                    println!("Temp Broker Trx created");

                    let _ = snapshot.conn().execute(r#"
                        UPDATE client_trx_temp ct
                        SET done_volume = x.done_volume
                        FROM (
                            SELECT
                                order_nid,
                                SUM(alloc_volume) AS done_volume
                            FROM match_result
                            GROUP BY order_nid
                        ) x
                        WHERE x.order_nid = ct.order_nid
                    "#, []).map_err(|err| println!("Err: {}", err));
                    println!("Temp Client Trx created");
                },
                Err(e) => {
                    println!("Failed to create broker_trx_temp: {:?}", e);
                }
            }

            let mut stmt_btx = snapshot.conn().prepare(
                r#"
                select
                    bt.broker_nid,
                    bt.broker_trx_n_type,
                    bt.b_buy_sell,
                    bt.b_stock_nid,
                    bt.b_trade_nid,
                    bt.due_date,
                    bt.settlement_mode,
                    bt.commission_percent,
                    bt.buy_commission_percent,
                    bt.sell_commission_percent,
                    bt.minimum_fee,
                    bt.buy_minimum_fee,
                    bt.sell_minimum_fee,
                    sum(bt.buy_volume) as total_buy_volume,
                    sum(bt.sell_volume) as total_sell_volume,
                    -- Kumpulkan semua order_nid untuk join nanti
                    group_concat(bt.order_nid) as order_nids
                from broker_trx_temp bt
                group by
                    bt.broker_nid,
                    bt.broker_trx_n_type,
                    bt.b_buy_sell,
                    bt.b_stock_nid,
                    bt.b_trade_nid,
                    bt.due_date,
                    bt.settlement_mode,
                    bt.commission_percent,
                    bt.buy_commission_percent,
                    bt.sell_commission_percent,
                    bt.minimum_fee,
                    bt.buy_minimum_fee,
                    bt.sell_minimum_fee;
                "#
            ).unwrap();

            let mut rows_btx = stmt_btx.query([]).unwrap();

            let mut btx_temp_block = Vec::new();
            let mut ctx_temp_block = Vec::new();

            while let Some(row) = rows_btx.next()? {
                let order_nids_str: String = row.get(15).unwrap_or_default();
                let order_nids: Vec<i32> = order_nids_str
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
                    
                btx_temp_block.push(BrokerTrxTmp {
                    broker_nid: row.get(0).unwrap_or_default(),
                    broker_trx_n_type: row.get(1).unwrap_or_default(),
                    b_buy_sell: row.get::<_, String>(2)?.chars().next().unwrap_or(' '),
                    b_stock_nid: row.get(3).unwrap_or_default(),
                    b_trade_nid: row.get(4).unwrap_or_default(),
                    due_date: row.get(5).unwrap_or_default(),
                    settlement_mode: row.get(6).unwrap_or_default(),
                    commission_percent: Decimal::from_usize(row.get(7).unwrap_or_default()).unwrap_or_default(),
                    buy_commission_percent: Decimal::from_usize(row.get(8)?).unwrap_or_default(),
                    sell_commission_percent: Decimal::from_usize(row.get(9)?).unwrap_or_default(),
                    minimum_fee: Decimal::from_usize(row.get(10)?).unwrap_or_default(),
                    buy_minimum_fee: Decimal::from_usize(row.get(11)?).unwrap_or_default(),
                    sell_minimum_fee: Decimal::from_usize(row.get(12)?).unwrap_or_default(),
                    total_buy_volume: row.get(13).unwrap_or_default(),
                    total_sell_volume: row.get(14).unwrap_or_default(),
                    order_nids,
                });
            }
            
            let mut stmt_ctx = snapshot.conn().prepare(
                r#"
                select
                    ct.client_nid,
                    ct.client_trx_n_type,
                    ct.c_buy_sell,
                    ct.c_stock_nid,
                    ct.c_order_nid,
                    ct.c_buy_avg_price,
                    ct.c_sell_avg_price,
                    sum(ct.buy_volume) as buy_volume,
                    sum(ct.sell_volume) as sell_volume,
                    ct.due_date,
                    ct.c_settlement_mode,
                    ct.sales_person_nid,
                    ct.office_nid,
                    ct.referral_nid,
                    ct.commission_mode,
                    ct.commission_percent,
                    ct.buy_commission_percent,
                    ct.sell_commission_percent,
                    ct.vat_percent,
                    ct.minimum_fee,
                    ct.buy_minimum_fee,
                    ct.sell_minimum_fee,
                    ct.wht_percent,
                    ct.referral_as_expense,
                    ct.day_trade,
                    ct.online_trading,
                    ct.force_buy_sell,
                    ct.stamp_duty_as_expense,
                    ct.exclude_stamp_duty_from_proceed_amount,
                    group_concat(ct.levy_percent) as levy_percents,
                    group_concat(ct.sinking_fund_percent) as sinking_fund_percents,
                    group_concat(ct.income_tax_percent) as income_tax_percents,
                    group_concat(ct.order_nid) as order_nids
                from client_trx_temp  ct
                group by
                    ct.client_nid,
                    ct.client_trx_n_type,
                    ct.c_buy_sell,
                    ct.c_stock_nid,
                    ct.c_order_nid,
                    ct.c_buy_avg_price,
                    ct.c_sell_avg_price,
                    ct.due_date,
                    ct.c_settlement_mode,
                    ct.sales_person_nid,
                    ct.office_nid,
                    ct.referral_nid,
                    ct.commission_mode,
                    ct.commission_percent,
                    ct.buy_commission_percent,
                    ct.sell_commission_percent,
                    ct.vat_percent,
                    ct.minimum_fee,
                    ct.buy_minimum_fee,
                    ct.sell_minimum_fee,
                    ct.wht_percent,
                    ct.referral_as_expense,
                    ct.day_trade,
                    ct.online_trading,
                    ct.force_buy_sell,
                    ct.stamp_duty_as_expense,
                    ct.exclude_stamp_duty_from_proceed_amount;
                "#
            ).unwrap();

            let mut rows_ctx = stmt_ctx.query([]).unwrap();

            while let Some(row) = rows_ctx.next().unwrap() {
                
                let levy_percent_str: String = row.get(29).unwrap_or_default();
                let levy_percents: Vec<Decimal> = levy_percent_str
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
                let sinking_fund_percent_str: String = row.get(30).unwrap_or_default();
                let sinking_fund_percents: Vec<Decimal> = sinking_fund_percent_str
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
                let income_tax_percent_str: String = row.get(31).unwrap_or_default();
                let income_tax_percents: Vec<Decimal> = income_tax_percent_str
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
                let order_nid_str: String = row.get(32).unwrap_or_default();
                let order_nids: Vec<i32> = order_nid_str
                    .split(',')
                    .filter_map(|s| s.parse().ok())
                    .collect();
                
                ctx_temp_block.push(ClientTrxTmp { 
                    client_nid: row.get(0).unwrap_or_default(), 
                    client_trx_n_type: row.get(1).unwrap_or_default(), 
                    c_buy_sell: row.get::<_, String>(2)?.chars().next().unwrap_or(' '), 
                    c_stock_nid: row.get(3).unwrap_or_default(), 
                    c_order_nid: row.get(4).unwrap_or_default(), 
                    c_buy_avg_price: Decimal::from_usize(row.get(5).unwrap_or_default()).unwrap_or_default(), 
                    c_sell_avg_price: Decimal::from_usize(row.get(6).unwrap_or_default()).unwrap_or_default(), 
                    buy_volume: row.get(7).unwrap_or_default(), 
                    sell_volume: row.get(8).unwrap_or_default(), 
                    due_date: row.get(9).unwrap_or_default(), 
                    c_settlement_mode: row.get(10).unwrap_or_default(), 
                    sales_person_nid: row.get(11).unwrap_or_default(), 
                    office_nid: row.get(12).unwrap_or_default(), 
                    referral_nid: row.get(13).unwrap_or_default(), 
                    commission_mode: row.get(14).unwrap_or_default(), 
                    commission_percent: Decimal::from_usize(row.get(15).unwrap_or_default()).unwrap_or_default(), 
                    buy_commission_percent: Decimal::from_usize(row.get(16).unwrap_or_default()).unwrap_or_default(), 
                    sell_commission_percent: Decimal::from_usize(row.get(17).unwrap_or_default()).unwrap_or_default(), 
                    vat_percent: Decimal::from_usize(row.get(18).unwrap_or_default()).unwrap_or_default(), 
                    minimum_fee: Decimal::from_usize(row.get(19).unwrap_or_default()).unwrap_or_default(), 
                    buy_minimum_fee: Decimal::from_usize(row.get(20).unwrap_or_default()).unwrap_or_default(), 
                    sell_minimum_fee: Decimal::from_usize(row.get(21).unwrap_or_default()).unwrap_or_default(), 
                    wht_percent: Decimal::from_usize(row.get(22).unwrap_or_default()).unwrap_or_default(), 
                    referral_as_expense: row.get(23).unwrap_or_default(), 
                    day_trade: row.get(23).unwrap_or_default(), 
                    online_trading: row.get(23).unwrap_or_default(), 
                    force_buy_sell: row.get(23).unwrap_or_default(), 
                    stamp_duty_as_expense: row.get(23).unwrap_or_default(), 
                    exclude_stamp_duty_from_proceed_amount: row.get(23).unwrap_or_default(), 
                    levy_percent: levy_percents, 
                    sinking_fund_percent: sinking_fund_percents, 
                    income_tax_percent: income_tax_percents,
                    order_nids: order_nids,
                 });
            }

            Ok((btx_temp_block, ctx_temp_block))

        }).await?;

        let (btx_snapshot, ctx_snapshot) = snapshot_result.unwrap();

        let trade_map = self.get_trade_matched_map(&mut tx).await?;

        let btx_rows = self.build_insert_btx_rows(
            btx_snapshot,
            &trade_map,
        );

        let copy_btx_result = self.copy_btx_to_postgres(&mut tx, &btx_rows).await;

        match copy_btx_result {
            Ok(_) =>  {
                println!("Success cppy btx")
            }, 
            Err(e) => {
                println!("Failed to copy btx: {}", e);
            }
        }

        let ctx_rows = self.build_insert_ctx_rows(
            ctx_snapshot,
            &trade_map,
        );

        println!("ctx rows: {}", ctx_rows.len());

        let copy_tx_result = self.copy_ctx_to_postgres(&mut tx, &ctx_rows).await;

        match copy_tx_result {
            Ok(_) =>  {
                println!("Success cppy ctx")
            }, 
            Err(e) => {
                println!("Failed to copy ctx: {:?}", e);
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn get_trade_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
        let stmt = trans.prepare(
            "
            SELECT *
            FROM trade
            WHERE trade_nid > $1
            ORDER BY trade_nid
            "
        ).await?;

        let portal = trans.bind(&stmt, &[&last_id]).await?;
        let result = trans.query_portal(&portal, limit).await?;
        Ok(result)
    }

    async fn get_order_list_async(&self, trans: &mut tokio_postgres::Transaction<'_> , last_id: i32, limit: i32) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
        let stmt = trans.prepare(
            "
            SELECT *
            FROM \"order\"
            WHERE order_nid > $1
            ORDER BY order_nid
            "
        ).await?;

        let portal = trans.bind(&stmt, &[&last_id]).await?;
        let result = trans.query_portal(&portal, limit).await?;
        Ok(result)
    }

    fn create_btx_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error> { 
        let result = snapshot.conn().execute(r#"
            CREATE OR REPLACE TABLE broker_trx_temp AS
            SELECT * FROM read_parquet("snapshot_mv_temp_broker_trx.parquet");
        "#, []);
        
        match result {
            Ok(_) => Ok("Temp Broker Trx created".to_string()),
            Err(e) => Err(e.into()),
        }
    }

    fn create_ctx_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error> { 
        let result = snapshot.conn().execute(r#"
            CREATE OR REPLACE TABLE client_trx_temp AS
            SELECT * FROM read_parquet("snapshot_mv_temp_client_trx.parquet");
        "#, []);
        
        match result {
            Ok(_) => Ok("Temp client Trx created".to_string()),
            Err(e) => Err(e.into()),
        }
    }

    fn create_alloc_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error> {
        let result = snapshot.conn().execute(r#"
            CREATE OR REPLACE TABLE alloc_temp AS
            select
            c.order_nid,
            t.trade_nid,
            t.market_order_id,
            t.client_nid,
            t.stock_nid,
            t.buy_sell,
            t.trade_price,

            case
                when t.running_volume <= c.order_volume
                    then t.trade_volume
                when t.running_volume - t.trade_volume < c.order_volume
                    then c.order_volume - (t.running_volume - t.trade_volume)
                else 0
            end as alloc_volume,

            t.trade_volume
        from
        (
            select
                t.trade_nid,
                t.market_order_id,
                t.client_nid,
                t.stock_nid,
                t.buy_sell,
                t.order_nid,
                t.trade_price,
                t.trade_volume,
                sum(t.trade_volume) over
                (
                    partition by
                        t.market_order_id,
                        t.client_nid,
                        t.buy_sell,
                        t.stock_nid
                    order by
                        t.trade_nid
                    rows unbounded preceding
                ) as running_volume
            from read_parquet('snapshot_trade.parquet') t
            where
                t.trade_date = '2026-01-15'
                and t.matched = false
        ) t
        join read_parquet("snapshot_mv_temp_client_trx.parquet") c
            on c.market_order_id = t.market_order_id
        and c.buy_sell        = t.buy_sell
        and c.stock_nid       = t.stock_nid
        where
            case
                when t.running_volume <= c.order_volume
                    then t.trade_volume
                when t.running_volume - t.trade_volume < c.order_volume
                    then c.order_volume - (t.running_volume - t.trade_volume)
                else 0
            end > 0;
        "#, []);
        
        match result {
            Ok(_) => Ok("Temp alloc created".to_string()),
            Err(e) => Err(e.into()),
        }
    }

    async fn update_trade_match(&self, tx: &mut tokio_postgres::Transaction<'_>, trade_nids: &[i32], order_nids: &[i32]) -> Result<()> {
        tx.execute(
            r#"
            UPDATE trade t
            SET
                matched   = true,
                order_nid = u.order_nid
            FROM (
                SELECT
                    unnest($1::int[]) AS trade_nid,
                    unnest($2::int[]) AS order_nid
            ) u
            WHERE
                t.trade_nid = u.trade_nid
            "#,
            &[&trade_nids, &order_nids],
        )
        .await?;

        Ok(())
    }

    async fn update_order_match(&self, tx: &mut tokio_postgres::Transaction<'_>, rows: &[OrderDone]) -> Result<()> {

        let order_nids: Vec<i32> =
            rows.iter().map(|r| r.order_nid).collect();

        let done_volumes: Vec<i32> =
            rows.iter().map(|r| r.done_volume).collect();

        tx.execute(
            r#"
            update "order" o
            set
                done_volume = u.done_volume,
                order_status = case
                    when u.done_volume = 0 then 'O'
                    when o.order_volume = u.done_volume then 'M'
                    when o.order_volume > u.done_volume then 'P'
                    when o.order_volume < u.done_volume then 'P'
                end
            from (
                select
                    unnest($1::int[]) as order_nid,
                    unnest($2::int[]) as done_volume
            ) u
            where u.order_nid = o.order_nid
            "#,
            &[&order_nids, &done_volumes],
        )
        .await?;

        Ok(())
    }

    async fn update_trade_partial(&self, tx: &mut tokio_postgres::Transaction<'_>, rows: &[PartialMatch]) -> Result<()> {

        let trade_nids: Vec<i32> = rows.iter().map(|r| r.trade_nid).collect();
        let order_nids: Vec<i32> = rows.iter().map(|r| r.order_nid).collect();
        let alloc_volumes: Vec<Decimal> = rows.iter().map(|r| r.alloc_volume).collect();

        tx.execute(
            r#"
            UPDATE trade t
            SET
                trade_volume = u.alloc_volume,
                matched      = true,
                order_nid    = u.order_nid
            FROM (
                SELECT
                    unnest($1::int[])     AS trade_nid,
                    unnest($2::int[])     AS order_nid,
                    unnest($3::numeric[]) AS alloc_volume
            ) u
            WHERE t.trade_nid = u.trade_nid
            "#,
            &[&trade_nids, &order_nids, &alloc_volumes],
        )
        .await?;

        Ok(())
    }

    async fn insert_trade_remainder(&self, tx: &mut tokio_postgres::Transaction<'_>, rows: &[PartialMatch]) -> Result<()> {

        let trade_nids: Vec<i32> = rows.iter().map(|r| r.trade_nid).collect();
        let remain_volumes: Vec<Decimal> = rows.iter().map(|r| r.remain_volume).collect();

        tx.execute(
            r#"
            INSERT INTO trade
            (
                trade_date, trade_time, session, market_nid, board_nid, broker_side_nid,
                buy_sell, stock_nid, lot_size,
                investor_type, currency_nid, trade_price, trade_volume,
                market_order_id, market_trade_id, off_market_trade_id,
                market_reference, counterpart_reference, market_note,
                trader_id, trading_id, client_nid, source, trx_n_type,
                levy, sinking_fund, income_tax, commission, fee, vat,
                matched, order_nid, client_trx_nid, broker_trx_nid,
                paid, settled_volume, settled_date, settled,
                failed, rejected, reject_user_nid, reject_time,
                reject_ip_address, reject_computer_name,
                selected, select_time, select_user_nid,
                select_computer_name, select_ip_address,
                app_user_nid, app_ip_address, app_computer_name,
                sys_user_id, sys_terminal_id, last_update,
                counterpart_order_id
            )
            SELECT
                t.trade_date, t.trade_time, t.session, t.market_nid, t.board_nid, t.broker_side_nid,
                t.buy_sell, t.stock_nid, t.lot_size,
                t.investor_type, t.currency_nid, t.trade_price,
                u.remain_volume,
                t.market_order_id, t.market_trade_id, t.off_market_trade_id,
                t.market_reference, t.counterpart_reference, t.market_note,
                t.trader_id, t.trading_id, t.client_nid, t.source, t.trx_n_type,
                t.levy, t.sinking_fund, t.income_tax, t.commission, t.fee, t.vat,
                false, NULL, t.client_trx_nid, t.broker_trx_nid,
                t.paid, t.settled_volume, t.settled_date, t.settled,
                t.failed, t.rejected, t.reject_user_nid, t.reject_time,
                t.reject_ip_address, t.reject_computer_name,
                t.selected, t.select_time, t.select_user_nid,
                t.select_computer_name, t.select_ip_address,
                t.app_user_nid, t.app_ip_address, t.app_computer_name,
                t.sys_user_id, t.sys_terminal_id, NOW(),
                t.counterpart_order_id
            FROM trade t
            JOIN (
                SELECT
                    unnest($1::int[])     AS trade_nid,
                    unnest($2::numeric[]) AS remain_volume
            ) u ON u.trade_nid = t.trade_nid
            "#,
            &[&trade_nids, &remain_volumes],
        )
        .await?;

        Ok(())
    }

    fn update_ctx_temp(&self, snapshot: &SnapshotDb) -> Result<String> {
        let result = snapshot.conn().execute(r#"
            update client_trx_temp c
            set done_volume = x.done_volume
            from (
                select
                    order_nid,
                    sum(alloc_volume) as done_volume
                from alloc_temp
                where alloc_volume > 0
                group by order_nid
            ) x
            where x.order_nid = c.order_nid;
        "#, []);

        match result {
            Ok(_) => Ok("".to_string()),
            Err(e) => Err(e.into()),
        }
    }

    fn get_broker_trx_done(&self, snapshot: &SnapshotDb) -> Result<Vec<BrokerTrxInsertOld>, anyhow::Error> {
        let mut stmt = snapshot.conn().prepare(r#"
            SELECT
                '2026-01-15'                       AS date,
                bt.broker_nid,
                bt.broker_trx_n_type,
                bt.b_buy_sell,
                bt.b_stock_nid,
                bt.b_trade_nid,
                0                                 AS buy_avg_price,
                0                                 AS sell_avg_price,
                sum(bt.buy_volume)                AS buy_volume,
                sum(bt.sell_volume)               AS sell_volume,
                sum(t.buy_amount)                 AS buy_amount,
                sum(t.sell_amount)                AS sell_amount,
                sum(t.sell_amount) - sum(t.buy_amount) AS net_amount,
                CAST('2026-01-19' AS VARCHAR) as due_date,
                bt.settlement_mode,
                bt.commission_percent,
                bt.buy_commission_percent,
                bt.sell_commission_percent,
                bt.minimum_fee,
                bt.buy_minimum_fee,
                bt.sell_minimum_fee,
                CAST(now() AS VARCHAR) as entry_time
            FROM broker_trx_temp bt
            JOIN (
                SELECT
                    order_nid,
                    sum(CASE WHEN buy_sell = 'B' THEN trade_volume * trade_price ELSE 0 END) AS buy_amount,
                    sum(CASE WHEN buy_sell = 'S' THEN trade_volume * trade_price ELSE 0 END) AS sell_amount
                FROM read_parquet('snapshot_trade.parquet')
                WHERE trade_date = '2026-01-15'
                AND matched = true
                GROUP BY order_nid
            ) t ON t.order_nid = bt.order_nid
            GROUP BY
                bt.broker_nid,
                bt.broker_trx_n_type,
                bt.b_buy_sell,
                bt.b_stock_nid,
                bt.b_trade_nid,
                bt.settlement_mode,
                bt.commission_percent,
                bt.buy_commission_percent,
                bt.sell_commission_percent,
                bt.minimum_fee,
                bt.buy_minimum_fee,
                bt.sell_minimum_fee
        "#)?;

        let rows = stmt.query_map([], |row| {
            let date_str: Option<String> = row.get(0)?;
            let due_date_str: Option<String> = row.get(13)?;
            let entry_time_str: Option<String> = row.get(21)?;

            let broker_trx = BrokerTrxInsertOld {
                date: date_str.unwrap_or_default(),
                broker_nid: row.get(1).unwrap_or_default(),
                broker_trx_n_type: row.get(2).unwrap_or_default(),
                buy_sell: row.get(3).unwrap_or_default(),
                stock_nid: row.get(4).unwrap_or_default(),
                trade_nid: row.get(5).unwrap_or_default(),
                buy_avg_price: match row.get::<_, usize>(6) {
                    Ok(v) => {
                        rust_decimal::Decimal::from_usize(v).unwrap_or_default()
                    },
                    Err(_) => rust_decimal::Decimal::ZERO,
                },
                sell_avg_price: row.get::<_, usize>(7).unwrap_or_default(),
                buy_volume: row.get::<_, usize>(8).unwrap_or_default(),
                sell_volume: row.get::<_, usize>(9).unwrap_or_default(),
                buy_amount: row.get::<_, usize>(10).unwrap_or_default(),
                sell_amount: match row.get::<_, usize>(11) {
                    Ok(v) => {
                        rust_decimal::Decimal::from_usize(v).unwrap_or_default()
                    },
                    Err(_) => rust_decimal::Decimal::ZERO,
                },
                net_amount: row.get::<_, usize>(12).unwrap_or_default(),
                due_date: due_date_str.unwrap_or_default(),
                settlement_mode: row.get(14).unwrap_or_default(),
                commission_percent: Some(row.get::<_, usize>(15).unwrap_or_default()),
                buy_commission_percent: Some(row.get::<_, usize>(16).unwrap_or_default()),
                sell_commission_percent: Some(row.get::<_, usize>(17).unwrap_or_default()),
                minimum_fee: Some(row.get::<_, usize>(18).unwrap_or_default()),
                buy_minimum_fee: Some(row.get::<_, usize>(19).unwrap_or_default()),
                sell_minimum_fee: Some(row.get::<_, usize>(20).unwrap_or_default()),
                entry_time: entry_time_str.unwrap_or_default(),
            };

            Ok(broker_trx)
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }

        Ok(result)
    }

    async fn get_trade_matched(&self, tx: &mut tokio_postgres::Transaction<'_>) -> Result<Vec<TradeMatched>> {

        let rows = tx.query(r#"
            select
                order_nid,
                sum(case when buy_sell = 'B' then trade_volume * trade_price else 0 end) as buy_amount,
                sum(case when buy_sell = 'S' then trade_volume * trade_price else 0 end) as sell_amount
            from trade
            where trade_date = '2026-01-15'
            and matched = true
            group by order_nid
        "#, &[]).await?;

        let mut result = Vec::new();
        for r in rows {
            result.push(TradeMatched {
                order_nid: r.get(0),
                buy_amount: r.get(1),
                sell_amount: r.get(2),
            });
        }
       
        return Ok(result);
    }

    async fn get_trade_matched_map(&self, tx: &tokio_postgres::Transaction<'_>) -> anyhow::Result<HashMap<i32, TradeMatched>> {

        let rows = tx.query(
            r#"
            select
                order_nid,
                sum(case when buy_sell = 'B' then trade_volume * trade_price else 0 end) as buy_amount,
                sum(case when buy_sell = 'S' then trade_volume * trade_price else 0 end) as sell_amount
            from trade
            where trade_date = '2026-01-15'
            and matched = true
            group by order_nid
            "#,
            &[],
        ).await?;

        let mut map = HashMap::with_capacity(rows.len());

        for r in rows {
            map.insert(
                r.get(0),
                TradeMatched {
                    order_nid: r.get(0),
                    buy_amount: r.get(1),
                    sell_amount: r.get(2),
                },
            );
        }

        Ok(map)
    }

    fn build_insert_btx_rows(&self, aggregated: Vec<BrokerTrxTmp>, trade_map: &HashMap<i32, TradeMatched>) -> Vec<BrokerTrxInsert> {
        let mut out = Vec::new();
        
        for agg in aggregated {
            // Hitung total buy_amount dan sell_amount dari semua order_nid
            let mut total_buy_amount = Decimal::ZERO;
            let mut total_sell_amount = Decimal::ZERO;
            
            for order_nid in &agg.order_nids {
                if let Some(trade) = trade_map.get(order_nid) {
                    total_buy_amount += trade.buy_amount;
                    total_sell_amount += trade.sell_amount;
                }
            }
            
            out.push(BrokerTrxInsert {
                date: chrono::NaiveDateTime::parse_from_str(
                    "2026-01-15 00:00:00",
                    "%Y-%m-%d %H:%M:%S"
                ).unwrap(),
                broker_nid: agg.broker_nid,
                broker_trx_n_type: agg.broker_trx_n_type,
                buy_sell: agg.b_buy_sell.to_string(),
                stock_nid: agg.b_stock_nid,
                trade_nid: agg.b_trade_nid,
                buy_volume: agg.total_buy_volume,
                sell_volume: agg.total_sell_volume,
                buy_amount: total_buy_amount,
                sell_amount: total_sell_amount,
                ar_amount: total_sell_amount,
                ap_amount: total_buy_amount,
                net_amount: total_sell_amount - total_buy_amount,
                due_date: agg.due_date,
                settlement_mode: agg.settlement_mode,
                commission_percent: agg.commission_percent,
                buy_commission_percent: agg.buy_commission_percent,
                sell_commission_percent: agg.sell_commission_percent,
                minimum_fee: agg.minimum_fee,
                buy_minimum_fee: agg.buy_minimum_fee,
                sell_minimum_fee: agg.sell_minimum_fee,
                entry_user_nid: 1,
                entry_ip_address: "".to_string(),
                entry_computer_name: "".to_string(),
            });
        }
        
        out
    }
      
    fn build_insert_ctx_rows(&self, aggregated: Vec<ClientTrxTmp>, trade_map: &HashMap<i32, TradeMatched>) -> Vec<ClientTrxInsert> {

        let mut out = Vec::with_capacity(aggregated.len());
        
        for agg in aggregated {
            // Hitung total buy_amount dan sell_amount dari semua order_nid
            let mut buy_amount_total = Decimal::ZERO;
            let mut sell_amount_total = Decimal::ZERO;

            let mut levy_buy_total = Decimal::ZERO;
            let mut levy_sell_total = Decimal::ZERO;
            let mut sinking_buy_total = Decimal::ZERO;
            let mut sinking_sell_total = Decimal::ZERO;
            let mut income_tax_total = Decimal::ZERO;

            for i in 0..agg.order_nids.len() {
                let order_nid = agg.order_nids[i];

                if let Some(trade) = trade_map.get(&order_nid) {
                    let buy = trade.buy_amount;
                    let sell = trade.sell_amount;

                    buy_amount_total += buy;
                    sell_amount_total += sell;

                    let levy = agg.levy_percent.get(i).copied().unwrap_or(Decimal::ZERO);
                    let sinking = agg.sinking_fund_percent.get(i).copied().unwrap_or(Decimal::ZERO);
                    let tax = agg.income_tax_percent.get(i).copied().unwrap_or(Decimal::ZERO);

                    levy_buy_total += buy * levy;
                    levy_sell_total += sell * levy;

                    sinking_buy_total += buy * sinking;
                    sinking_sell_total += sell * sinking;

                    income_tax_total += sell * tax;
                }
            }
            
            out.push(ClientTrxInsert { 
                date: chrono::NaiveDateTime::parse_from_str(
                    "2026-01-15 00:00:00",
                    "%Y-%m-%d %H:%M:%S"
                ).unwrap(), 
                client_nid: agg.client_nid, 
                client_trx_n_type: agg.client_trx_n_type, 
                buy_sell: agg.c_buy_sell.to_string(), 
                stock_nid: agg.c_stock_nid, 
                order_nid: agg.c_order_nid, 
                buy_avg_price: agg.c_buy_avg_price, 
                sell_avg_price: agg.c_sell_avg_price, 
                buy_volume: agg.buy_volume, 
                sell_volume: agg.sell_volume, 
                buy_levy_percent: levy_buy_total, 
                sell_levy_percent: levy_sell_total, 
                buy_sinking_fund_percent: sinking_buy_total, 
                sell_sinking_fund_percent: sinking_sell_total, 
                income_tax_percent: income_tax_total, 
                buy_amount: buy_amount_total, 
                sell_amount: sell_amount_total, 
                net_amount: sell_amount_total - buy_amount_total, 
                due_date: agg.due_date, 
                settlement_mode: agg.c_settlement_mode, 
                sales_person_nid: agg.sales_person_nid, 
                office_nid: agg.office_nid, 
                referral_nid: agg.referral_nid, 
                commission_mode: agg.commission_mode, 
                commission_percent: agg.commission_percent, 
                buy_commission_percent: agg.buy_commission_percent, 
                sell_commission_percent: agg.sell_commission_percent, 
                vat_percent: agg.vat_percent, 
                minimum_fee: agg.minimum_fee, 
                buy_minimum_fee: agg.buy_minimum_fee, 
                sell_minimum_fee: agg.sell_minimum_fee, 
                wht_percent: agg.wht_percent, 
                referral_as_expense: agg.referral_as_expense, 
                day_trade: agg.day_trade, 
                online_trading: agg.online_trading, 
                force_buy_sell: agg.force_buy_sell, 
                stamp_duty_as_expense: agg.stamp_duty_as_expense, 
                exclude_stamp_duty_from_proceed_amount: agg.exclude_stamp_duty_from_proceed_amount, 
                entry_user_nid: 1, 
                entry_ip_address: "".to_string(), 
                entry_computer_name: "".to_string(),
             });
        }

        out
    }
 
    async fn insert_btx_fast(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[BrokerTrxInsert]) -> anyhow::Result<()> {

        if rows.is_empty() {
            return Ok(());
        }

        // ====== flatten ke vec per kolom ======
        let dates: Vec<_> = rows.iter().map(|r| r.date).collect();
        let broker_nids: Vec<_> = rows.iter().map(|r| r.broker_nid).collect();
        let trx_types: Vec<_> = rows.iter().map(|r| r.broker_trx_n_type).collect();
        let buy_sells: Vec<_> = rows.iter().map(|r| r.buy_sell.as_str()).collect();
        let stock_nids: Vec<_> = rows.iter().map(|r| r.stock_nid).collect();
        let trade_nids: Vec<_> = rows.iter().map(|r| r.trade_nid).collect();

        let buy_amounts: Vec<_> = rows.iter().map(|r| r.buy_amount).collect();
        let sell_amounts: Vec<_> = rows.iter().map(|r| r.sell_amount).collect();
        let ar_amounts: Vec<_> = rows.iter().map(|r| r.ar_amount).collect();
        let ap_amounts: Vec<_> = rows.iter().map(|r| r.ap_amount).collect();
        let net_amounts: Vec<_> = rows.iter().map(|r| r.ar_amount - r.ap_amount).collect();

        let due_dates: Vec<_> = rows.iter().map(|r| r.due_date).collect();
        let settlement_modes: Vec<_> = rows.iter().map(|r| r.settlement_mode).collect();

        let commission_percents: Vec<_> = rows.iter().map(|r| r.commission_percent).collect();
        let buy_commission_percents: Vec<_> = rows.iter().map(|r| r.buy_commission_percent).collect();
        let sell_commission_percents: Vec<_> = rows.iter().map(|r| r.sell_commission_percent).collect();

        let minimum_fees: Vec<_> = rows.iter().map(|r| r.minimum_fee).collect();
        let buy_minimum_fees: Vec<_> = rows.iter().map(|r| r.buy_minimum_fee).collect();
        let sell_minimum_fees: Vec<_> = rows.iter().map(|r| r.sell_minimum_fee).collect();

        let entry_user_nids: Vec<_> = rows.iter().map(|r| r.entry_user_nid).collect();
        let entry_ips: Vec<_> = rows.iter().map(|r| r.entry_ip_address.as_str()).collect();
        let entry_computers: Vec<_> = rows.iter().map(|r| r.entry_computer_name.as_str()).collect();

        let entry_times: Vec<_> = rows
            .iter()
            .map(|_| chrono::Utc::now().naive_utc())
            .collect();

        // ====== SQL ======
        let sql = r#"
            INSERT INTO broker_trx (
                date,
                broker_nid,
                broker_trx_n_type,
                buy_sell,
                stock_nid,
                trade_nid,
                buy_avg_price,
                sell_avg_price,
                buy_volume,
                sell_volume,
                buy_amount,
                sell_amount,
                buy_levy,
                sell_levy,
                buy_sinking_fund,
                sell_sinking_fund,
                income_tax,
                buy_fee,
                buy_vat,
                buy_commission,
                buy_wht,
                buy_charge_amount1,
                buy_charge_amount2,
                buy_charge_amount3,
                buy_charge_amount4,
                buy_charge_amount5,
                buy_charge_amount6,
                buy_charge_amount7,
                buy_charge_amount8,
                buy_charge_amount9,
                sell_fee,
                sell_vat,
                sell_commission,
                sell_wht,
                sell_charge_amount1,
                sell_charge_amount2,
                sell_charge_amount3,
                sell_charge_amount4,
                sell_charge_amount5,
                sell_charge_amount6,
                sell_charge_amount7,
                sell_charge_amount8,
                sell_charge_amount9,
                ar_amount,
                ap_amount,
                net_amount,
                cash_receive_date,
                cash_payment_date,
                stock_deliver_date,
                stock_receive_date,
                due_date,
                settlement_mode,
                commission_percent,
                buy_commission_percent,
                sell_commission_percent,
                currency_nid,
                currency_rate,
                minimum_fee,
                buy_minimum_fee,
                sell_minimum_fee,
                final,
                checked,
                approved,
                rejected,
                change_nid,
                entry_user_nid,
                entry_ip_address,
                entry_computer_name,
                entry_time
            )
            SELECT *
            FROM UNNEST(
                $1::DATE[],           
                $2::INT4[],           
                $3::INT2[],          
                $4::VARCHAR[],        
                $5::INT4[],           
                $6::INT4[],          
                $7::NUMERIC[],        
                $8::NUMERIC[],        
                $9::INT8[],           
                $10::INT8[],          
                $11::NUMERIC[],        
                $12::NUMERIC[],       
                $13::NUMERIC[],        
                $14::NUMERIC[],       
                $15::NUMERIC[],      
                $16::NUMERIC[],        
                $17::NUMERIC[],       
                $18::NUMERIC[],        
                $19::NUMERIC[],        
                $20::NUMERIC[],        
                $21::NUMERIC[],        
                $22::NUMERIC[],        
                $23::NUMERIC[],        
                $24::NUMERIC[],        
                $25::NUMERIC[],        
                $26::NUMERIC[],        
                $27::NUMERIC[],        
                $28::NUMERIC[],        
                $29::NUMERIC[],        
                $30::NUMERIC[],        
                $31::NUMERIC[],        
                $32::NUMERIC[],        
                $33::NUMERIC[],        
                $34::NUMERIC[],       
                $35::NUMERIC[],        
                $36::NUMERIC[],        
                $37::NUMERIC[],        
                $38::NUMERIC[],        
                $39::NUMERIC[],        
                $40::NUMERIC[],        
                $41::NUMERIC[],        
                $42::NUMERIC[],        
                $43::NUMERIC[],       
                $44::NUMERIC[],        
                $45::NUMERIC[],        
                $46::NUMERIC[],        
                $47::DATE[],           
                $48::DATE[],          
                $49::DATE[],           
                $50::DATE[],           
                $51::DATE[],           
                $52::INT2[],           
                $53::NUMERIC[],        
                $54::NUMERIC[],        
                $55::NUMERIC[],        
                $56::INT4[],           
                $57::NUMERIC[],        
                $58::NUMERIC[],        
                $59::NUMERIC[],        
                $60::NUMERIC[],        
                $61::BOOL[],          
                $62::BOOL[],          
                $63::BOOL[],           
                $64::BOOL[],           
                $65::INT4[],           
                $66::INT4[],           
                $67::VARCHAR[],        
                $68::VARCHAR[],        
                $69::TIMESTAMP[],      
            )
        "#;

        tx.execute(
            sql,
            &[
                &dates,
                &broker_nids,
                &trx_types,
                &buy_sells,
                &stock_nids,
                &trade_nids,
                &buy_amounts,
                &sell_amounts,
                &ar_amounts,
                &ap_amounts,
                &net_amounts,
                &due_dates,
                &settlement_modes,
                &commission_percents,
                &buy_commission_percents,
                &sell_commission_percents,
                &minimum_fees,
                &buy_minimum_fees,
                &sell_minimum_fees,
                &entry_user_nids,
                &entry_ips,
                &entry_computers,
                &entry_times,
            ],
        )
        .await?;

        Ok(())
    }

    async fn copy_btx_to_postgres(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[BrokerTrxInsert]) -> anyhow::Result<()> {
        println!("copying {} rows to broker_trx", rows.len());
        
        let sink = tx.copy_in(
            r#"
            COPY broker_trx (
                date,
                broker_nid,
                broker_trx_n_type,
                buy_sell,
                stock_nid,
                trade_nid,
                buy_avg_price,
                sell_avg_price,
                buy_volume,
                sell_volume,
                buy_amount,
                sell_amount,
                buy_levy,
                sell_levy,
                buy_sinking_fund,
                sell_sinking_fund,
                income_tax,
                buy_fee,
                buy_vat,
                buy_commission,
                buy_wht,
                buy_charge_amount1,
                buy_charge_amount2,
                buy_charge_amount3,
                buy_charge_amount4,
                buy_charge_amount5,
                buy_charge_amount6,
                buy_charge_amount7,
                buy_charge_amount8,
                buy_charge_amount9,
                sell_fee,
                sell_vat,
                sell_commission,
                sell_wht,
                sell_charge_amount1,
                sell_charge_amount2,
                sell_charge_amount3,
                sell_charge_amount4,
                sell_charge_amount5,
                sell_charge_amount6,
                sell_charge_amount7,
                sell_charge_amount8,
                sell_charge_amount9,
                ar_amount,
                ap_amount,
                net_amount,
                cash_receive_date,
                cash_payment_date,
                stock_deliver_date,
                stock_receive_date,
                due_date,
                settlement_mode,
                commission_percent,
                buy_commission_percent,
                sell_commission_percent,
                currency_nid,
                currency_rate,
                minimum_fee,
                buy_minimum_fee,
                sell_minimum_fee,
                final,
                checked,
                approved,
                rejected,
                change_nid,
                entry_user_nid,
                entry_ip_address,
                entry_computer_name,
                entry_time
            )
            FROM STDIN (FORMAT BINARY)
            "#
        ).await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::TIMESTAMP,      // 1. date
                tokio_postgres::types::Type::INT4,           // 2. broker_nid
                tokio_postgres::types::Type::INT4,           // 3. broker_trx_n_type (ubah ke INT4 karena di struct i32)
                tokio_postgres::types::Type::VARCHAR,        // 4. buy_sell
                tokio_postgres::types::Type::INT4,           // 5. stock_nid
                tokio_postgres::types::Type::INT4,           // 6. trade_nid
                tokio_postgres::types::Type::NUMERIC,        // 7. buy_avg_price
                tokio_postgres::types::Type::NUMERIC,        // 8. sell_avg_price
                tokio_postgres::types::Type::INT8,           // 9. buy_volume (ubah ke INT8 karena i64)
                tokio_postgres::types::Type::INT8,           // 10. sell_volume (ubah ke INT8 karena i64)
                tokio_postgres::types::Type::NUMERIC,        // 11. buy_amount
                tokio_postgres::types::Type::NUMERIC,        // 12. sell_amount
                tokio_postgres::types::Type::NUMERIC,        // 13. buy_levy
                tokio_postgres::types::Type::NUMERIC,        // 14. sell_levy
                tokio_postgres::types::Type::NUMERIC,        // 15. buy_sinking_fund
                tokio_postgres::types::Type::NUMERIC,        // 16. sell_sinking_fund
                tokio_postgres::types::Type::NUMERIC,        // 17. income_tax
                tokio_postgres::types::Type::NUMERIC,        // 18. buy_fee
                tokio_postgres::types::Type::NUMERIC,        // 19. buy_vat
                tokio_postgres::types::Type::NUMERIC,        // 20. buy_commission
                tokio_postgres::types::Type::NUMERIC,        // 21. buy_wht
                tokio_postgres::types::Type::NUMERIC,        // 22. buy_charge_amount1
                tokio_postgres::types::Type::NUMERIC,        // 23. buy_charge_amount2
                tokio_postgres::types::Type::NUMERIC,        // 24. buy_charge_amount3
                tokio_postgres::types::Type::NUMERIC,        // 25. buy_charge_amount4
                tokio_postgres::types::Type::NUMERIC,        // 26. buy_charge_amount5
                tokio_postgres::types::Type::NUMERIC,        // 27. buy_charge_amount6
                tokio_postgres::types::Type::NUMERIC,        // 28. buy_charge_amount7
                tokio_postgres::types::Type::NUMERIC,        // 29. buy_charge_amount8
                tokio_postgres::types::Type::NUMERIC,        // 30. buy_charge_amount9
                tokio_postgres::types::Type::NUMERIC,        // 31. sell_fee
                tokio_postgres::types::Type::NUMERIC,        // 32. sell_vat
                tokio_postgres::types::Type::NUMERIC,        // 33. sell_commission
                tokio_postgres::types::Type::NUMERIC,        // 34. sell_wht
                tokio_postgres::types::Type::NUMERIC,        // 35. sell_charge_amount1
                tokio_postgres::types::Type::NUMERIC,        // 36. sell_charge_amount2
                tokio_postgres::types::Type::NUMERIC,        // 37. sell_charge_amount3
                tokio_postgres::types::Type::NUMERIC,        // 38. sell_charge_amount4
                tokio_postgres::types::Type::NUMERIC,        // 39. sell_charge_amount5
                tokio_postgres::types::Type::NUMERIC,        // 40. sell_charge_amount6
                tokio_postgres::types::Type::NUMERIC,        // 41. sell_charge_amount7
                tokio_postgres::types::Type::NUMERIC,        // 42. sell_charge_amount8
                tokio_postgres::types::Type::NUMERIC,        // 43. sell_charge_amount9
                tokio_postgres::types::Type::NUMERIC,        // 44. ar_amount
                tokio_postgres::types::Type::NUMERIC,        // 45. ap_amount
                tokio_postgres::types::Type::NUMERIC,        // 46. net_amount
                tokio_postgres::types::Type::TIMESTAMP,      // 47. cash_receive_date
                tokio_postgres::types::Type::TIMESTAMP,      // 48. cash_payment_date
                tokio_postgres::types::Type::TIMESTAMP,      // 49. stock_deliver_date
                tokio_postgres::types::Type::TIMESTAMP,      // 50. stock_receive_date
                tokio_postgres::types::Type::TIMESTAMP,      // 51. due_date
                tokio_postgres::types::Type::INT4,           // 52. settlement_mode (ubah ke INT4 karena i32)
                tokio_postgres::types::Type::NUMERIC,        // 53. commission_percent
                tokio_postgres::types::Type::NUMERIC,        // 54. buy_commission_percent
                tokio_postgres::types::Type::NUMERIC,        // 55. sell_commission_percent
                tokio_postgres::types::Type::INT4,           // 56. currency_nid
                tokio_postgres::types::Type::NUMERIC,        // 57. currency_rate (ubah ke NUMERIC)
                tokio_postgres::types::Type::NUMERIC,        // 58. minimum_fee
                tokio_postgres::types::Type::NUMERIC,        // 59. buy_minimum_fee
                tokio_postgres::types::Type::NUMERIC,        // 60. sell_minimum_fee
                tokio_postgres::types::Type::BOOL,           // 61. final
                tokio_postgres::types::Type::BOOL,           // 62. checked
                tokio_postgres::types::Type::BOOL,           // 63. approved
                tokio_postgres::types::Type::BOOL,           // 64. rejected
                tokio_postgres::types::Type::INT4,           // 65. change_nid
                tokio_postgres::types::Type::INT4,           // 66. entry_user_nid
                tokio_postgres::types::Type::VARCHAR,        // 67. entry_ip_address
                tokio_postgres::types::Type::VARCHAR,        // 68. entry_computer_name
                tokio_postgres::types::Type::TIMESTAMP,      // 69. entry_time
            ],
        );

        let mut writer = pin!(writer);

        for r in rows {
            let entry_time = chrono::Utc::now().naive_utc();

            let params:  Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
                &r.date,                                    // 1. date
                &r.broker_nid,                              // 2. broker_nid
                &r.broker_trx_n_type,                       // 3. broker_trx_n_type
                &r.buy_sell,                                // 4. buy_sell
                &r.stock_nid,                               // 5. stock_nid
                &r.trade_nid,                               // 6. trade_nid
                &Decimal::ZERO,                             // 7. buy_avg_price
                &Decimal::ZERO,                             // 8. sell_avg_price
                &r.buy_volume,                              // 9. buy_volume (konversi eksplisit)
                &r.sell_volume,                             // 10. sell_volume (konversi eksplisit)
                &r.buy_amount,                              // 11. buy_amount
                &r.sell_amount,                             // 12. sell_amount
                &Decimal::ZERO,                             // 13. buy_levy
                &Decimal::ZERO,                             // 14. sell_levy
                &Decimal::ZERO,                             // 15. buy_sinking_fund
                &Decimal::ZERO,                             // 16. sell_sinking_fund
                &Decimal::ZERO,                             // 17. income_tax
                &Decimal::ZERO,                             // 18. buy_fee
                &Decimal::ZERO,                             // 19. buy_vat
                &Decimal::ZERO,                             // 20. buy_commission
                &Decimal::ZERO,                             // 21. buy_wht
                &Decimal::ZERO,                             // 22. buy_charge_amount1
                &Decimal::ZERO,                             // 23. buy_charge_amount2
                &Decimal::ZERO,                             // 24. buy_charge_amount3
                &Decimal::ZERO,                             // 25. buy_charge_amount4
                &Decimal::ZERO,                             // 26. buy_charge_amount5
                &Decimal::ZERO,                             // 27. buy_charge_amount6
                &Decimal::ZERO,                             // 28. buy_charge_amount7
                &Decimal::ZERO,                             // 29. buy_charge_amount8
                &Decimal::ZERO,                             // 30. buy_charge_amount9
                &Decimal::ZERO,                             // 31. sell_fee
                &Decimal::ZERO,                             // 32. sell_vat
                &Decimal::ZERO,                             // 33. sell_commission
                &Decimal::ZERO,                             // 34. sell_wht
                &Decimal::ZERO,                             // 35. sell_charge_amount1
                &Decimal::ZERO,                             // 36. sell_charge_amount2
                &Decimal::ZERO,                             // 37. sell_charge_amount3
                &Decimal::ZERO,                             // 38. sell_charge_amount4
                &Decimal::ZERO,                             // 39. sell_charge_amount5
                &Decimal::ZERO,                             // 40. sell_charge_amount6
                &Decimal::ZERO,                             // 41. sell_charge_amount7
                &Decimal::ZERO,                             // 42. sell_charge_amount8
                &Decimal::ZERO,                             // 43. sell_charge_amount9
                &r.ar_amount,                               // 44. ar_amount
                &r.ap_amount,                               // 45. ap_amount
                &r.net_amount,                              // 46. net_amount
                &r.due_date,                                // 47. cash_receive_date
                &r.due_date,                                // 48. cash_payment_date
                &r.due_date,                                // 49. stock_deliver_date
                &r.due_date,                                // 50. stock_receive_date
                &r.due_date,                                // 51. due_date
                &r.settlement_mode,                         // 52. settlement_mode
                &r.commission_percent,                      // 53. commission_percent
                &r.buy_commission_percent,                  // 54. buy_commission_percent
                &r.sell_commission_percent,                 // 55. sell_commission_percent
                &1i32,                                      // 56. currency_nid
                &Decimal::ONE,                              // 57. currency_rate (ubah ke Decimal)
                &r.minimum_fee,                             // 58. minimum_fee
                &r.buy_minimum_fee,                         // 59. buy_minimum_fee
                &r.sell_minimum_fee,                        // 60. sell_minimum_fee
                &true,                                      // 61. final
                &false,                                     // 62. checked
                &false,                                     // 63. approved
                &false,                                     // 64. rejected
                &0i32,                                      // 65. change_nid
                &r.entry_user_nid,                          // 66. entry_user_nid
                &r.entry_ip_address,                        // 67. entry_ip_address
                &r.entry_computer_name,                     // 68. entry_computer_name
                &entry_time,                                // 69. entry_time
            ];
            
           writer.as_mut().write(&params).await?;
        }

        match writer.as_mut().finish().await {
            Ok(_) => (),
            Err(e) => {
                println!("Failed to insert finish broker_trx: {:?}", e);
                return Err(anyhow::anyhow!(e))
            },
        }
        Ok(())
    }

    async fn copy_ctx_to_postgres(&self, tx: &tokio_postgres::Transaction<'_>, rows: &[ClientTrxInsert]) -> anyhow::Result<()> {

        println!("copying {} rows to client_trx", rows.len());
        let sink = tx.copy_in(
            r#"
            COPY client_trx (
                date,
                client_nid,
                client_trx_n_type,
                buy_sell,
                stock_nid,
                order_nid,
                buy_avg_price,
                sell_avg_price,
                buy_volume,
                sell_volume,
                buy_amount,
                sell_amount,
                buy_levy,
                sell_levy,
                buy_sinking_fund,
                sell_sinking_fund,
                income_tax,
                buy_fee,
                buy_vat,
                buy_wapu_vat,
                buy_wht,
                buy_otc_fee,
                buy_referral,
                buy_other_charges,
                sell_fee,
                sell_vat,
                sell_wapu_vat,
                sell_wht,
                sell_otc_fee,
                sell_referral,
                sell_other_charges,
                ar_amount,
                ap_amount,
                net_amount,
                cash_receive_date,
                cash_payment_date,
                stock_deliver_date,
                stock_receive_date,
                ar_due_date,
                ap_due_date,
                due_date,
                settlement_mode,
                sales_person_nid,
                office_nid,
                referral_nid,
                commission_mode,
                commission_percent,
                buy_commission_percent,
                sell_commission_percent,
                vat_percent,
                currency_nid,
                currency_rate,
                minimum_fee,
                buy_minimum_fee,
                sell_minimum_fee,
                wht_percent,
                referral_percent,
                referral_as_expense,
                settle_currency_nid,
                settle_currency_rate,
                day_trade,
                online_trading,
                force_buy_sell,
                stamp_duty_as_expense,
                exclude_stamp_duty_from_proceed_amount,
                final,
                full_trades,
                checked,
                approved,
                rejected,
                change_nid,
                entry_user_nid,
                entry_ip_address,
                entry_computer_name,
                entry_time
            )
            FROM STDIN (FORMAT BINARY)
            "#
        ).await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::TIMESTAMP,             // 1. date
                tokio_postgres::types::Type::INT4,                  // 2. client_nid
                tokio_postgres::types::Type::INT4,                  // 3. client_trx_n_type
                tokio_postgres::types::Type::VARCHAR,               // 4. buy_sell
                tokio_postgres::types::Type::INT4,                  // 5. stock_nid
                tokio_postgres::types::Type::INT4,                  // 6. order_nid
                tokio_postgres::types::Type::NUMERIC,               // 7. buy_avg_price
                tokio_postgres::types::Type::NUMERIC,               // 8. sell_avg_price
                tokio_postgres::types::Type::INT8,                  // 9. buy_volume
                tokio_postgres::types::Type::INT8,                  // 10. sell_volume
                tokio_postgres::types::Type::NUMERIC,               // 11. buy_amount
                tokio_postgres::types::Type::NUMERIC,               // 12. sell_amount
                tokio_postgres::types::Type::NUMERIC,               // 13. buy_levy
                tokio_postgres::types::Type::NUMERIC,               // 14. sell_levy
                tokio_postgres::types::Type::NUMERIC,               // 15. buy_sinking_fund
                tokio_postgres::types::Type::NUMERIC,               // 16. sell_sinking_fund
                tokio_postgres::types::Type::NUMERIC,               // 17. income_tax
                tokio_postgres::types::Type::NUMERIC,               // 18. buy_fee
                tokio_postgres::types::Type::NUMERIC,               // 19. buy_vat
                tokio_postgres::types::Type::NUMERIC,               // 20. buy_wapu_vat
                tokio_postgres::types::Type::NUMERIC,               // 21. buy_wht
                tokio_postgres::types::Type::NUMERIC,               // 22. buy_otc_fee
                tokio_postgres::types::Type::NUMERIC,               // 23. buy_referral
                tokio_postgres::types::Type::NUMERIC,               // 24. buy_other_charges
                tokio_postgres::types::Type::NUMERIC,               // 25. sell_fee
                tokio_postgres::types::Type::NUMERIC,               // 26. sell_vat
                tokio_postgres::types::Type::NUMERIC,               // 27. sell_wapu_vat
                tokio_postgres::types::Type::NUMERIC,               // 28. sell_wht
                tokio_postgres::types::Type::NUMERIC,               // 29. sell_otc_fee
                tokio_postgres::types::Type::NUMERIC,               // 30. sell_referral
                tokio_postgres::types::Type::NUMERIC,               // 31. sell_other_charges
                tokio_postgres::types::Type::NUMERIC,               // 32. ar_amount
                tokio_postgres::types::Type::NUMERIC,               // 33. ap_amount
                tokio_postgres::types::Type::NUMERIC,               // 34. net_amount
                tokio_postgres::types::Type::TIMESTAMP,             // 35. cash_receive_date
                tokio_postgres::types::Type::TIMESTAMP,             // 36. cash_payment_date
                tokio_postgres::types::Type::TIMESTAMP,             // 37. stock_deliver_date
                tokio_postgres::types::Type::TIMESTAMP,             // 38. stock_receive_date
                tokio_postgres::types::Type::TIMESTAMP,             // 39. ar_due_date
                tokio_postgres::types::Type::TIMESTAMP,             // 40. ap_due_date
                tokio_postgres::types::Type::TIMESTAMP,             // 41. due_date
                tokio_postgres::types::Type::INT4,                  // 42. settlement_mode
                tokio_postgres::types::Type::INT4,                  // 43. sales_person_nid
                tokio_postgres::types::Type::INT4,                  // 44. office_nid
                tokio_postgres::types::Type::INT4,                  // 45. referral_nid
                tokio_postgres::types::Type::INT4,                  // 46. commission_mode
                tokio_postgres::types::Type::NUMERIC,               // 47. commission_percent
                tokio_postgres::types::Type::NUMERIC,               // 48. buy_commission_percent
                tokio_postgres::types::Type::NUMERIC,               // 49. sell_commission_percent
                tokio_postgres::types::Type::NUMERIC,               // 50. vat_percent
                tokio_postgres::types::Type::INT4,                  // 51. currency_nid
                tokio_postgres::types::Type::NUMERIC,               // 52. currency_rate
                tokio_postgres::types::Type::NUMERIC,               // 53. minimum_fee
                tokio_postgres::types::Type::NUMERIC,               // 54. buy_minimum_fee
                tokio_postgres::types::Type::NUMERIC,               // 55. sell_minimum_fee
                tokio_postgres::types::Type::NUMERIC,               // 56. wht_percent
                tokio_postgres::types::Type::NUMERIC,               // 57. referral_percent
                tokio_postgres::types::Type::BOOL,                  // 58. referral_as_expense
                tokio_postgres::types::Type::INT4,                  // 59. settle_currency_nid
                tokio_postgres::types::Type::NUMERIC,               // 60. settle_currency_rate
                tokio_postgres::types::Type::BOOL,                  // 61. day_trade
                tokio_postgres::types::Type::BOOL,                  // 62. online_trading
                tokio_postgres::types::Type::BOOL,                  // 63. force_buy_sell
                tokio_postgres::types::Type::BOOL,                  // 64. stamp_duty_as_expense
                tokio_postgres::types::Type::BOOL,                  // 65. exclude_stamp_duty_from_proceed_amount
                tokio_postgres::types::Type::BOOL,                  // 66. final
                tokio_postgres::types::Type::BOOL,                  // 67. full_trades
                tokio_postgres::types::Type::BOOL,                  // 68. checked
                tokio_postgres::types::Type::BOOL,                  // 69. approved
                tokio_postgres::types::Type::BOOL,                  // 70. rejected
                tokio_postgres::types::Type::INT4,                  // 71. change_nid
                tokio_postgres::types::Type::INT4,                  // 72. entry_user_nid
                tokio_postgres::types::Type::VARCHAR,               // 73. entry_ip_address
                tokio_postgres::types::Type::VARCHAR,               // 74. entry_computer_name
                tokio_postgres::types::Type::TIMESTAMP,             // 75. entry_time
            ],
        );

        let mut writer = pin!(writer);
        
        let entry_time = chrono::Utc::now().naive_utc();

        for r in rows {

            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
                &r.date,                                            // 1. date
                &r.client_nid,                                      // 2. client_nid
                &r.client_trx_n_type,                               // 3. client_trx_n_type
                &r.buy_sell,                                        // 4. buy_sell
                &r.stock_nid,                                       // 5. stock_nid
                &r.order_nid,                                       // 6. order_nid
                &r.buy_avg_price,                                   // 7. buy_avg_price
                &r.sell_avg_price,                                  // 8. sell_avg_price
                &r.buy_volume,                                      // 9. buy_volume
                &r.sell_volume,                                     // 10. sell_volume
                &r.buy_amount,                                      // 11. buy_amount
                &r.sell_amount,                                     // 12. sell_amount
                &r.buy_levy_percent,                                // 13. buy_levy_percent
                &r.sell_levy_percent,                               // 14. sell_levy_percent
                &r.buy_sinking_fund_percent,                        // 15. buy_sinking_fund_percent
                &r.sell_sinking_fund_percent,                       // 16. sell_sinking_fund_percent
                &r.income_tax_percent,                              // 17. income_tax_percent
                &Decimal::ZERO,                                     // 18. buy_fee
                &Decimal::ZERO,                                     // 19. buy_vat
                &Decimal::ZERO,                                     // 20. buy_wapu_vat
                &Decimal::ZERO,                                     // 21. buy_wht
                &Decimal::ZERO,                                     // 22. buy_otc_fee
                &Decimal::ZERO,                                     // 23. buy_referral
                &Decimal::ZERO,                                     // 24. buy_other_charges
                &Decimal::ZERO,                                     // 25. sell_fee
                &Decimal::ZERO,                                     // 26. sell_vat
                &Decimal::ZERO,                                     // 27. sell_wapu_vat
                &Decimal::ZERO,                                     // 28. sell_wht
                &Decimal::ZERO,                                     // 29. sell_otc_fee
                &Decimal::ZERO,                                     // 30. sell_referral
                &Decimal::ZERO,                                     // 31. sell_other_charges
                &r.buy_amount,                                      // 32. buy_amount
                &r.sell_amount,                                     // 33. sell_amount
                &r.net_amount,                                      // 34. net_amount
                &r.due_date,                                        // 35. cash_receive_date
                &r.due_date,                                        // 36. cash_payment_date
                &r.due_date,                                        // 37. stock_deliver_date
                &r.due_date,                                        // 38. stock_receive_date
                &r.due_date,                                        // 39. ar_due_date
                &r.due_date,                                        // 40. ap_due_date
                &r.due_date,                                        // 41. due_date
                &r.settlement_mode,                                 // 42. settlement_mode
                &r.sales_person_nid,                                // 43. sales_person_nid
                &r.office_nid,                                      // 44. office_nid
                &r.referral_nid,                                    // 45. referral_nid
                &r.commission_mode,                                 // 46. commission_mode
                &r.commission_percent,                              // 47. commission_percent
                &r.buy_commission_percent,                          // 48. buy_commission_percent
                &r.sell_commission_percent,                         // 49. sell_commission_percent
                &r.vat_percent,                                     // 50. vat_percent
                &1i32,                                              // 51. currency_nid
                &Decimal::ZERO,                                     // 52. currency_rate
                &r.minimum_fee,                                     // 53. minimum_fee
                &r.buy_minimum_fee,                                 // 54. buy_minimum_fee
                &r.sell_minimum_fee,                                // 55. sell_minimum_fee
                &r.wht_percent,                                     // 56. wht_percent
                &Decimal::ZERO,                                     // 57. referral_percent
                &r.referral_as_expense,                             // 58. referral_as_expense
                &1i32,                                              // 59. settle_currency_nid
                &Decimal::ZERO,                                     // 60. settle_currency_rate
                &r.day_trade,                                       // 61. day_trade
                &r.online_trading,                                  // 62. online_trading
                &r.force_buy_sell,                                  // 63. force_buy_sell
                &r.stamp_duty_as_expense,                           // 64. stamp_duty_as_expense
                &r.exclude_stamp_duty_from_proceed_amount,          // 65. exclude_stamp_duty_from_proceed_amount
                &true,                                              // 66. final
                &false,                                             // 67. full_trades
                &false,                                             // 68. checked
                &false,                                             // 69. approved
                &false,                                             // 70. rejected
                &0i32,                                              // 71. change_nid
                &r.entry_user_nid,                                  // 72. entry_user_nid
                &r.entry_ip_address,                                // 73. entry_ip_address
                &r.entry_computer_name,                             // 74. entry_computer_name
                &entry_time,                                        // 75. entry_time
            ];

            writer.as_mut().write(&params).await?;
        }

        match writer.finish().await {
            Ok(_) => (),
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(anyhow::anyhow!(e));
            },
        };
        Ok(())
    }

    async fn insert_sales_person(&self, sales: &Vec<SalesPerson>) -> Result<()> {
        let mut conn = self.conn.get().await?;
        let tx = conn.transaction().await?;

        let sink = tx.copy_in("COPY sales (
            sales_id,
            sales_name,
            price,
            expired_date
        ) FROM STDIN (FORMAT BINARY)").await?;

        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::TIMESTAMP,
            ],
        );

        let mut writer = pin!(writer);

        for s in sales {
            writer.as_mut().write(&[
                &s.sales_id,
                &s.sales_name,
                &s.price,
                &s.expired_date,
            ]).await?;
        }

        writer.finish().await?;

        tx.commit().await?;
        Ok(())
    }

}
