use async_trait::async_trait;
use anyhow::Result;
use deadpool_postgres::Pool;
use rust_decimal::Decimal;

use crate::applications::{
    services::order::domain::order_entity::{
        BrokerTrxConfig, BrokerTrxEntity, BrokerTrxState, ClientTrxState, OrderEntity, ProcessingConfig
    }, 
    contracts::order_trait::TOrderRepository
};

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

    async fn match_orders(&self, config: &ProcessingConfig, order: &OrderEntity, btx: &mut BrokerTrxState, ctrx: & mut ClientTrxState,  order_ids: &[i32]) -> Result<()> {
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

    async fn match_orders_async(&self) -> Result<String> {
        let mut conn = self.conn.get().await?;

        let mut last_id: i32 = 0;
        let limit: i32 = 100_000;

        loop {
            let mut tx = conn.transaction().await?;
            let orders = self.get_order_list_async(&mut tx, last_id, limit).await?;
            if orders.is_empty() {
                break;
            }

            for order in orders {
                let order_nid: i32 = order.get("order_nid");
                println!("Processing order_nid: {}", order_nid);
                // Add your processing logic here
                last_id = order_nid;
            }

            tx.commit().await?;
        }
        
        Ok("async job started".into())
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
}
