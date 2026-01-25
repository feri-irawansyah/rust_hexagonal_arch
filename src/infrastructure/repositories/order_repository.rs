use async_trait::async_trait;
use anyhow::Result;
use deadpool_postgres::Pool;
use rust_decimal::Decimal;

use crate::{applications::{
    contracts::order_trait::TOrderRepository, services::order::domain::order_entity::{
        BrokerTrxConfig, BrokerTrxEntity, BrokerTrxState, ClientTrxState, MatchResult, OrderDone, OrderEntity, PartialMatch, ProcessingConfig
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

        // 2️⃣ PARTIAL MATCH
        for chunk in result.partial.chunks(CHUNK) {
            self.update_trade_partial(&mut tx, chunk).await?;
            self.insert_trade_remainder(&mut tx, chunk).await?;
        }

        // 3️⃣ ORDER MATCH
        for chunk in result.order_done.chunks(CHUNK) {
            self.update_order_match(&mut tx, chunk).await?;
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
            select
                o.order_nid,
                c.client_nid,
                o.buy_sell,
                o.stock_nid,
                o.market_order_id,
                fb.broker_trx_n_type,
                fb.trx_mode,
                fb.trx_net,
                case
                    when b.board_type = 0 then m.clearing_nid
                    else o.broker_nid
                end as broker_nid,
                case
                    when fb.trx_net = 0 then o.buy_sell
                    else 'n'
                end as b_buy_sell,
                case
                    when fb.trx_mode = 2 then 0
                    else 0
                end as b_trade_nid,
                coalesce(o.settlement_mode, r.settlement_mode) as settlement_mode,
                case
                    when fb.trx_mode >= 1 then o.stock_nid
                    else null
                end as b_stock_nid,
                bct.commission_percent,
                bct.buy_commission_percent,
                bct.sell_commission_percent,
                bct.minimum_fee,
                bct.buy_minimum_fee,
                bct.sell_minimum_fee,
                sum(o.order_volume) as order_volume,
                0::numeric as done_volume,
                sum(case when o.buy_sell = 'b' then o.order_volume else 0 end) as buy_volume,
                sum(case when o.buy_sell = 's' then o.order_volume else 0 end) as sell_volume

            from read_parquet('snapshot_order.parquet') o
                join read_parquet('snapshot_client.parquet') c on c.client_nid = o.client_nid
                join read_parquet('snapshot_board.parquet') b on b.board_nid = o.board_nid
                join read_parquet('snapshot_market.parquet') m on m.market_nid = o.market_nid
                join read_parquet('snapshot_broker.parquet') r
                    on r.broker_nid =
                    case
                        when b.board_type = 0 then m.clearing_nid
                        else o.broker_nid
                    end

                cross join lateral (
                    SELECT DISTINCT
                        a.broker_trx_n_type,
                        bct.trx_mode,
                        bct.trx_net,
                        bct.cash_payment,
                        bct.cash_receive,
                        bct.stock_deliver,
                        bct.stock_receive
                    FROM read_parquet('snapshot_trx_type_match.parquet') a
                    JOIN read_parquet('snapshot_broker_commission_trx.parquet') bct
                        ON a.broker_trx_n_type = bct.broker_trx_n_type
                    JOIN read_parquet('snapshot_broker.parquet') c2
                        ON bct.broker_commission_nid = c2.broker_commission_nid
                    WHERE a.order_trx_n_type = o.trx_n_type
                    AND a.trade_trx_n_type = o.trx_n_type
                    AND c2.broker_nid =
                        CASE
                            WHEN b.board_type = 0 THEN m.clearing_nid
                            ELSE o.broker_nid
                        END
                    AND a.ng_report = o.ng_report
                    AND a.day_trade = o.day_trade
                    AND a.online_trading = o.online_trading
                ) fb
                join read_parquet('snapshot_broker_commission_trx.parquet') bct
                    on bct.broker_commission_nid = r.broker_commission_nid
                and bct.broker_trx_n_type = fb.broker_trx_n_type
            where
                o.order_date = '2026-01-15'
                and o.order_status in ('O', 'P')
            group by
                o.order_nid,
                c.client_nid,
                o.buy_sell,
                o.stock_nid,
                o.market_order_id,
                fb.broker_trx_n_type,
                fb.trx_mode,
                fb.trx_net,
                case when b.board_type = 0 then m.clearing_nid else o.broker_nid end,
                case when fb.trx_net = 0 then o.buy_sell else 'n' end,
                case when fb.trx_mode = 2 then 0 else 0 end,
                coalesce(o.settlement_mode, r.settlement_mode),
                case when fb.trx_mode >= 1 then o.stock_nid else null end,
                bct.commission_percent,
                bct.buy_commission_percent,
                bct.sell_commission_percent,
                bct.minimum_fee,
                bct.buy_minimum_fee,
                bct.sell_minimum_fee;
        "#, []);
        
        match result {
            Ok(_) => Ok("Temp Broker Trx created".to_string()),
            Err(e) => Err(e.into()),
        }
    }

    fn create_ctx_temp(&self, snapshot: &SnapshotDb) -> Result<String, anyhow::Error> { 
        let result = snapshot.conn().execute(r#"
            CREATE OR REPLACE TABLE client_trx_temp AS
            select
            o.order_nid,
            c.client_nid,
            o.buy_sell,
            o.stock_nid,
            o.market_order_id,
            fc.client_trx_n_type,
            fc.client_trx_net,
            fc.client_trx_mode,
            coalesce(o.commission_mode, cct.commission_mode) as commission_mode,
            case fc.client_trx_net
                when 0 then o.buy_sell
                else 'n'
            end as c_buy_sell,
            case
                when fc.client_trx_mode >= 1 then o.stock_nid
                else null
            end as c_stock_nid,
            case
                when fc.client_trx_mode = 2 then o.order_nid
                else null
            end as c_order_nid,
            coalesce(o.settlement_mode, c.settlement_mode) as c_settlement_mode,
            fc.cash_payment,
            fc.cash_receive,
            fc.stock_deliver,
            fc.stock_receive,
            fc.ar_due_time,
            fc.ap_due_time,
        --    case
        --        when b.board_type = 0 then
        --            public.f_get_due_date(m.market_nid, o.order_date, fc.cash_payment)
        --        else
        --            case
        --                when o.settlement_date is null then
        --                    public.f_get_due_date(m.market_nid, o.order_date, fc.cash_payment)
        --                else o.settlement_date
        --            end
        --    end as due_date,
            case
                when fc.client_trx_mode = 0 then fc.levy_percent_cct
                else fc.levy_percent
            end as levy_percent,
            case
                when fc.client_trx_mode = 0 then fc.sinking_fund_percent_cct
                else fc.sinking_fund_percent
            end as sinking_fund_percent,
            case
                when fc.client_trx_mode = 0 then fc.income_tax_percent_cct
                else fc.income_tax_percent
            end as income_tax_percent,
            coalesce(
                o.commission_percent,
                coalesce(fc.t_commission_percent, cct.commission_percent)
            ) as commission_percent,
            coalesce(
                o.commission_percent,
                coalesce(fc.t_commission_percent, cct.buy_commission_percent)
            ) as buy_commission_percent,
            coalesce(
                o.commission_percent,
                coalesce(fc.t_commission_percent, cct.sell_commission_percent)
            ) as sell_commission_percent,
            cct.vat_percent,
            cct.minimum_fee,
            cct.buy_minimum_fee,
            cct.sell_minimum_fee,
            cct.wht_percent,
            cct.referral_as_expense,
            case
                when fc.client_trx_mode = 3 and o.buy_sell = 'b'
                    then 0
                else 0
            end as c_buy_avg_price,
            case
                when fc.client_trx_mode = 3 and o.buy_sell = 's'
                    then 0
                else 0
            end as c_sell_avg_price,
            o.day_trade,
            o.online_trading,
            o.force_buy_sell,
            c.stamp_duty_as_expense,
            c.exclude_stamp_duty_from_proceed_amount,
            coalesce(o.office_nid, c.office_nid) as office_nid,
            c.sales_person_nid,
            c.referral_nid,
            sum(o.order_volume) as order_volume,
            0::numeric as done_volume,
            sum(case when o.buy_sell = 'b' then o.order_volume else 0 end) as buy_volume,
            sum(case when o.buy_sell = 's' then o.order_volume else 0 end) as sell_volume

        from read_parquet('snapshot_order.parquet') o
            join read_parquet('snapshot_client.parquet') c on c.client_nid = o.client_nid
            join read_parquet('snapshot_board.parquet') b on b.board_nid = o.board_nid
            join read_parquet('snapshot_market.parquet') m on m.market_nid = o.market_nid
            CROSS JOIN LATERAL (
            SELECT DISTINCT
                a.client_trx_n_type,
                b.trx_net              AS client_trx_net,
                b.trx_mode             AS client_trx_mode,
                b.cash_payment,
                b.cash_receive,
                b.stock_deliver,
                b.stock_receive,
                b.ar_due_time,
                b.ap_due_time,
                b.levy_percent,
                b.sinking_fund_percent,
                b.income_tax_percent,
                tt.levy_percent        AS levy_percent_cct,
                tt.sinking_fund_percent AS sinking_fund_percent_cct,
                tt.income_tax_percent  AS income_tax_percent_cct,
                b.commission_percent   AS t_commission_percent
            FROM read_parquet('snapshot_trx_type_match.parquet') a
            JOIN read_parquet('snapshot_client_commission_trx.parquet') b
                ON a.client_trx_n_type = b.client_trx_n_type
            JOIN read_parquet('snapshot_client.parquet') c2
                ON b.client_commission_nid = c2.client_commission_nid
            JOIN read_parquet('snapshot_trx_type.parquet') tt
                ON tt.trx_n_type = a.order_trx_n_type
            WHERE
                a.order_trx_n_type = o.trx_n_type
                AND a.trade_trx_n_type = o.trx_n_type
                AND c2.client_nid = o.client_nid
                AND a.ng_report = o.ng_report
                AND a.day_trade = o.day_trade
                AND a.online_trading = o.online_trading
                AND a.criteria1 = FALSE
                AND a.criteria2 = FALSE
                AND a.criteria3 = FALSE
                AND a.criteria4 = FALSE
                AND a.criteria5 = FALSE
        ) fc
            join read_parquet('snapshot_client_commission_trx.parquet') cct
                on cct.client_commission_nid = c.client_commission_nid
            and cct.client_trx_n_type = fc.client_trx_n_type
        where
            o.order_date = '2026-01-15'
            and o.order_status in ('O', 'P')
        group by
            o.order_nid,
            c.client_nid,
            o.buy_sell,
            o.stock_nid,
            o.market_order_id,
            fc.client_trx_n_type,
            fc.client_trx_net,
            fc.client_trx_mode,
            coalesce(o.commission_mode, cct.commission_mode),
            case fc.client_trx_net when 0 then o.buy_sell else 'n' end,
            case when fc.client_trx_mode >= 1 then o.stock_nid else null end,
            case when fc.client_trx_mode = 2 then o.order_nid else null end,
            coalesce(o.settlement_mode, c.settlement_mode),
            fc.cash_payment,
            fc.cash_receive,
            fc.stock_deliver,
            fc.stock_receive,
            fc.ar_due_time,
            fc.ap_due_time,
        --    case
        --        when b.board_type = 0 then
        --            public.f_get_due_date(m.market_nid, o.order_date, fc.cash_payment)
        --        else
        --            case
        --                when o.settlement_date is null then
        --                    public.f_get_due_date(m.market_nid, o.order_date, fc.cash_payment)
        --                else o.settlement_date
        --            end
        --    end,
            case when fc.client_trx_mode = 0 then fc.levy_percent_cct else fc.levy_percent end,
            case when fc.client_trx_mode = 0 then fc.sinking_fund_percent_cct else fc.sinking_fund_percent end,
            case when fc.client_trx_mode = 0 then fc.income_tax_percent_cct else fc.income_tax_percent end,
            coalesce(o.commission_percent, coalesce(fc.t_commission_percent, cct.commission_percent)),
            coalesce(o.commission_percent, coalesce(fc.t_commission_percent, cct.buy_commission_percent)),
            coalesce(o.commission_percent, coalesce(fc.t_commission_percent, cct.sell_commission_percent)),
            cct.vat_percent,
            cct.minimum_fee,
            cct.buy_minimum_fee,
            cct.sell_minimum_fee,
            cct.wht_percent,
            cct.referral_as_expense,
            case when fc.client_trx_mode = 3 and o.buy_sell = 'b' then 0 else 0 end,
            case when fc.client_trx_mode = 3 and o.buy_sell = 's' then 0 else 0 end,
            o.day_trade,
            o.online_trading,
            o.force_buy_sell,
            c.stamp_duty_as_expense,
            c.exclude_stamp_duty_from_proceed_amount,
            coalesce(o.office_nid, c.office_nid),
            c.sales_person_nid,
            c.referral_nid;
        "#, []);
        
        match result {
            Ok(_) => Ok("Temp Broker Trx created".to_string()),
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

    fn get_broker_trx_done(&self, snapshot: &SnapshotDb) -> Result<Vec<serde_json::Value>, anyhow::Error> {

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
            Ok(serde_json::json!({
                "date": row.get::<_, String>(0)?,
                "broker_nid": row.get::<_, i32>(1)?,
                "broker_trx_n_type": row.get::<_, i32>(2)?,
                "buy_sell": row.get::<_, String>(3)?,
                "stock_nid": row.get::<_, Option<i32>>(4)?,
                "trade_nid": row.get::<_, Option<i32>>(5)?,
                "buy_avg_price": row.get::<_, i32>(6)?,
                "sell_avg_price": row.get::<_, i32>(7)?,
                "buy_volume": row.get::<_, i64>(8)?,
                "sell_volume": row.get::<_, i64>(9)?,
                "buy_amount": row.get::<_, f64>(10)?,
                "sell_amount": row.get::<_, f64>(11)?,
                "net_amount": row.get::<_, f64>(12)?,
                "due_date": row.get::<_, String>(13)?,
                "settlement_mode": row.get::<_, Option<i32>>(14)?,
                "commission_percent": row.get::<_, Option<f64>>(15)?,
                "buy_commission_percent": row.get::<_, Option<f64>>(16)?,
                "sell_commission_percent": row.get::<_, Option<f64>>(17)?,
                "minimum_fee": row.get::<_, Option<f64>>(18)?,
                "buy_minimum_fee": row.get::<_, Option<f64>>(19)?,
                "sell_minimum_fee": row.get::<_, Option<f64>>(20)?,
                "entry_time": row.get::<_, String>(21)?
            }))
        })?;

        let mut result = Vec::new();
        for r in rows {
            result.push(r?);
        }

        Ok(result)
    }

}
