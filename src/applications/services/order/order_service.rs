use std::{str::FromStr, sync::Arc};
use anyhow::Result;
use rust_decimal::Decimal;
use tokio::{sync::mpsc, time::Instant};

use crate::{
    applications::{
        services::order::domain::order_entity::{BrokerTrxState, ClientTrxState, OrderEntity, ProcessingConfig}, 
        contracts::order_trait::TOrderRepository
    }, 
    infrastructure::database::snapshot::SnapshotDb
};

pub struct OrderService {
    repo: Arc<dyn TOrderRepository>,
    config: ProcessingConfig,
}

impl OrderService {
    pub fn new(repo: Arc<dyn TOrderRepository>) -> Self {
        Self { repo, config: ProcessingConfig::default() }
    }

    pub async fn get_views(&self) -> Result<Vec<serde_json::Value>> {
        let rows = self.repo.get_views().await?;
        let mut result = Vec::new();

        for row in rows {
            let trader_id = row.get::<_, String>("trader_id");
            let json_value = serde_json::json!({"trader_id": trader_id});
            result.push(json_value);
        }

        Ok(result)
    }

    pub async fn load_match_data(&self) -> Result<String> {
        let snapshot = SnapshotDb::new()?;
        let tables = vec![
                ("trade", None),
                ("setup", None),
                ("setup_contracting", None),
                ("board", None),
                ("market", None),
                ("client", None),
                ("broker", None),
                ("order", None),
            ];

            for (table, where_clause) in tables {
                let start = Instant::now();
                if let Err(e) = snapshot.load_data_from_db(table, where_clause) {
                    eprintln!("[JOB] snapshot failed table={}: {}", table, e);
                    continue;
                }
                println!("[JOB] snapshot table={} done in {:?}", table, start.elapsed());
            }
        Ok("Load match data completed".to_string())
    }

    pub async fn match_orders(&self) -> Result<String> {
        let repo = self.repo.clone();
        let repo_clone = repo.clone();
        let config = self.config.clone();

        // 1️⃣ channel kirim order_nid dari DuckDB ke Postgres
        tokio::spawn(async move {
            let mut broker_state = BrokerTrxState::default();
            let mut client_state = ClientTrxState::default();
            let order_state = OrderEntity::default();

            let (tx, mut rx) = mpsc::channel::<OrderEntity>(10_000);

            let pg_handle = tokio::spawn(async move {
                let mut batch: Vec<i32> = Vec::with_capacity(10_000);
                while let Some(order) = rx.recv().await {

                    broker_state.prev_broker_nid = order.broker_nid;

                    batch.push(order.order_nid);
                    if batch.len() >= 10_000 {
                        if let Err(e) = repo_clone.match_orders(&config, &order, &mut broker_state, &mut client_state, &batch).await {
                            eprintln!("[JOB] update batch failed: {:?}", e);
                            break;
                        }
                        batch.clear();
                    }
                }

                if !batch.is_empty() {
                    if let Err(e) = repo_clone.match_orders(&config, &order_state, &mut broker_state, &mut client_state, &batch).await {
                        eprintln!("[JOB] update batch failed: {:?}", e);
                        return;
                    }
                }

                println!("[JOB] All orders processed, channel closed.");
            });

            // 3️⃣ spawn blocking task untuk DuckDB snapshot
            let tx_clone = tx.clone();
            let duck_handle = tokio::task::spawn_blocking(move || {
                let job_start = Instant::now();
                println!("[JOB] match_orders started (DuckDB)");

                let snapshot = SnapshotDb::new().unwrap();

                // kirim order_nid ke channel
                let mut last_order_nid: i32 = 0;
                let batch_size = 10_000;
                loop {
                    let mut stmt = snapshot.conn().prepare(
                        r#"
                        SELECT * FROM read_parquet('snapshot_order.parquet')
                        WHERE order_date = '2026-01-15'
                        AND currency_nid = 1
                        AND market_order_id > 0
                        AND (order_volume - done_volume) > 0
                        AND order_status IN ('O', 'P')
                        AND order_nid > ?
                        ORDER BY order_nid
                        LIMIT ?
                        "#
                    ).unwrap();

                    let mut rows = stmt.query(&[&last_order_nid, &batch_size]).unwrap();
                    let mut batch_count = 0;

                    while let Ok(Some(row)) = rows.next() {
                        let entity = OrderEntity {
                            order_nid: row.get("order_nid").unwrap_or_default(),
                            order_date: row.get("order_date").unwrap_or_default(),
                            settlement_date: row.get("settlement_date").unwrap_or_default(),
                            market_nid: row.get("market_nid").unwrap_or_default(),
                            board_nid: row.get("board_nid").unwrap_or_default(),
                            broker_nid: row.get("broker_nid").unwrap_or_default(),
                            buy_sell: row.get::<_, String>("buy_sell").unwrap_or_default(),
                            stock_nid: row.get("stock_nid").unwrap_or_default(),
                            client_nid: row.get("client_nid").unwrap_or_default(),
                            order_price: row.get::<_, String>("order_price")
                                .and_then(|s| Ok(Decimal::from_str(&s).ok()))
                                .unwrap_or(Some(Decimal::new(0, 0))).unwrap_or_default(),
                            order_volume: row.get::<_, String>("order_volume")
                                .and_then(|s| Ok(Decimal::from_str(&s).ok()))
                                .unwrap_or(Some(Decimal::new(0, 0))).unwrap_or_default(),
                            done_volume: row.get::<_, String>("done_volume")
                                .and_then(|s| Ok(Decimal::from_str(&s).ok()))
                                .unwrap_or(Some(Decimal::new(0, 0))).unwrap_or_default(),
                            order_status: row.get("order_status").unwrap_or_default(),
                            market_order_id: row.get::<_, String>("market_order_id")
                                .and_then(|s| Ok(Decimal::from_str(&s).ok()))
                                .unwrap_or(Some(Decimal::new(0, 0))).unwrap_or_default(),
                            currency_nid: row.get("currency_nid").unwrap_or_default(),
                            commission_mode: row.get("commission_mode").unwrap_or_default(),
                            commission_percent: row.get::<_, Option<String>>("commission_percent")
                                .and_then(|opt_s| {
                                    Ok(opt_s.map(|s| Decimal::from_str(&s).ok()))
                                })
                                .unwrap_or(None).unwrap_or_default(),
                            settlement_mode: row.get("settlement_mode").unwrap_or_default(),
                            day_trade: row.get("day_trade").unwrap_or_default(),
                            online_trading: row.get("online_trading").unwrap_or_default(),
                            ng_report: row.get("ng_report").unwrap_or_default(),
                            force_buy_sell: row.get("force_buy_sell").unwrap_or_default(),
                            office_nid: row.get("office_nid").unwrap_or_default(),
                            trx_n_type: row.get("trx_n_type").unwrap_or_default(),
                        };

                        let _ = tx_clone.blocking_send(entity);
                        last_order_nid = row.get("order_nid").unwrap_or_default();
                        batch_count += 1;
                    }

                    if batch_count == 0 { break; }
                }

                println!("[JOB] DuckDB snapshot & order fetch finished in {:?}", job_start.elapsed());
            });

            // 4️⃣ tunggu DuckDB selesai baru close tx
            duck_handle.await.unwrap();
            drop(tx); // pastiin channel ditutup supaya Postgres task berhenti

            // 5️⃣ tunggu Postgres selesai
            pg_handle.await.unwrap();
        });

        Ok("Match job started".to_string())
    }

    pub async fn update_order_status(&self) -> Result<()> {
        self.repo.update_order_status(1).await
    }

    pub async fn match_orders_async(&self) -> Result<String> {
        let repo = self.repo.clone();
        tokio::spawn(async move {
            let result = repo.match_orders_async().await;
            match result {
                Ok(res) => println!("Match async job finished {}", res),
                Err(e) => println!("Error: {}", e),
            }
        });

        Ok("Match async job started".to_string())
    }

}
