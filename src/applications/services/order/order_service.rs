use std::{str::FromStr, sync::Arc};
use anyhow::Result;
use rust_decimal::{Decimal, prelude::FromPrimitive};
use tokio::{sync::mpsc, time::Instant};

use crate::{
    applications::{
        contracts::order_trait::TOrderRepository, services::order::domain::order_entity::{BrokerTrxState, ClientTrxState, MatchResult, OrderDone, OrderEntity, PartialMatch, ProcessingConfig}
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
                ("broker_commission_trx", None),
                ("client_commission_trx", None),
                ("trx_type_match", None),
                ("trx_type", None),
                ("mv_temp_broker_trx", None),
                ("mv_temp_client_trx", None),
            ];

            for (table, where_clause) in tables {
                let start = Instant::now();
                if let Err(e) = snapshot.load_data_from_parquet(table, where_clause) {
                    eprintln!("[JOB] snapshot failed table={}: {}", table, e);
                    continue;
                }
                println!("[JOB] snapshot table={} done in {:?}", table, start.elapsed());
            }
            
            // dummy execute to clear cache
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
        let start = Instant::now();
        
        tokio::spawn(async move {
            let repo_clone = repo.clone();

            let snapshot = SnapshotDb::new().unwrap();
            let handle = tokio::task::spawn_blocking(move || -> Result<MatchResult> {
                // temp tables
                // repo_clone.create_btx_temp(&snapshot)?;
                // println!("[JOB] Temp Broker Trx created {:?}", start.elapsed());
                // repo_clone.create_ctx_temp(&snapshot)?;
                // println!("[JOB] Temp Client Trx created {:?}", start.elapsed());
                repo_clone.create_alloc_temp(&snapshot)?;
                println!("[JOB] Temp Alloc created {:?}", start.elapsed());

                // ===============================
                // 1️⃣ CREATE MATCH RESULT (SET-BASED)
                // ===============================
                snapshot.conn().execute(r#"
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
                "#, [])?;

                // ===============================
                // 4️⃣ BUILD RESULT FOR POSTGRES
                // ===============================
                let mut full = Vec::new();
                let mut partial = Vec::new();

                let mut stmt = snapshot.conn().prepare(r#"
                    SELECT trade_nid, order_nid, alloc_volume, remain_volume, match_type
                    FROM match_result
                "#)?;

                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, i32>(0)?,
                        row.get::<_, i32>(1)?,
                        match row.get::<_, i32>(2) {
                            Ok(v) => {
                                rust_decimal::Decimal::from_i32(v).unwrap_or_default()
                            },
                            Err(_) => rust_decimal::Decimal::ZERO,
                        },
                        match row.get::<_, i32>(3) {
                            Ok(v) => {
                                rust_decimal::Decimal::from_i32(v).unwrap_or_default()
                            },
                            Err(_) => rust_decimal::Decimal::ZERO,
                        },
                        row.get::<_, i32>(4)?,
                    ))
                })?;

                for r in rows {
                    let (t, o, alloc, remain, kind) = r?;
                    if kind == 1 {
                        full.push((t, o));
                    } else {
                        partial.push(PartialMatch {
                            trade_nid: t,
                            order_nid: o,
                            alloc_volume: alloc,
                            remain_volume: remain,
                        });
                    }
                }

                // ===============================
                // 5️⃣ ORDER DONE (DuckDB → Rust)
                // ===============================
                let mut order_done = Vec::new();
                let mut stmt = snapshot.conn().prepare(r#"
                    SELECT order_nid, SUM(alloc_volume) AS done_volume
                    FROM alloc_temp
                    WHERE alloc_volume > 0
                    GROUP BY order_nid
                "#)?;

                let rows = stmt.query_map([], |row| {
                    Ok(OrderDone {
                        order_nid: row.get(0)?,
                        done_volume: row.get(1)?,
                    })
                })?;

                for r in rows {
                    order_done.push(r?);
                }

                Ok(MatchResult {
                    full_trade: full,
                    partial,
                    order_done,
                })
            });

            let result = handle.await.unwrap();

            match result {
                Ok(result) => {
                    println!("DuckDB done in {:?}", start.elapsed());
                    let _ = repo.match_orders_async(result).await;
                    println!("All done in {:?}", start.elapsed());
                }
                Err(e) => println!("Error: {}", e),
            }
        });

        Ok("Match async job started".to_string())
    }

}
