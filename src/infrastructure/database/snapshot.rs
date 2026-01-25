use duckdb::{Connection, Result};

pub struct SnapshotDb {
    conn: Connection,
}

impl SnapshotDb {
    pub fn new() -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        conn.execute_batch(
            r#"
            LOAD postgres_scanner;
            PRAGMA threads=8;
            PRAGMA memory_limit='8GB';
            PRAGMA enable_progress_bar=true;
            PRAGMA enable_object_cache=true;
            SET preserve_insertion_order=false;
            "#,
        )?;

        Ok(Self { conn })
    }

    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    pub fn load_data_from_parquet(&self, table_name: &str, where_clause: Option<&str>) -> Result<String> {
        dotenvy::dotenv().ok();
        let pg_conn = std::env::var("SNAPSHOT_PG").expect("Not found SNAPSHOT_PG in .env");
        // let pg_conn = "host=localhost user=postgres password=password_baru dbname=orionatlas";

        let where_sql = where_clause
            .map(|w| format!("WHERE {}", w))
            .unwrap_or_default();

        let sql = format!(
            r#"
                COPY (
                    SELECT *
                    FROM postgres_scan('{pg_conn}', 'public', '{table}')
                    {where_sql}
                )
                TO 'snapshot_{table}.parquet'
                (FORMAT PARQUET, COMPRESSION ZSTD);
            "#,
            table = table_name,
        );

        match self.conn.execute_batch(&sql) {
            Ok(_) => Ok(format!("snapshot_{}.parquet", table_name)),
            Err(e) => Err(e),
        }
    }

    pub fn load_data_from_db(&self, table_name: &str, where_clause: Option<&str>) -> Result<String> {
        dotenvy::dotenv().ok();
        let pg_conn = std::env::var("SNAPSHOT_PG").expect("Not found SNAPSHOT_PG in .env");
        // let pg_conn = "host=localhost user=postgres password=password_baru dbname=orionatlas";

        let where_sql = where_clause
            .map(|w| format!("WHERE {}", w))
            .unwrap_or_default();

        let sql = format!(
            r#"
                CREATE TABLE temp_{table} AS
                SELECT *
                FROM postgres_scan('{pg_conn}', 'public', '{table}')
                {where_sql};
            "#,
            table = table_name,
        );

        match self.conn.execute_batch(&sql) {
            Ok(_) => Ok(format!("Table temp_{} created", table_name)),
            Err(e) => Err(e),
        }
    }
}
