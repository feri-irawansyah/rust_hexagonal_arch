use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;

pub type PgPool = Pool;

pub fn create_connection(database_url: &str) -> PgPool {
    let mut cfg = Config::new();
    cfg.url = Some(database_url.to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    cfg.create_pool(None, NoTls)
        .expect("Failed to create postgres pool")
}
