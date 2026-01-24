pub mod app;
pub mod api {
    pub mod features {
        pub mod order_handler;
        pub mod order_feature;
    }
}
pub mod applications {
    pub mod services {
        pub mod order {
            pub mod order_service;

            pub  mod  domain {
                pub mod order_entity;
            }
        }
    }
    pub mod contracts {
        pub mod order_trait;
    }
}

pub mod infrastructure {
    pub mod repositories {
        pub mod order_repository;
    }
    pub mod database {
        pub mod connection;
        pub mod snapshot;
    }
}

fn init_duckdb() -> duckdb::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute_batch(
        r#"
        SET extension_directory='snapshots';
        INSTALL postgres_scanner;
        "#,
    )?;

    Ok(())
}

#[actix_web::main]
async fn main() {
    match init_duckdb() {
        Ok(_) => println!("DuckDB initialized successfully."),
        Err(e) => eprintln!("Failed to initialize DuckDB: {}", e),
    }
    app::run().await;
}
