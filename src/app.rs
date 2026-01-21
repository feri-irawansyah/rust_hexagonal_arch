use std::sync::Arc;

use actix_web::{App, HttpServer, middleware, web};

use crate::{
    api::features::order_feature::order_feature, 
    application::services::order::order_service::OrderService, infrastructure::{database::connection::create_connection, repositories::order_repository::OrderRepository}
};

#[derive(Clone)]
pub struct AppService {
    pub order_service: Arc<OrderService>,
}

pub async fn run() {
    dotenvy::dotenv().ok();
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = create_connection(&db_url);
    let repo = Arc::new(OrderRepository::new(pool.clone()));

    let services = AppService {
        order_service: Arc::new(OrderService::new(repo.clone())),
    };

    HttpServer::new(move || {
        App::new()
            // Configure your app here
            .app_data(web::Data::new(services.clone()))
            .configure(|cfg| {
                cfg.service(order_feature());
            })
            .wrap(middleware::Logger::default())
    })
    .bind(("127.0.0.1", 8080))
    .expect("Failed to bind to address")
    .run()
    .await
    .expect("Failed to run server");
}