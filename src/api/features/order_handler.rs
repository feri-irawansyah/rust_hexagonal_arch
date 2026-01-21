use actix_web::{web, HttpResponse};

use crate::app::AppService;

pub async fn get_views(service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.get_views().await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}

pub async fn add(
    service: web::Data<AppService>,
) -> HttpResponse {
    match service.order_service.add().await {
        Ok(_) => HttpResponse::Ok().finish(),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}
