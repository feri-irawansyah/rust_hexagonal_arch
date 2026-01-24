use actix_web::{web, HttpResponse};

use crate::app::AppService;

pub async fn get_views(service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.get_views().await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}

pub async fn match_orders( service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.match_orders().await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}

pub async fn update_order_status(service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.update_order_status().await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}

pub async fn match_orders_async(service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.match_orders_async().await {
        Ok(_) => HttpResponse::Ok().body("Async order matching started"),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}

pub async fn load_match_data(service: web::Data<AppService>) -> HttpResponse {
    match service.order_service.load_match_data().await {
        Ok(_) => HttpResponse::Ok().body("Async order matching started"),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error: {}", e)),
    }
}