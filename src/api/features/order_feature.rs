use actix_web::{web, Scope};

use crate::api::features::order_handler;

pub fn order_feature() -> Scope {
    web::scope("/order")
        .service(
            web::resource("/views")
                .route(web::get().to(order_handler::get_views)),
        )
        .service(
            web::resource("/load_match_data")
                .route(web::get().to(order_handler::load_match_data)),
        )
        .service(
            web::resource("/match")
                .route(web::post().to(order_handler::match_orders)),
        )
        .service(
            web::resource("/update_status")
                .route(web::post().to(order_handler::update_order_status)),
        )
        .service(
            web::resource("/match_async")
                .route(web::post().to(order_handler::match_orders_async)),
        )
}
