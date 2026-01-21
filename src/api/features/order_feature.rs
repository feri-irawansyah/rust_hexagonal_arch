use actix_web::{web, Scope};

use crate::api::features::order_handler;

pub fn order_feature() -> Scope {
    web::scope("/order")
        .service(
            web::resource("/views")
                .route(web::get().to(order_handler::get_views)),
        )
}
