pub mod app;
pub mod api {
    pub mod features {
        pub mod order_handler;
        pub mod order_feature;
    }
}
pub mod application {
    pub mod services {
        pub mod order {
            pub mod order_service;
        }
    }
    pub mod traits {
        pub mod order_trait;
    }
}

pub mod infrastructure {
    pub mod repositories {
        pub mod order_repository;
    }
    pub mod database {
        pub mod connection;
    }
}

#[actix_web::main]
async fn main() {
    app::run().await;
}
