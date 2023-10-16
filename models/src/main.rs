
use std::error::Error;

use sqlx::{postgres::PgPoolOptions};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@localhost:5433/skillforgeacademy")
        .await?;
    
    sqlx::migrate!("./migrations/").run(&pool).await?;

    Ok(())
}