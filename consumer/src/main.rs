use std::{env, error::Error};
use dotenv::dotenv;
use serde_json::{from_str};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use models::{TruckMetrics, TruckMetricsRepository};
use sqlx::{postgres::PgPoolOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    
    // Get some variables from .env
    let topic = env::var("TOPIC").unwrap();
    let broker = env::var("BROKER").unwrap();
    let url_db = env::var("DATABASE_URL").unwrap();

    // Create a connection pool to the database
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url_db)
        .await?;

    // Repository to interact with the database
    let truck_repositoty = TruckMetricsRepository::new();    
     

    // Create consumer to receive messages from kafka
    let mut consumer = Consumer::from_hosts(vec![broker])
        // topic  
        .with_topic(topic)
        // what messages will be listening
        .with_fallback_offset(FetchOffset::Earliest)
        // at what group of consumers this consumer belongs
        .with_group("group1".to_string())
        // save readed offsets
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create().unwrap();
    
    println!("hello world");
    loop {

        // Listening to the topic
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                // cast list of utf8 to string
                let value = String::from_utf8_lossy(m.value);
                
                // cast string to struct
                let value: TruckMetrics = from_str(&value).unwrap();
                
                // saving
                let metrics = truck_repositoty.save(value, &pool).await?;
                println!("metrics saved {:?}", metrics);
            }
            // consuming this messageset
            let _ = consumer.consume_messageset(ms);
        }
        // persist in kafka what records were readed
       consumer.commit_consumed().unwrap();
    }

}

