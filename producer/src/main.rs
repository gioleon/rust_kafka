use dotenv::dotenv;
use kafka::producer::{Producer, Record, RequiredAcks};
use serde::Serialize;
use serde_json::to_string;
use std::collections::HashMap;
use std::time::Duration;
use std::vec;
use std::{env, error::Error, sync::mpsc, thread};
use rand::{Rng, thread_rng};

use models::{Message, TruckMetrics};

#[tokio::main]
async fn main() {
    // Loading env vars
    dotenv().ok();

    // create list of plates
    let plates = vec!["abc", "def", "hij", "abc", "hij", "def", "abc"];

    // in this case we won't save into the hashmaps threads but channels
    // that have direct communication to receiver into the channels
    let mut trucks_map: HashMap<String, mpsc::Sender<TruckMetrics>> = HashMap::new();
     
    // vec to save thread to later join them with the main thread
    let mut thread_vec = vec![];

    for plate in plates.iter() {
        // convert &&str to String
        let plate = plate.to_string();

        // Create thread_rng to generate random numbers
        let mut rng = thread_rng();

        // look if we already have process this plate
        match trucks_map.get(&plate) {
            // if we already proccessed this plate let's use the sender attached to it
            Some(sender) => {
                // send message
                sender.send(
                    // create truck metric that we will send
                    TruckMetrics::new(
                        plate.clone(),
                        rng.gen_range(0.0..100.0),
                        rng.gen_range(0.0..100.0)
                    )
                )
                .unwrap();
            }
            // otherwise, let's create a new channel and thread for it.
            None => {
                // Generate channels
                let (tx, rx) = mpsc::channel();
                
                // send message
                tx.send(
                    // create truck metric that we will send
                    TruckMetrics::new(
                        plate.clone(),
                        rng.gen_range(0.0..100.0),
                        rng.gen_range(0.0..100.0)
                    )
                )
                .unwrap();

                // connect the channel to the plate
                trucks_map.insert(plate.clone(), tx);

                let handler = thread::spawn(move || {
                    // configuration for the kafka sender function
                    let topic = env::var("TOPIC")
                        .expect("TOPIC environment variable not set");
                    let broker = env::var("BROKER")
                        .expect("BROKER environment variable not set");

                    // we use a for loop to keep our thread alive
                    loop {
                        // here we will receive the structs of type Truck from the channel
                        let payload = rx.recv().unwrap();

                        // send the truck to kafka
                        let result = send_message(&payload, &topic, vec![broker.clone()]);

                        // validating result
                        match result {
                            Ok(()) => {
                                println!("Metrics for truck {:?} delivered property", payload)
                            }
                            Err(e) => println!("Error sending message: {}", e),
                        }
                    }
                });

                // Saving thread to later sync them with the main thread
                thread_vec.push(handler);
            }
        }
    }
    for thread in thread_vec {
        thread.join().unwrap();
    }
}

fn send_message<T>(payload: &T, topic: &str, broker: Vec<String>) -> Result<(), Box<dyn Error>>
where
    T: Message + Serialize,
{
    let mut producer = Producer::from_hosts(broker)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let payload = to_string(payload)?;

    producer.send(&Record::from_value(topic, payload))?;

    Ok(())
}
