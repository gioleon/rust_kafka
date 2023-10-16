use std::error::Error;
use serde::{Serialize, Deserialize};
use sqlx::{Row};


// Model
pub trait Message {}

#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Truck {
    pub plate: String,
    pub year: i32,
    pub model: String
}

impl Truck {
    pub fn new(plate: String, year: i32, model: String) -> Self {
        Truck {
            plate,
            year,
            model
        }
    }

}


#[derive(Serialize, Deserialize, Debug)]
pub struct TruckMetrics {
    pub truck_plate: String,
    pub gasoline: f32,
    pub speed: f32
}

impl TruckMetrics {
    pub fn new(truck_plate: String, gasoline: f32, speed: f32) -> Self {

        TruckMetrics { 
            truck_plate: truck_plate, 
            gasoline: gasoline, 
            speed: speed 
        }
    }
}

impl Message for TruckMetrics {}

// Repository

pub struct TruckMetricsRepository  {}

impl TruckMetricsRepository {
    pub fn new() -> Self {
        TruckMetricsRepository {}
    }
}

impl TruckMetricsRepository {
    pub async fn save(&self, metrics: TruckMetrics, conn: &sqlx::PgPool) -> Result<TruckMetrics, Box<dyn std::error::Error>> {
        let query = "INSERT INTO truck_metric (truck_plate, gasoline, speed) VALUES ($1, $2, $3)";

        sqlx::query(query)
            .bind(&metrics.truck_plate)
            .bind(metrics.gasoline)
            .bind(metrics.speed)
            .execute(conn)
            .await?;

        Ok(metrics)
    }

    pub async fn get_by_plate(
        &self, plate: &str, conn: &sqlx::PgPool
    ) -> Result<Vec<TruckMetrics>, Box<dyn Error>> {
        let query = "SELECT * FROM truck_metric WHERE truck_plate = $1";

        let rows = sqlx::query(query)
            .bind(plate)
            .fetch_all(conn)
            .await?;

        let metrics = rows.iter().map(|row| {
            TruckMetrics::new(
                row.get("truck_plate"),
                row.get("gasoline"),
                row.get("speed")
            )
        }).collect();

        Ok(metrics)
    }

    pub async fn get_all(&self, conn: &sqlx::PgPool) -> Result<Vec<TruckMetrics>, Box<dyn Error>> {
        let query = "SELECT * FROM truck_metric";

        let rows = sqlx::query(query)
            .fetch_all(conn)
            .await?;
        
        let metrics = rows.iter().map(|row| {
            TruckMetrics::new(
                row.get("truck_plate"),
                row.get("gasoline"),
                row.get("speed")
            )
        }).collect();

        Ok(metrics)
    }


    pub async fn delete_by_plate(&self, plate: &str, conn: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
        let query = "DELETE FROM truck_metric WHERE truck_plate = $1";

        sqlx::query(query)
            .bind(plate)
            .execute(conn)
            .await?;
        
        Ok(())
    }

    pub async fn delete_all(&self, conn: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
        let query = "DELETE FROM truck_metric";
        sqlx::query(query)
            .execute(conn)
            .await?;

        Ok(())
    }

}

pub struct TruckRepository  {}

impl TruckRepository {
    pub fn new() -> Self {
        TruckRepository {}
    }
}

impl TruckRepository {
    pub async fn save(&self, truck: Truck, conn: &sqlx::PgPool) -> Result<Truck, Box<dyn std::error::Error>> {
        let query = "INSERT INTO truck (plate, year, model) VALUES ($1, $2, $3)";

        sqlx::query(query)
            .bind(&truck.plate)
            .bind(truck.year)
            .bind(&truck.model)
            .execute(conn)
            .await?;

        Ok(truck)
    }

    pub async fn get_by_plate(
        &self, plate: &str, conn: &sqlx::PgPool
    ) -> Result<Truck, Box<dyn Error>> {
        let query = "SELECT * FROM truck WHERE plate = $1";

        let row = sqlx::query(query)
            .bind(plate)
            .fetch_optional(conn)
            .await?;

        match row {
            Some(row) => {
                let truck = Truck::new(
                    row.get("plate"),
                    row.get("year"),
                    row.get("model")
                );

                Ok(truck)
            },
            None => Err("Truck not found".into())
        }
    }

    pub async fn get_all(&self, conn: &sqlx::PgPool) -> Result<Vec<Truck>, Box<dyn Error>> {
        let query = "SELECT * FROM truck";

        let rows = sqlx::query(query)
            .fetch_all(conn)
            .await?;
        
        let metrics = rows.iter().map(|row| {
            Truck::new(
                row.get("plate"),
                row.get("year"),
                row.get("model")
            )
        }).collect();

        Ok(metrics)
    }


    pub async fn delete_by_plate(&self, plate: &str, conn: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
        let query = "DELETE FROM truck WHERE plate = $1";

        sqlx::query(query)
            .bind(plate)
            .execute(conn)
            .await?;
        
        Ok(())
    }

    pub async fn delete_all(&self, conn: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
        let query = "DELETE FROM truck";
        sqlx::query(query)
            .execute(conn)
            .await?;

        Ok(())
    }

}

