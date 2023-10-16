CREATE TABLE IF NOT EXISTS truck (
    plate varchar(10) PRIMARY KEY,
    year int,
    model varchar(20)
);

CREATE TABLE IF NOT EXISTS truck_metric (
    id serial PRIMARY KEY,
    truck_plate varchar(10),
    gasoline numeric,
    speed numeric,
    FOREIGN KEY (truck_plate) 
        REFERENCES truck (plate)
);
