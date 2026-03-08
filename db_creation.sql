
CREATE TABLE accidents(
    id SERIAL PRIMARY KEY,
    accident_id INT UNIQUE NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    severity VARCHAR(10) CHECK (severity IN ('Fatal', 'Slight', 'Serious')) DEFAULT 'Unknown',
    borough VARCHAR(30) CHECK (borough IN (
        'Brent', 'Redbridge', 'Waltham Forest', 'Lewisham', 'Wandsworth', 
        'Barnet', 'Southwark', 'Bexley', 'Lambeth', 'Richmond upon Thames', 
        'Hounslow', 'Sutton', 'Enfield', 'City of Westminster', 'Tower Hamlets', 
        'Kensington and Chelsea', 'Camden', 'Islington', 'City of London', 
        'Havering', 'Hammersmith and Fulham', 'Newham', 'Harrow', 'Merton', 
        'Bromley', 'Hackney', 'Hillingdon', 'Ealing', 'Greenwich', 'Kingston', 
        'Barking and Dagenham', 'Croydon', 'Haringey'
    )) DEFAULT 'Unknown'
);

CREATE TABLE casualties(
    id SERIAL PRIMARY KEY,
    accident_id INT REFERENCES accidents (id),
    age INT DEFAULT NULL,
    class VARCHAR(10) CHECK (class IN ('Driver', 'Pedestrian', 'Passenger')) DEFAULT 'Unknown',
    mode VARCHAR(20) CHECK (mode IN (
        'PrivateHire', 'Taxi', 'GoodsVehicle', 'Car', 'BusOrCoach', 
        'Pedestrian', 'PoweredTwoWheeler', 'OtherVehicle', 'PedalCycle'
    )) DEFAULT 'Unknown',
    severity VARCHAR(10) CHECK (severity IN ('Fatal', 'Slight', 'Serious')) DEFAULT 'Unknown',
    age_band VARCHAR (10) CHECK (age_band IN ('Child', 'Adult')) DEFAULT 'Unknown'
);

CREATE TABLE vehicles(
    id SERIAL PRIMARY KEY,
    accident_id INT REFERENCES accidents (id),
    type VARCHAR(30) DEFAULT 'Unknown'
);