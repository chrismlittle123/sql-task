-- Create dates dimension table
DROP TABLE IF EXISTS dim_dates;

CREATE TABLE dim_dates (
    date_id INT GENERATED ALWAYS AS IDENTITY,
    full_date DATE,
    year INT,
    month INT,
    day INT
) USING DELTA; 