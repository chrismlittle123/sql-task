-- Create dates dimension table
DROP TABLE IF EXISTS dim_dates;

CREATE TABLE dim_dates (
    date_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT
) USING DELTA; 