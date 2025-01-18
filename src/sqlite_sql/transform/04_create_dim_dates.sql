CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date DATETIME,
    year INTEGER,
    month INTEGER,
    day INTEGER
); 