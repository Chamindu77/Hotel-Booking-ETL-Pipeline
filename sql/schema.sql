CREATE TABLE IF NOT EXISTS hotels (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    price           NUMERIC(10,2) CHECK (price > 0),
    rating          NUMERIC(3,1) CHECK (rating BETWEEN 1 AND 5),
    country         VARCHAR(100),
    created_date    DATE,
    rooms_available INTEGER,
    reviews_count   INTEGER DEFAULT 0
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_hotels_category ON hotels(category);
CREATE INDEX IF NOT EXISTS idx_hotels_country  ON hotels(country);
CREATE INDEX IF NOT EXISTS idx_hotels_rating   ON hotels(rating);
CREATE INDEX IF NOT EXISTS idx_hotels_created  ON hotels(created_date);