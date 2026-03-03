-- =============================================================================
-- Hotel ETL Pipeline — Analytical Queries
-- =============================================================================

SELECT * FROM public.hotels
ORDER BY id ASC 


SELECT COUNT(*) FROM hotels;

-- -----------------------------------------------------------------------------
-- QUERY 1 — Top Categories by Revenue
-- Index used: idx_hotels_category
-- -----------------------------------------------------------------------------
SELECT
    category,
    COUNT(*)                         AS total_hotels,
    ROUND(SUM(price)::NUMERIC, 2)    AS total_revenue,
    ROUND(AVG(price)::NUMERIC, 2)    AS avg_price,
    ROUND(MIN(price)::NUMERIC, 2)    AS min_price,
    ROUND(MAX(price)::NUMERIC, 2)    AS max_price
FROM hotels
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- QUERY 2 — Monthly Growth Analysis
-- Index used: idx_hotels_created
-- -----------------------------------------------------------------------------
SELECT
    TO_CHAR(DATE_TRUNC('month', created_date), 'YYYY-MM')  AS month,
    COUNT(*)                                               AS new_listings,
    ROUND(AVG(price)::NUMERIC, 2)                          AS avg_price,
    ROUND(AVG(rating)::NUMERIC, 2)                         AS avg_rating,
    SUM(COUNT(*)) OVER (
        ORDER BY DATE_TRUNC('month', created_date)
    )                                                      AS cumulative_listings
FROM hotels
GROUP BY DATE_TRUNC('month', created_date)
ORDER BY month;


-- -----------------------------------------------------------------------------
-- QUERY 3 — Average Rating by Country
-- Index used: idx_hotels_country
-- -----------------------------------------------------------------------------
SELECT
    country,
    COUNT(*)                         AS hotel_count,
    ROUND(AVG(rating)::NUMERIC, 2)   AS avg_rating,
    ROUND(AVG(price)::NUMERIC, 2)    AS avg_price,
    SUM(reviews_count)               AS total_reviews,
    ROUND(AVG(rooms_available))      AS avg_rooms
FROM hotels
GROUP BY country
ORDER BY avg_rating DESC, hotel_count DESC;



-- -----------------------------------------------------------------------------
-- INDEX PERFORMANCE DEMO
-- -----------------------------------------------------------------------------

-- Disable indexes → forces Seq Scan
SET enable_indexscan  = OFF;
SET enable_bitmapscan = OFF;

EXPLAIN ANALYZE
SELECT * FROM hotels
WHERE category = 'Luxury' AND rating >= 4.0;

-- Re-enable indexes → uses Index Scan
SET enable_indexscan  = ON;
SET enable_bitmapscan = ON;

EXPLAIN ANALYZE
SELECT * FROM hotels
WHERE category = 'Luxury' AND rating >= 4.0;

