-- 1. Top 10 categories by total revenue
SELECT category, 
       COUNT(*) AS total_bookings,
       ROUND(SUM(price)::numeric, 2) AS total_revenue
FROM hotels
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Monthly growth analysis
SELECT DATE_TRUNC('month', created_date) AS month,
       COUNT(*) AS new_listings,
       ROUND(AVG(price)::numeric, 2) AS avg_price
FROM hotels
GROUP BY month
ORDER BY month;

-- 3. Average rating by country
SELECT country,
       COUNT(*) AS hotel_count,
       ROUND(AVG(rating)::numeric, 2) AS avg_rating,
       ROUND(AVG(price)::numeric, 2) AS avg_price
FROM hotels
GROUP BY country
ORDER BY avg_rating DESC;

-- 4. EXPLAIN ANALYZE to show index usage
EXPLAIN ANALYZE
SELECT * FROM hotels WHERE category = 'Luxury' AND rating >= 4.0;