-- 7. Output the category of movies that have the highest number of total rental 
-- hours in the city (customer.address_id in this city) and that start with the letter “a”. 
-- Do the same for cities that have a “-” in them. Write everything in one query.

WITH rental_hours AS (
  SELECT 
    ci.city AS city_name,
    c.name AS category_name,
    SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS total_hours
  FROM rental r
  JOIN inventory i ON r.inventory_id = i.inventory_id
  JOIN film f ON i.film_id = f.film_id
  JOIN film_category fc ON f.film_id = fc.film_id
  JOIN category c ON fc.category_id = c.category_id
  JOIN customer cu ON r.customer_id = cu.customer_id
  JOIN address a ON cu.address_id = a.address_id
  JOIN city ci ON a.city_id = ci.city_id
  WHERE c.name ILIKE 'a%' OR ci.city LIKE '%-%'
  GROUP BY ci.city, c.name
),
ranked AS (
  SELECT *,
         RANK() OVER (PARTITION BY city_name ORDER BY total_hours DESC) AS rnk
  FROM rental_hours
)
SELECT city_name, category_name, total_hours
FROM ranked
WHERE rnk = 1;

