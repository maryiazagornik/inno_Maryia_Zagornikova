-- 6. Output cities with the number of active and inactive customers (active - customer.active = 1). 
-- Sort by the number of inactive customers in descending order.

SELECT ci.city AS city_name,
       COUNT(CASE WHEN cu.active = 1 THEN 1 END) AS active_customers,
       COUNT(CASE WHEN cu.active = 0 THEN 1 END) AS inactive_customers
FROM customer cu
JOIN address a ON cu.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
GROUP BY ci.city
ORDER BY inactive_customers DESC;

