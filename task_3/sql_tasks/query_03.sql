-- 3. Output the category of movies on which the most money was spent.

SELECT c.name AS category_name, SUM(p.amount) AS total_spent
FROM payment p
JOIN rental r ON p.rental_id = r.rental_id
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 1;

