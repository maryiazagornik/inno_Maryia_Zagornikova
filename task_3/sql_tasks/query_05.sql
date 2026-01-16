-- 5. Output the top 3 actors who have appeared the most in movies in the “Children” category. 
-- If several actors have the same number of movies, output all of them.

WITH children_films AS (
  SELECT f.film_id
  FROM film f
  JOIN film_category fc ON f.film_id = fc.film_id
  JOIN category c ON fc.category_id = c.category_id
  WHERE c.name = 'Children'
),
actor_counts AS (
  SELECT a.actor_id, a.first_name, a.last_name, COUNT(*) AS film_count
  FROM actor a
  JOIN film_actor fa ON a.actor_id = fa.actor_id
  WHERE fa.film_id IN (SELECT film_id FROM children_films)
  GROUP BY a.actor_id, a.first_name, a.last_name
),
ranked AS (
  SELECT *, DENSE_RANK() OVER (ORDER BY film_count DESC) AS rnk
  FROM actor_counts
)
SELECT first_name, last_name, film_count
FROM ranked
WHERE rnk <= 3;

