WITH orders_with_total AS (
    SELECT order_id, price + tax AS total
    FROM Orders
)
SELECT order_id, SUM(total)
FROM orders_with_total
GROUP BY order_id
----------
SELECT user, amount
FROM Orders
WHERE product IN (
    SELECT product FROM NewProducts
)
----------
(SELECT s FROM t1) UNION ALL SELECT s FROM t2
----------
(SELECT s FROM t1) INTERSECT ALL SELECT s FROM t2
----------
(SELECT s FROM t1) EXCEPT ALL SELECT s FROM t2