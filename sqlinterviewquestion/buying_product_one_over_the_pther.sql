-- Create the table
CREATE TABLE orders (
    user_id INT,
    product_id VARCHAR(50),
    frequency INT
);

-- Insert 20 entries with frequencies
INSERT INTO orders (user_id, product_id, frequency) VALUES
(1, 'Apple', 5),
(1, 'Banana', 3),
(1, 'Cherry', 8),
(1, 'Date', 2),
(1, 'Elderberry', 7),
(1, 'Fig', 1),
(2, 'Grape', 4),
(2, 'Honeydew', 6),
(2, 'Kiwi', 3),
(2, 'Lemon', 4),
(3, 'Apple', 5),
(3, 'Banana', 2),
(3, 'Cherry', 3),
(3, 'Date', 1),
(2, 'Elderberry', 6),
(2, 'Fig', 4),
(2, 'Grape', 5),
(2, 'Honeydew', 2),
(2, 'Kiwi', 3),
(3, 'Lemon', 4);
-- assign a rank to the each product per person based on frequency
with ranking_table as (SELECT
user_id,
product_id,
frequency,
dense_rank() over(partition by user_id order by frequency desc) as ranking
FROM
orders)

-- select from that table to do a case when

select
user_id, product_id, 
case
when frequency >5 then 'most likely' 
when 3<  frequency <5 then 'likely'
else 'not likely'
end as likelihood_product_purchase
from ranking_table


