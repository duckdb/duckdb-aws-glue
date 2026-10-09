SELECT sum(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales ,
     item ,
     date_dim
WHERE i_manufact_id = 977
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
  AND cs_sold_date_bucket BETWEEN date_diff('day', DATE '1970-01-01', DATE '2000-01-27') // 10 AND date_diff('day', DATE '1970-01-01', DATE '2000-04-26') // 10
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt >
    ( SELECT 1.3 * avg(cs_ext_discount_amt)
     FROM catalog_sales ,
          date_dim
     WHERE cs_item_sk = i_item_sk
       AND d_date BETWEEN '2000-01-27' AND cast('2000-04-26' AS date)
       AND cs_sold_date_bucket BETWEEN date_diff('day', DATE '1970-01-01', DATE '2000-01-27') // 10 AND date_diff('day', DATE '1970-01-01', DATE '2000-04-26') // 10
       AND d_date_sk = cs_sold_date_sk )
LIMIT 100;

