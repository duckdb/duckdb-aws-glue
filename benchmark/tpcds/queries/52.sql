SELECT dt.d_year,
       item.i_brand_id brand_id,
       item.i_brand brand,
       sum(ss_ext_sales_price) ext_price
FROM date_dim dt,
     store_sales,
     item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy=11
  AND dt.d_year=2000
  AND store_sales.ss_sold_date_bucket BETWEEN date_diff('day', DATE '1970-01-01', DATE '2000-11-01') // 10 AND date_diff('day', DATE '1970-01-01', DATE '2000-11-30') // 10
GROUP BY dt.d_year,
         item.i_brand,
         item.i_brand_id
ORDER BY dt.d_year,
         ext_price DESC,
         brand_id
LIMIT 100 ;

