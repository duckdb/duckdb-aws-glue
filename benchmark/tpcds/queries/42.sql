SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       sum(ss_ext_sales_price)
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
         item.i_category_id,
         item.i_category
ORDER BY sum(ss_ext_sales_price) DESC,dt.d_year,
                                      item.i_category_id,
                                      item.i_category
LIMIT 100 ;

