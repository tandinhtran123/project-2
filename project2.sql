with cte as 
(select user_id, sales,
format_date('%Y %m',first_purchase_date) as cohort_date,
created_at,
((extract(year from created_at)-extract(year from first_purchase_date))*12+
extract(month from created_at)-extract(month from first_purchase_Date))+1 as index
from
(select user_id,
sale_price as sales,
min(created_at)over(partition by user_id order by user_id) as first_purchase_date,
created_at 
from bigquery-public-data.thelook_ecommerce.order_items
where status in ('Complete'))a),
ct2 as
(select cohort_date,
index, 
count(distinct user_id) as count_user,
sum(sales) as revenue
from cte
group by cohort_date, index
order by cohort_date, index),
ct3 as
(select cohort_date,
sum(case when index=1 then count_user else 0 end as index1),
sum(case when index=2 then count_user else 0 end as index2),
sum(case when index=3 then count_user else 0 end as index3),
sum(case when index=4 then count_user else 0 end as index4)
from ct2
group by 1)
--retention cohort
select cohort_date,
100.00*(index1/index1) as i1
100.00*(index2/index1) as i2
100.00*(index3/index1) as i3
100.00*(index4/index1) as i4
from ct3
