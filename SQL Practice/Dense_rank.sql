use market_star_schema;

select * from cust_dimen limit 5;

select * from market_fact_full limit 5;

select * from orders_dimen limit 5;

select * from prod_dimen limit 5;

select * from shipping_dimen limit 5;

-- Dense Rank, rank, percent rank Example


select  ord_id
        , discount
        , customer_name
        ,rank() over(order by discount DESC ) as disc_rank
        ,dense_rank() over(order by discount DESC ) as disc_rank
        ,percent_rank() over(order by discount DESC ) as disc_rank
from market_fact_full as mff
join cust_dimen as cd on cd.cust_id = mff.cust_id
where customer_name ='rick wilson';