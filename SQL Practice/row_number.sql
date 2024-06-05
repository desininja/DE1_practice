use market_star_schema;


select customer_name,
		count(distinct ord_id) order_count,
        rank() over(order by count(distinct ord_id) DESC) as order_rank,
        dense_rank() over(order by count(distinct ord_id) DESC) as order_dense_rank,
        row_number() over(order by count(distinct ord_id) DESC) as order_row_number
        
        
from market_fact_full as mff 
inner join cust_dimen as cd on cd.cust_id=mff.cust_id
group by customer_name;




-- Named Window
select  ord_id
        , discount
        , customer_name
        ,rank() over w as disc_rank
        ,dense_rank() over w as disc_rank
        ,row_number() over w as disc_row_number
        ,percent_rank() over w as disc_percent_rank
from market_fact_full as mff
join cust_dimen as cd on cd.cust_id = mff.cust_id
window w as  (partition by customer_name order by discount DESC)
;



-- Upgrad Question:

 -- rewrite query with named window:
 
 SELECT *,
RANK() OVER (
  PARTITION BY ship_mode
  ORDER BY COUNT(*)) 'Rank',
DENSE_RANK() OVER (
  PARTITION BY ship_mode
  ORDER BY COUNT(*)) 'Dense Rank',
PERCENT_RANK() OVER (
  PARTITION BY ship_mode
  ORDER BY COUNT(*)) 'Percent Rank'
FROM shipping_dimen;


-- Answer:


 SELECT *,
RANK() OVER w 'Rank',
DENSE_RANK() OVER w 'Dense Rank',
PERCENT_RANK() OVER w 'Percent Rank'
FROM shipping_dimen
window w as (PARTITION BY ship_mode ORDER BY COUNT(*));


-- shipping cost and dates
with daily_shipping_summary as (
select ship_date,
		sum(shipping_cost) as daily_total
        
        from market_fact_full m
        inner join shipping_dimen s on m.ship_id = s.ship_id
group by ship_date)

select *,
		sum(daily_total) over w1 as running_total,
        avg(daily_total) over w2 as moving_average
from daily_shipping_summary
window w1 as (order by ship_date rows unbounded preceding),
 w2 as (order by ship_date  rows 6 preceding);
 
 -- lead lag example:
with order_date_cte as (
 select 
		cd.customer_name,
        mff.ord_id,
        od.order_date
from market_fact_full as mff
left join cust_dimen as cd on cd.cust_id =mff.cust_id
left join orders_dimen as od on od.ord_id = mff.ord_id
group by cd.customer_name,
        mff.ord_id,
        od.order_date
 ),
 order_date_summary as (
 select *,
		lead(order_date, 1) over(partition by customer_name order by order_date, ord_id) as next_order_date
from  order_date_cte)

select *, datediff(next_order_date,order_date)
from order_date_summary;
 
 -- optimized query: use distinct instead of group by clause.
 WITH order_date_cte AS (
    SELECT DISTINCT
        cd.customer_name,
        mff.ord_id,
        od.order_date
    FROM market_fact_full AS mff
    LEFT JOIN cust_dimen AS cd ON cd.cust_id = mff.cust_id
    LEFT JOIN orders_dimen AS od ON od.ord_id = mff.ord_id
),
order_date_summary AS (
    SELECT 
        customer_name,
        ord_id,
        order_date,
        LEAD(order_date, 1) OVER (PARTITION BY customer_name ORDER BY order_date, ord_id) AS next_order_date
    FROM order_date_cte
)

SELECT 
    customer_name,
    ord_id,
    order_date,
    next_order_date,
    DATEDIFF(next_order_date, order_date) AS days_between_orders
FROM order_date_summary;

 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 