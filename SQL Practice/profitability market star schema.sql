use market_star_schema;

/*
Problem statement: Identify the sustainable (profitable) product categories so that the growth team can 
capitalise on them to increase sales.

Metrics: Some of the metrics that can be used for performing the profitability analysis are as follows:

Profits per product category
Profits per product subcategory
Average profit per order
Average profit percentage per order

Tables: The tables that are required for solving this problem are as follows:

'market_fact_full'
'prod_dimen'
'orders_dimen'
*/

select * from market_fact_full limit 4;
select * from prod_dimen limit 4;
-- Profits per product Category

	-- using subquery:
    
select product_category
		, sum(total_profit) as total_profit
        
from (
select pd.Product_category as product_category
	   ,sum(mff.profit) as total_profit
from market_fact_full as mff
inner join prod_dimen as pd on pd.prod_id = mff.prod_id
group by mff.prod_id
order by 2 DESC) as temp 
group by 1
order by 2 DESC
;

   -- using CTE


select pd.Product_category as product_category
	   ,sum(mff.profit) as total_profit
from market_fact_full as mff
inner join prod_dimen as pd on pd.prod_id = mff.prod_id
group by 1
order by 2 DESC;


-- Profits per product subcategory


select pd.Product_sub_category as product_sub_category
	   ,sum(mff.profit) as total_profit
from market_fact_full as mff
inner join prod_dimen as pd on pd.prod_id = mff.prod_id
group by 1
order by 2 DESC;



-- Average profit per order



-- Average profit percentage per order

/*
Problem statement: Extract the details of the top ten customers in the expected output format.

Expected Output form: cust_id, rank, customer_name, profit, customer_city, customer_state, sales.
Tables: The tables that are required for solving this problem are as follows:

'cust_dimen'
'market_fact_full'

*/
 use market_star_schema;
select * from cust_dimen limit 4;
select * from market_fact_full limit 4;

select  cust_id, 
		rank_ as 'rank', 
        customer_name,
        total_profit as profit, 
        city as customer_city, 
        state as customer_state, 
        round(sales,2) as sales
from (
select cd.*,
		sum(profit) as total_profit,
        sum(sales) as sales,
        rank() over(order by sum(profit) DESC ) as rank_,
        dense_rank() over(order by sum(profit) DESC ) as rank_dense
        from cust_dimen as cd
        inner join market_fact_full as mff on mff.cust_id= cd.cust_id
        group by cust_id ) as temp
        where rank_ <=10 and rank_dense <=10;
        


/*
Problem statement: Extract the required details of the customers who have not placed an order yet.
Expected columns: The columns that are required as the output are as follows:

'cust_id'
'cust_name'
'city'
'state'
'customer_segment'
A flag to indicate that there is another customer with the exact same name and city but a different customer ID.
Tables: The tables that are required for solving this problem are as follows:

'cust_dimen'
'market_fact_full'
*/


select cdd.*
 from cust_dimen cdd
where cdd.cust_id IN (
select distinct cd.cust_id
from cust_dimen as cd 
left outer join market_fact_full as mff 
on mff.cust_id = cd.cust_id 
where mff.ord_id is null)
order by cdd.customer_name;

-- No such customer with no order palced.

-- Finding customers that have placed multiple order.
with orders_cte as (
select cd.*,
		count(ord_id) as number_of_orders
from cust_dimen as cd
left join market_fact_full as mff
on mff.cust_id = cd.cust_id
group by  cust_id
having count(ord_id) >1
),

fraud_check_cte as (
select customer_name,
		city,
        count(cust_id) as customer_id_count
from cust_dimen
group by customer_name, city
)

select oc.* ,
		case 
			when fcc.customer_id_count >1 then 'FRAUD' else 'NORMAL' END as FRAUD_TAG
from orders_cte as oc 
left join fraud_check_cte as fcc 
on fcc.customer_name = oc.customer_name 
and fcc.city = oc.city