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

select 


-- Average profit percentage per order












