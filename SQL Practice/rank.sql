use market_star_schema;

select * from cust_dimen limit 5;

select * from market_fact_full limit 5;

select * from orders_dimen limit 5;

select * from prod_dimen limit 5;

select * from shipping_dimen limit 5;


-- TOP 10 sales orders from a customer
  -- First approach to use order by and limit clause.
select ord_id,customer_name ,sales from market_fact_full  as mff
join cust_dimen as cd on cd.cust_id = mff.cust_id
where customer_name = 'rick wilson'
order by sales DESC 
limit 10;
  
  -- Second approach to use rank, in this approach we can again use the limit option here only but this is still not flexible.
  
select    ord_id
		, customer_name
        , round(sales,2) as rounded_sales
        , rank() over(order by sales DESC) as sales_rank from market_fact_full as mff
join cust_dimen as cd on cd.cust_id =mff.cust_id
where customer_name = 'rick wilson' ;

  -- Third approach is to use CTE and rank 
  
  with sales_rank_cte as (
  select ord_id, customer_name, round(sales,2) as rounded_sales, rank() over(order by sales DESC) as sales_rank from market_fact_full as mff
  join cust_dimen as cd on cd.cust_id = mff.cust_id
  where customer_name = 'rick wilson'
  )
  
  select ord_id, customer_name , rounded_sales from sales_rank_cte where sales_rank <=10;
  
  -- Fourth approach is to use Subquery
  
  select ord_id, customer_name, rounded_sales from
  (
  select ord_id, customer_name, round(sales,2) as rounded_sales, rank() over(order by sales DESC) as sales_rank from market_fact_full as mff
  join cust_dimen as cd on cd.cust_id = mff.cust_id
  where customer_name ='rick wilson'
  ) as temp
  
  where sales_rank <=10;
  
  
  /*
  Recommendation
For simplicity and performance: Use the first approach with ORDER BY and LIMIT if you only need the top 10 results and no additional ranking operations.
For flexibility and future maintainability: Use the third approach (CTE) or fourth approach (Subquery) if you need to perform additional operations based on the rank or
 if you anticipate extending the query with more complex logic in the future.
Given these considerations, the first approach is typically the best for straightforward cases. For more complex scenarios or when you need to handle ranks explicitly,
 the third approach (CTE) is usually preferable.
  */