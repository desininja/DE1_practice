

DELIMITER $$

CREATE PROCEDURE sales_filter (sales_input int)

BEGIN
	select cust_id,
    round(sales)
    from market_fact_full
    where round(sales) > sales_input
    order by sales;
			
END $$
DELIMITER ;


call sales_filter(300);

drop procedure sales_filter;