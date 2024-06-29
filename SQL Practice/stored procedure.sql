

DELIMITER $$

CREATE PROCEDURE sales_filter (sales_input int)

BEGIN
	SELECT 
    cust_id, ROUND(sales)
FROM
    market_fact_full
WHERE
    ROUND(sales) > sales_input
ORDER BY sales;
			
END $$
DELIMITER ;


call sales_filter(300);

drop procedure sales_filter;