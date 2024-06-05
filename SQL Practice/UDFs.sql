-- UDFs

Delimiter $$

Create function profit_type(profit int)
	returns varchar(30) deterministic
    
    BEGIN
    
    Declare message varchar(30);
    if profit < -500 then set message = 'Huge loss';
    elseif profit Between -500 and 0 then set message = 'Bearable Loss';
    elseif profit between 0 and 500 then set message = 'Decent Profit';
    else set message = 'Great Profit';
    end if ;
    Return message;
    END;
    $$ 
    
    delimiter ;
    
    
    select profit_type(-1011) as udf_profit_type;
    
    use market_star_schema;
    select ord_id, prod_id, cust_id, profit, profit_type(profit) from market_fact_full limit 5;