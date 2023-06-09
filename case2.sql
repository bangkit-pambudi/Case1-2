# MYSQL Version : mysql  Ver 8.0.31-0ubuntu0.20.04.2 for Linux on x86_64
# Created by : Bagas Pambudi
# Date : 08-Juni-2023


# Drop if sp duplicate_data and unique_data for initial
DROP PROCEDURE duplicate_data;
DROP PROCEDURE unique_data;


# Create SP(Stored Procedure) For Transpose and Get count for specific column
# IN : dimension_summary(name_column, expected_value), OUT : Temp Table
CREATE PROCEDURE duplicate_data(IN n_table CHAR(255), IN num_e INT)
BEGIN
    SET @sql = CONCAT('CREATE TEMPORARY TABLE temp_dup_', n_table, ' SELECT
''null_value_of_', n_table, ''' AS test_case,
COUNT(CASE WHEN (', n_table, ' = NULL OR ', n_table, ' = '''') THEN 1 ELSE NULL END) AS actual_value,
', num_e, ' AS expected_value,  
DATE(ss.gmt_create) AS report_date
FROM (
    SELECT o.order_id,
        symbol_id,
        buyer_id,
        order_status,
        order_platform,
        o.gmt_create,
        o.gmt_modified,
        payment_id,
        pay_method,
        pay_status,
        user_id,
        registration_site,
        registration_platform,
        is_premium_user
    FROM mifx.order o
    INNER JOIN mifx.pay p ON o.order_id = p.order_id
    INNER JOIN mifx.user u ON o.buyer_id = u.user_id
) AS ss
GROUP BY
    report_date');
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;

# Create SP(Stored Procedure) For Transpose and Get count for specific column
# IN : dimension_summary(name_column, expected_value), OUT : Temp Table
CREATE PROCEDURE unique_data(IN n_table CHAR(255), IN num_e INT)
BEGIN
    SET @sql = CONCAT('CREATE TEMPORARY TABLE temp_unq_', n_table, ' SELECT
''unique_', n_table, '_count'' AS test_case,
COUNT(DISTINCT(', n_table, '))  AS actual_value,
', num_e, ' AS expected_value,  
DATE(ss.gmt_create) AS report_date
FROM (
    SELECT o.order_id,
        symbol_id,
        buyer_id,
        order_status,
        order_platform,
        o.gmt_create,
        o.gmt_modified,
        payment_id,
        pay_method,
        pay_status,
        user_id,
        registration_site,
        registration_platform,
        is_premium_user
    FROM mifx.order o
    INNER JOIN mifx.pay p ON o.order_id = p.order_id
    INNER JOIN mifx.user u ON o.buyer_id = u.user_id
) AS ss
WHERE ', n_table, ' <> ''''
GROUP BY
    report_date');
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;

# Drop if TEMPORARY TABLE for initial
DROP TEMPORARY TABLE IF EXISTS temp_dup_symbol_id;
DROP TEMPORARY TABLE IF EXISTS temp_dup_payment_platform;
DROP TEMPORARY TABLE IF EXISTS temp_dup_pay_method;
DROP TEMPORARY TABLE IF EXISTS temp_dup_is_premium_user;
DROP TEMPORARY TABLE IF EXISTS temp_unq_symbol_id;
DROP TEMPORARY TABLE IF EXISTS temp_unq_payment_platform;
DROP TEMPORARY TABLE IF EXISTS temp_unq_pay_method;
DROP TEMPORARY TABLE IF EXISTS temp_unq_is_premium_user;

# Call SP dimension_summary with input name column
CALL duplicate_data("symbol_id",0);
CALL duplicate_data("order_platform",0);
CALL duplicate_data("pay_method",0);
CALL duplicate_data("is_premium_user",0);
CALL unique_data("symbol_id",10);
CALL unique_data("order_platform",5);
CALL unique_data("pay_method",6);
CALL unique_data("is_premium_user",2);

#Create Table summary_trx_user_count and save value for summary
CREATE TABLE summary_data_quality AS
SELECT
    sss.report_date,
    sss.test_case,
    sss.actual_value,
    sss.expected_value,
    CASE WHEN (sss.actual_value = sss.expected_value) THEN 'PASSED' ELSE 'FAILED' END AS status
FROM (
select * from temp_dup_symbol_id
UNION ALL
select * from temp_dup_order_platform
UNION ALL
select * from temp_dup_pay_method
UNION ALL
select * from temp_dup_is_premium_user
UNION ALL
select * from temp_unq_symbol_id
UNION ALL
select * from temp_unq_order_platform
UNION ALL
select * from temp_unq_pay_method
UNION ALL
select * from temp_unq_is_premium_user
) as sss
order by report_date asc



