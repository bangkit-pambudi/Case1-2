# MYSQL Version : mysql  Ver 8.0.31-0ubuntu0.20.04.2 for Linux on x86_64
# Created by : Bagas Pambudi
# Date : 08-Juni-2023

# Drop if sp dimension_summary for initial
DROP PROCEDURE IF EXISTS dimension_summary;

# Create SP(Stored Procedure) For Transpose and Get all dimension for specific column
# IN : dimension_summary(name_column), OUT : Temp Table
CREATE PROCEDURE dimension_summary(IN n_column CHAR(255))
BEGIN
    SET @sql = CONCAT('CREATE TEMPORARY TABLE temp_sum_', n_column, ' SELECT 
''', n_column, ''' AS dimension_type, 
CASE WHEN (ss.', n_column, ' = '''' or ss.', n_column, ' = NULL or ss.', n_column, ' = ''NULL'') THEN ''OTHERS'' ELSE ss.', n_column, ' END AS dimension_value,
ss.order_id AS order_id,
ss.buyer_id AS buyer_id,
ss.order_status AS order_status,
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
) AS ss');
    
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;

# Drop if TEMPORARY TABLE for initial
DROP TEMPORARY TABLE IF EXISTS temp_sum_registration_site;
DROP TEMPORARY TABLE IF EXISTS temp_sum_registration_platform;
DROP TEMPORARY TABLE IF EXISTS temp_sum_is_premium_user;
DROP TEMPORARY TABLE IF EXISTS temp_sum_pay_status;
DROP TEMPORARY TABLE IF EXISTS temp_sum_pay_method;
DROP TEMPORARY TABLE IF EXISTS temp_sum_order_platform;
DROP TEMPORARY TABLE IF EXISTS temp_sum_order_status;
DROP TEMPORARY TABLE IF EXISTS temp_sum_symbol_id;

# Call SP dimension_summary with input name column
CALL dimension_summary("registration_site");
CALL dimension_summary("registration_platform");
CALL dimension_summary("is_premium_user");
CALL dimension_summary("pay_status");
CALL dimension_summary("pay_method");
CALL dimension_summary("order_platform");
CALL dimension_summary("order_status");
CALL dimension_summary("symbol_id");


#Create Table summary_trx_user_count and save value for summary
CREATE TABLE summary_trx_user_count AS
SELECT
    sss.report_date,
    sss.dimension_type AS dimension_type,
    sss.dimension_value AS dimension_value,
    COUNT(DISTINCT sss.order_id) AS all_trx_count,
    COUNT(DISTINCT sss.buyer_id) AS all_trx_user_count,
    COUNT(DISTINCT CASE WHEN sss.order_status = 'SUCCESS' THEN sss.order_id END) AS success_trx_count,
    COUNT(DISTINCT CASE WHEN sss.order_status = 'SUCCESS' THEN sss.buyer_id END) AS success_trx_user_count
FROM (
select * from temp_sum_registration_site
UNION ALL
select * from temp_sum_registration_platform
UNION ALL
select * from temp_sum_is_premium_user
UNION ALL
select * from temp_sum_pay_status
UNION ALL
select * from temp_sum_pay_method
UNION ALL
select * from temp_sum_order_platform
UNION ALL
select * from temp_sum_order_status
UNION ALL
select * from temp_sum_symbol_id) AS sss
GROUP BY
    sss.report_date,
    sss.dimension_type,
    sss.dimension_value;