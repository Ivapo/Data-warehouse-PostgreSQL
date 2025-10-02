/*
------------------------------------------------------------------
Silver layer quality checks
------------------------------------------------------------------

This script checks the created silver layer tables for data quality. 
Each block is commented with the table name, checks and expected results.
*/

------------------------------------------------------------------  
-- Tests: for silver.crm_cust_info table
------------------------------------------------------------------

-- Check for duplicates in silver.crm_cust_info based on cst_id
SELECT cst_id, COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;
-- No rows should be returned

-- Check for unwanted spaces in cst_firstname column
SELECT cst_firstname    
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname); 
-- No rows should be returned

-- Check for unwanted spaces in cst_lastname column
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname <> TRIM(cst_lastname);
-- No rows should be returned   

-- Check normalized values in cst_marital_status column
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;  
-- Should return only: Single, Married, Divorced, n/a

-- Check normalized values in cst_gndr column
SELECT DISTINCT cst_gndr    
FROM silver.crm_cust_info;
-- Should return only: Male, Female, n/a

------------------------------------------------------------------  
-- Tests: for silver.crm_prd_info table
------------------------------------------------------------------

-- Check for duplicates or Nulls on prd_id column
SELECT prd_id, COUNT(*) AS count 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- No rows should be returned

-- Check for unwanted spaces in prd_key column
SELECT prd_key
FROM silver.crm_prd_info
WHERE prd_key <> TRIM(prd_key);
-- No rows should be returned

-- Check for unwanted spaces in prd_nm column
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);
-- No rows should be returned

-- Check for null or negative values in prd_cost column
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0; 
-- No rows should be returned

-- Check unique values in prd_line column for normalization with CASE statement and information from the experts who provided the data source
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;
-- Should return only: Mountain, Road, Touring, Other sale, n/a

-- Check the start and end dates, the start should not be null nor after the end date.
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt IS NULL OR (prd_end_dt IS NOT NULL AND prd_start_dt > prd_end_dt);
-- No rows should be returned
 
------------------------------------------------------------------
-- Tests: for silver.crm_sales_details table
------------------------------------------------------------------  

-- Check for sales = quantity * price rule since the other fields were already checked during the insert
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=sls_quantity * sls_price; 
-- No rows should be returned

------------------------------------------------------------------  
-- Tests: for silver.erp_cust_az12 table
------------------------------------------------------------------
-- Check if all cid values exist in the crm_cust_info table
SELECT        
    cid
FROM silver.erp_cust_az12
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
-- No rows should be returned

-- Check impossible birth dates (i.e., birth dates in the future which we converted to to Nulls)
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate > CURRENT_DATE;
-- No rows should be returned

-- Check normalized values in gen column
SELECT DISTINCT gen
FROM silver.erp_cust_az12;      
-- Should return: Male, Female, n/a

------------------------------------------------------------------  
-- Tests: for silver.erp_loc_a101 table     
------------------------------------------------------------------

-- Check if all cid values exist in the erp_cust_az12 table
SELECT        
    cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT DISTINCT cid FROM silver.erp_cust_az12);
-- No rows should be returned

-- Check normalized values in cntry column
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;   
-- Should return: Germany, United States, n/a, and full names of countries, not abbreviations
