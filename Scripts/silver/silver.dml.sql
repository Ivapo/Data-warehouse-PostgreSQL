------------------------------------------------------------------
-- silver.crm_cust_info table
------------------------------------------------------------------  

-- Check for duplicates in bronze.crm_cust_info based on cst_id
SELECT cst_id, COUNT(*) AS count 
FROM bronze.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- When I check i.e cst_id = 29466 there are 3 but one has a more recent date
SELECT * 
FROM bronze.crm_cust_info 
WHERE cst_id = 29466;

-- So I will keep the most recent one, since I assume it has the 'best' data, based on cst_create_date by assigning a value to an additional column, where ranking = 1 is the most recent. Also, filter out NULL cst_id values. Then i have unique cst_id values.
SELECT * 
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking --This is a window function, assigns a row number for the partition
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) WHERE ranking = 1;

-- Check for unwanted spaces in string columns
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname)

-- So i use the TRIM fucntion to remove unwanted spaces, and I also modifify the gender and martial status columns to have full name values (i.e., Male/Female, Single/Married/Divorced)

------------------------------------------------------------------
-- Create the silver.crm_cust_info table 
------------------------------------------------------------------

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE upper(TRIM(cst_marital_status))
        WHEN 'S' THEN 'Single'
        WHEN 'M' THEN 'Married'
        WHEN 'D' THEN 'Divorced'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE upper(TRIM(cst_gndr))
        WHEN 'M' THEN 'Male'
        WHEN 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS ranking --This is a window function, assigns a row number for the partition
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) WHERE ranking = 1;

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
-- silver.crm_prd_info table 
------------------------------------------------------------------

-- Check for duplicates or Nulls in bronze.crm_prd_info based on prd_id
SELECT prd_id, COUNT(*) AS count 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- No duplicates or Nulls found

-- Check for unwanted spaces in prd_key column
SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key <> TRIM(prd_key);
-- None found, no need to use TRIM function

-- Check for unwanted spaces in prd_nm column
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);
-- None found, no need to use TRIM function

-- Check for null or negative values in prd_cost column
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0; 
-- Got no negative values, but some nulls, which we can replace with a zero value

-- Check unique values in prd_line column for normalization with CASE statement and information from the experts who provided the data source
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
-- Values found: M, R, T, S, null. We will replace them with full names

-- Check the start and end dates, the start should not be null nor after the end date, and for the same products the dates should not overlap, so the start date of a new product should be after the end date of the previous one plus one day.
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt IS NULL OR (prd_end_dt IS NOT NULL AND prd_start_dt > prd_end_dt);
-- This returns 200 rows which is wrong, and doesnt even check overlaps on dates. We will assume that the end dates are wrong.

-- So we will now check for null start dates
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt IS NULL;
-- we got 0 rows, so no null start dates, meaning we can use them to create end dates column.

------------------------------------------------------------------
-- So we will create the silver.crm_prd_info table 
------------------------------------------------------------------

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
    SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- The replace is done to match the erp_px_cat_g1v2 table format of product categories
    SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost, 
    CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        WHEN 'S' THEN 'Other sale'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
    FROM bronze.crm_prd_info

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
-- silver.sales_details table 
------------------------------------------------------------------

-- Check for duplicates or Nulls in bronze.crm_sales_details based on sls_ord_num   
SELECT sls_ord_num, COUNT(*) AS count 
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL;
-- Many duplicates, no Nulls found .. no need to get rid of ducplicates atm

-- Check for unwanted spaces in sls_prd_key column
SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key <> TRIM(sls_prd_key);
-- None found, no need to use TRIM function

-- Check for Nulls in sls_cust_id column 
SELECT sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id IS NULL;
-- None found, no need to filter them out

-- Check for Nulls, zeros or negative values in sls_order_dt column   
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL OR sls_order_dt = 0 OR sls_order_dt < 0;
-- Found some zeros, we convert them to Nulls

-- Check for numbers with length different than 8 in sls_order_dt column, this case covers the previous one as well
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE LENGTH(CAST(sls_order_dt AS TEXT)) <> 8;
-- Some found, we convert them to Nulls

-- Check for Nulls in sls_ship_dt column    
SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt IS NULL;  
-- None found, no need to filter them out, but these should be casted into date type

-- Check for Nulls in sls_due_dt column
SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt IS NULL;
-- None found, no need to filter them out, but these should be casted into date type

-- There is a rule: Sum of sales = quantity * price, so we will check if this is true, no nulls, zeros or negative values should be present in these columns
-- Check for negative values in sls_sales column and for Nulls
SELECT sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales < 0;
-- There are some Nulls and some negative values.

-- Check for negative values in sls_quantity column and for Nulls
SELECT sls_quantity
FROM bronze.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity < 0;
-- None found, no need to filter them out.

-- Check for negative values in sls_price column and for Nulls  
SELECT sls_price
FROM bronze.crm_sales_details
WHERE sls_price IS NULL OR sls_price < 0;
-- There are some Nulls and some negative values.


------------------------------------------------------------------
-- So we will create the silver.crm_sales_details table 
------------------------------------------------------------------

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN LENGTH(CAST(sls_order_dt AS TEXT)) <> 8 THEN NULL
    ELSE TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
END AS sls_order_dt,
CASE 
    WHEN LENGTH(CAST(sls_ship_dt AS TEXT)) <> 8 THEN NULL
    ELSE TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
END AS sls_ship_dt,
CASE 
    WHEN LENGTH(CAST(sls_due_dt AS TEXT)) <> 8 THEN NULL
    ELSE TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
END AS sls_due_dt,
CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0) -- to avoid division by zero
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details;
 
------------------------------------------------------------------
-- Tests: for silver.crm_sales_details table
------------------------------------------------------------------  

-- Check for sales = quantity * price rule since the other fields were already checked during the insert
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=sls_quantity * sls_price; 
-- No rows should be returned

------------------------------------------------------------------
-- silver.erp_cust_az12 table
------------------------------------------------------------------

-- In the cid column, we can check if the values look like the customer ids from the crm_cust_info table, since they can be connected. One way to check if there is something that looks like a customer key from table crm_cust_info in this table, This is one customer key: AW00011000, i need to add % to make it a pattern
SELECT
cid
FROM bronze.erp_cust_az12
WHERE cid LIKE '%AW00011000%';
-- We got one row, which cid has 'NAS' prefix. which we will remove.

-- We can also look for cid that dont exist in the crm_cust_info table
SELECT
cid
FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);
-- we get many rows, and we want none back, so we remove the prefix 'NAS' from the cid values

-- Check impossible birth dates (i.e., birth dates in the future which we will convert to Nulls)
SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate > CURRENT_DATE;
-- There are some rows, we will convert them to Nulls

-- Check the unique values in the gen column for normalization
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
-- Values found: M, F, Male, Female, M, F, Null, and empty. We will normalize them to Male, Female, or n/a like in the cust_info table

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


------------------------------------------------------------------
-- So we will create the silver.erp_cust_az12 table 
------------------------------------------------------------------

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)  
    SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid)) 
        ELSE cid 
    END AS cid,
    CASE 
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) = 'M' OR UPPER(TRIM(gen)) = 'MALE' THEN 'Male'
        WHEN UPPER(TRIM(gen)) = 'F' OR UPPER(TRIM(gen)) = 'FEMALE' THEN 'Female'
        ELSE 'n/a'
    END AS gen
    FROM bronze.erp_cust_az12;

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
