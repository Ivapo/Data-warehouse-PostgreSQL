
/*
DML for Silver Layer Tables
DML: Data Manipulation Language, it is a subset of SQL used for inserting, updating, deleting, and retrieving data.
This is saved as a procedure in the database.
- Truncates and loads data into the silver layer tables from the bronze tables.
Procedure Name: silver.load_silver
Usage Example:
    CALL silver.load_silver();
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE PLPGSQL
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_duration INTERVAL;
    v_batch_start_time TIMESTAMP;
    v_batch_end_time TIMESTAMP;
BEGIN
    v_batch_start_time := clock_timestamp();

    RAISE NOTICE '============================';
    RAISE NOTICE '    Loading Silver Layer';
    RAISE NOTICE '============================';
    RAISE NOTICE 'Loading Silver Tables...';
    RAISE NOTICE '----------------------------';

    ------------------------------------------------------------------
    -- Create the silver.crm_cust_info table 
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>>> Insterting data into: silver.crm_cust_info';

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
        ) WHERE ranking = 1; -- We keep only the most recent record for each customer based on create date

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '----------------------------';

    ------------------------------------------------------------------
    -- Create the silver.crm_prd_info table 
    -------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>>> Insterting data into: silver.crm_prd_info';
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
        FROM bronze.crm_prd_info;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '----------------------------';

    -----------------------------------------------------------------
    -- Create the silver.crm_sales_details table 
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;   
    RAISE NOTICE '>>> Insterting data into: silver.crm_sales_details';
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
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '----------------------------';

    ------------------------------------------------------------------
    -- Create the silver.erp_cust_az12 table 
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>>> Insterting data into: silver.erp_cust_az12';

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

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '----------------------------';

    ------------------------------------------------------------------
    -- Create the silver.erp_loc_a101 table      
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>>> Insterting data into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
        )
        SELECT 
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN UPPER(TRIM(cntry)) IN ('GERMANY', 'DE') THEN 'Germany'
            WHEN UPPER(TRIM(cntry)) IN ('UNITED STATES', 'US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
        FROM bronze.erp_loc_a101;

    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '----------------------------';

    ------------------------------------------------------------------
    -- Create the silver.erp_px_cat_g1v2 table   , this data was clean enought and no test needed
    ------------------------------------------------------------------
    v_start_time := clock_timestamp();

    RAISE NOTICE '>>> Truncating table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>>> Insterting data into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
        )
        SELECT 
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    
    v_batch_end_time := clock_timestamp();
    v_duration := v_batch_end_time - v_batch_start_time;
    RAISE NOTICE '============================';
    RAISE NOTICE '  END Loading Silver Layer';
    RAISE NOTICE '  >>> Total duration: % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '============================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '===============================';
            RAISE WARNING 'Errors loading silver layer';
            RAISE WARNING 'Error Code: %', SQLSTATE;
            RAISE WARNING 'Error Message: %', SQLERRM;
            RAISE NOTICE '===============================';
END;
$$;

