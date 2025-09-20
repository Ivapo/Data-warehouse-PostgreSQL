/*
DML for Bronze Layer Tables
DML: Data Manipulation Language, it is a subset of SQL used for inserting, updating, deleting, and retrieving data.
This is saved as a procedure in the database.
- Truncates and loads data into the bronze layer tables from CSV files.
Procedure Name: bronze.load_data
Usage Example:
    CALL bronze.load_data();
*/

CREATE OR REPLACE PROCEDURE bronze.load_data()
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
    RAISE NOTICE '    Loading Bronze Layer';
    RAISE NOTICE '============================';
    RAISE NOTICE 'Loading CRP Tables...';
    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>>> Insterting data into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');

    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>>> Insterting data into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');

    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>>> Insterting data into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    
    RAISE NOTICE '----------------------------';

    RAISE NOTICE 'Loading ERP Tables...';
    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>>> Insterting data into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');

    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>>> Insterting data into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    
    RAISE NOTICE '----------------------------';
    v_start_time := clock_timestamp();
    RAISE NOTICE '>>> Truncating table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>>> Insterting data into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:/Users/pivan/Documents/Repos/Data-warehouse-PostgreSQL/Datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT CSV,
        HEADER TRUE,
        DELIMITER ','
    );
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    RAISE NOTICE '>>> -> % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    v_batch_end_time := clock_timestamp();
    v_duration := v_batch_end_time - v_batch_start_time;
    RAISE NOTICE '============================';
    RAISE NOTICE '  END Loading Bronze Layer';
    RAISE NOTICE '  >>> Total duration: % seconds', to_char(EXTRACT(EPOCH FROM v_duration),'FM999999990.000');
    RAISE NOTICE '============================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '===============================';
            RAISE WARNING 'Errors loading bronze layer';
            RAISE WARNING 'Error: %', SQLERRM;
            RAISE WARNING 'State: %', SQLSTATE; 
            RAISE NOTICE '===============================';
END;
$$;

