/*
Golden layer: gets data from silver layer, applies business rules and stores in dimensional model (star schema).
It creates the data objects (views) which are ready to be used for analysis and reporting.
*/

-------------------------------
-- Customer dimension (not fact) / Join multiple silver tables to create a comprehensive customer profile
-- Assuming silver.crm_cust_info contains cleaned and transformed customer data, we use LEFT JOINs

CREATE VIEW gold.dim_customers AS
WITH customers AS ( -- Gather all customer related data from the silver tables.
    SELECT
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.cst_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 AS la ON ci.cst_key = la.cid
)
SELECT --Data integration using ci as the master table
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
cst_id AS customer_id,
cst_key AS customer_number,
cst_firstname AS first_name,
cst_lastname AS last_name,
cntry AS country,
cst_marital_status AS marital_status,
CASE 
    WHEN cst_gndr != 'n/a' THEN cst_gndr
    ELSE COALESCE(gen, 'n/a') 
END AS gender, 
bdate AS birthdate,
cst_create_date AS create_date
FROM customers;

-------------------------------
-- Product dimension / Join multiple silver tables to create a comprehensive product profile
-- Assuming silver.prod_info contains cleaned and transformed product data  

CREATE VIEW gold.dim_products AS
WITH products AS ( -- Gather all product related data from the silver tables.
    SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
    pc.id,
    pc.cat,
    pc.subcat,
    pc.maintenance
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pn.cat_id = pc.id
)
SELECT --Data integration using pn as the master table
ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key, -- Surrogate key
prd_id AS product_id,
prd_key AS product_number,
prd_nm AS product_name,
cat_id AS category_id,
cat AS category,
subcat AS subcategory,
maintenance,
prd_cost AS cost,
prd_line AS product_line,
prd_start_dt AS start_date
FROM products
WHERE prd_end_dt IS NULL; -- only active products

-------------------------------
-- Sales fact table (which connects to the dimension views above)
CREATE VIEW gold.fact_sales AS
WITH sales AS (
    SELECT
    sd.sls_ord_num,
    sd.sls_prd_key,
    pr.product_key,
    sd.sls_cust_id,
    cu.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,  
    sd.sls_quantity,
    sd.sls_price
    FROM silver.crm_sales_details AS sd
    LEFT JOIN gold.dim_products AS pr ON sd.sls_prd_key = pr.product_number
    LEFT JOIN gold.dim_customers AS cu ON sd.sls_cust_id = cu.customer_id
)
SELECT
sls_ord_num AS order_number,
product_key,
customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shipping_date,
sls_due_dt AS due_date,
sls_sales AS sales_amount,  
sls_quantity AS quantity,
sls_price AS price
FROM sales;

