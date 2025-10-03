/*
Golden layer tests: checks for some duplicates and nulls. 
More tests can be added as needed.
*/

-------------------------------
-- Customer dimension view checks
 
-- check for duplicates in the customer ids - none should be found
SELECT cst_id, COUNT(*) FROM ( 
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
)t
GROUP BY cst_id
HAVING COUNT(*) > 1; 

-------------------------------
-- Product dimension view checks

-- check for duplicates in the product keys - none should be found
SELECT prd_key , COUNT(*) FROM (
    SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
    FROM silver.crm_prd_info AS pn
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL)t
GROUP BY prd_key
HAVING COUNT(*) > 1;

-------------------------------
-- Sales fact view checks

-- check for null in the liking surrogate keys - none should be found
Select *
From gold.fact_sales AS F 
LEFT JOIN gold.dim_customers AS C ON C.customer_key = F.customer_key
LEFT JOIN gold.dim_products AS P ON P.product_key = F.product_key
Where P.product_key is null OR C.customer_key is null; 