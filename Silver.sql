-- Silver Layer 
-- clean and transform

-- because its another page, we need to specify the database name
USE DataWarehouse;
GO
-- CREATE SCHEMA silver;
-- GO

-- Create Silver Tables 
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_giv2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_giv2;
GO
CREATE TABLE silver.erp_px_cat_giv2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

--=========================
--      crm_cust_info
--=========================

-- check for null or duplicate in primary key 
-- expectation: no result 
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1; -- result was not satisfactory

-- Duplicate 
-- closer look by spot checking a specific cst_id
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id = 29466; 
--sort all records by create date descending to see which is the latest record
SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info

------------------------------------------------------------------
-- remove duplicates and only keep the latest which is the most relevant record for each cst_id by filtering 
------------------------------------------------------------------
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- check for Unwanted space in string values
-- expectation: no result
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); -- not satisfy
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); -- not satisfy
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr); -- satisfy 

-- check consistency of values in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info; -- not satisfy

------------------------------------------------------------------
-- Transformation to remove unwanted spaces normalizing data to readable format, handling missing
------------------------------------------------------------------

TRUNCATE TABLE silver.crm_cust_info;
GO
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,   
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,

    CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
    END AS cst_material_status,
    
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1
)s;

-- check result 
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); -- satisfy
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname); -- satisfy
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info; -- satisfy
SELECT * FROM silver.crm_cust_info; -- final check good 

--=========================
--      crm_prd_info
--=========================
SELECT prd_id,
    prd_key,
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 

-- check for duplicates or null in prd_id
-- expectation: no result
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; -- satisfy

-- check if key pattern is consistent
SELECT prd_id,
    prd_key,
    SUBSTRING(prd_key,1,5) AS cat_id, 
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 
-- check if key pattern is consistent
-- expectation: CO-RF
SELECT distinct id FROM bronze.erp_px_cat_giv2; -- not satisfy, AC_BC

------------------------------------------------------------------
-- replace - with _
------------------------------------------------------------------
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 
-- check result, by comparing with disti
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info 
    WHERE REPLACE(SUBSTRING(prd_key,1,5), '-', '_') NOT IN 
    (SELECT distinct id from bronze.erp_px_cat_giv2); 
    -- only CO_RF is not matched, this is how it should be since it was't in bronze.erp_px_cat_giv2

-- extract second part of prd_key which is after 6th letter
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 
-- check result, which product keys (after cleaning) don't appear in the sales details table.
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info 
    WHERE SUBSTRING(prd_key,7,LEN(prd_key)) NOT IN 
    (SELECT sls_prd_key from bronze.crm_sales_details );

-- check if unwanted spaces in prd_nm
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- expect no result, satisfy

 -- check if nulls or negative numers in prd_cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0; -- expect no result, 2 nulls
------------------------------------------------------------------
-- tranforming prd_cost nulls to 0
------------------------------------------------------------------
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 

-- check data standarization and consitency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;
------------------------------------------------------------------
-- normalizing prd_line data to readable format
------------------------------------------------------------------
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    CASE WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    prd_end_dt 
    FROM bronze.crm_prd_info ; 
-- simplier or shorter form of the same above
/* CASE  UPPER(TRIM(prd_line)) 
        WHEN 'S' THEN 'Other Sales'
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,*/

-- Check for invalid date order, end date must not be earlier than start date
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt; -- expect no result, not satisfy
-- after looking closer, decide to switch end date to start date 
-- notice over laping on dates and null start date (null end date is ok))
-- decide to correct by end date = start date of next record -1
SELECT
    prd_id,
    prd_key,
    prd_nm ,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test 
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');
------------------------------------------------------------------
-- final transformation 
------------------------------------------------------------------
SELECT prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    ISNULL(prd_cost, 0) AS prd_cost,
    prd_line,
    CASE WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_dt
    FROM bronze.crm_prd_info ; 


------------------------------------------------------------------
-- Update by include cat_id and change datetime to date 
------------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),    
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- final insert into silver table
-------------------------------------------------------------------
TRUNCATE TABLE silver.crm_prd_info;
GO
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm ,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT prd_id,
    REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
    prd_nm ,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
         WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_dt
    FROM bronze.crm_prd_info ; 

----------------------------------------------------
-- final result check 
----------------------------------------------------
-- check for duplicates or null in prd_id
-- expectation: no result
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; -- satisfy
-- check if unwanted spaces in prd_nm
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- expect no result, satisfy
 -- check if nulls or negative numers in prd_cost
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0; -- expect no result, satisfy
-- check data standarization and consitency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;
-- Check for invalid date order, end date must not be earlier than start date
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt; -- expect no result, not satisfy
-- overall check 
SELECT * FROM silver.crm_prd_info;


--=========================
--      crm_sales_details
--=========================
-- see if crm_sales_details connect with crm_prd_info by prd_key
-- expectation: no result, satisfy
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
-- see if crm_sales_details connect with crm_prd_info by prd_key
-- expectation: no result, satisfy
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

-- check for valid date, negative or zero can't be cast to a date
-- check length of date must be 8 digits
-- check outlier by validating boundaries of date range 
SELECT 
sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt>20500101; -- expect no result, not satisfy
-------------------------------------------------
-- transform negative or zero date to null for dates
-------------------------------------------------
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details

-- check for invalid date order 
SELECT *
FROM bronze.crm_sales_details
WHERE sls_due_dt < sls_order_dt OR sls_ship_dt < sls_order_dt; -- expect no result, satisfy

-- check for sum sales = quantity * price
-- check negative, zeros, nulls 
SELECT 
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0 -- expect no result, not satisfy
ORDER BY sls_sales, sls_quantity, sls_price;

-- if sales is negative, zero or null derive it using quanitty and price
-- if price is zero or null calculate it using sales and quantity
-- if quantity is negative convert to positive 
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* abs(sls_price) 
    THEN sls_quantity * abs(sls_price)
    ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0 
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0 -- expect no result, not satisfy
ORDER BY sls_sales, sls_quantity, sls_price;

-------------------------------------------------
-- transform sales and price to correct values
-------------------------------------------------
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* abs(sls_price) 
    THEN sls_quantity * abs(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0 
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details 

-------------------------------------------------
-- change int to date
-------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    -- meta data columns, exctra columnns added for tracking purpose
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

---------------------------------------------------
-- final insert into silver table
---------------------------------------------------
TRUNCATE TABLE silver.crm_sales_details;
GO
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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
     ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* abs(sls_price) 
    THEN sls_quantity * abs(sls_price)
    ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <=0 
    THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details 

----------------------------------------------------
-- final result check
----------------------------------------------------
-- check for invalid date order 
SELECT *
FROM silver.crm_sales_details
WHERE sls_due_dt < sls_order_dt OR sls_ship_dt < sls_order_dt; -- expect no result, satisfy
-- check for sum sales = quantity * price
-- check negative, zeros, nulls 
SELECT 
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0 -- expect no result, not satisfy
ORDER BY sls_sales, sls_quantity, sls_price;

--=========================
--      erp_cust_az12
--=========================
SELECT 
cid,
bdate,
gen
FROM bronze.erp_cust_az12

----------------------------------------------------
-- clean unnecessary letters NAS in cid
----------------------------------------------------
SELECT 
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
ELSE cid 
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
ELSE cid  --- check if all cleaned cid appear in silver.crm_cust_info
-- expectation: no result means all cid are matched
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- check for invalid birth date such as older than 100 year old or born in future 
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE(); -- expect no result, not satisfy

 -- decided to convert invalid birth date to null
 SELECT 
 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
    ELSE cid
    END AS cid,
 CASE WHEN bdate > GETDATE() THEN NULL 
    ELSE bdate
    END AS bdate,
gen
FROM bronze.erp_cust_az12

-- check for consistency
SELECT DISTINCT gen AS gen
FROM bronze.erp_cust_az12; -- saw null and empty values, not satisfy

-- should work but not working 
-- but i have unprintable characters in that data so have to use another way 
SELECT DISTINCT
gen,
CASE 
  WHEN UPPER(REPLACE(TRIM(gen), ' ', '')) IN ('F', 'FEMALE') THEN 'Female'
  WHEN UPPER(REPLACE(TRIM(gen), ' ', '')) IN ('M', 'MALE') THEN 'Male'
  ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12;

-- this works to see unprintable characters
SELECT DISTINCT
  CASE
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen
FROM bronze.erp_cust_az12;

-- decided to remove unwanted spaces only 
SELECT 
 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
    ELSE cid
    END AS cid,
 CASE WHEN bdate > GETDATE() THEN NULL 
    ELSE bdate
    END AS bdate,
CASE
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen
FROM bronze.erp_cust_az12

-- final insert into silver table
TRUNCATE TABLE silver.erp_cust_az12;
GO
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
 CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
    ELSE cid
    END AS cid,
 CASE WHEN bdate > GETDATE() THEN NULL 
    ELSE bdate
    END AS bdate,
CASE
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('M','MALE') THEN 'Male'
    ELSE 'n/a'
  END AS gen
FROM bronze.erp_cust_az12

-- final result check
SELECT DISTINCT gen AS gen
FROM silver.erp_cust_az12; -- satisfy
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();-- satisfy


--=========================
--      erp_loc_a101
--=========================
SELECT 
cid,
cntry
FROM bronze.erp_loc_a101
 -- cid is used to connect with cst_key and their don't have - so need to clean

-- check if clean working 
SELECT 
REPLACE(cid,'-','') AS cid,
cntry
FROM bronze.erp_loc_a101 WHERE REPLACE(cid,'-','') NOT IN 
(SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- check data consistency 
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry; -- saw unwanted spaces and different format, not satisfy


-- transform to cleaned country names
-- this looks complex but is had unprintable characters to deal with
SELECT 
REPLACE(cid,'-','') AS cid,
    CASE
        -- Standardize country codes after cleaning
        WHEN UPPER(cleaned_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(cleaned_cntry) IN ('US','USA') THEN 'United States'
        WHEN UPPER(cleaned_cntry) = 'UNITEDKINGDOM' THEN 'United Kingdom'
        WHEN LEN(cleaned_cntry) = 0 THEN 'n/a'
        ELSE
            -- Add space for other United* countries
            CASE 
                WHEN LEFT(cleaned_cntry,6) = 'United' THEN
                    REPLACE(cleaned_cntry,'United','United ')
                ELSE cleaned_cntry
            END
    END AS cntry_cleaned
FROM (
    -- Step 1: Remove all unprintable characters
    SELECT
        cid,
        REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cntry), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '') AS cleaned_cntry
    FROM bronze.erp_loc_a101
) AS cntry;

-- final insert into silver table
TRUNCATE TABLE silver.erp_loc_a101;
GO
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
REPLACE(cid,'-','') AS cid,
    CASE
        -- Standardize country codes after cleaning
        WHEN UPPER(cleaned_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(cleaned_cntry) IN ('US','USA') THEN 'United States'
        WHEN UPPER(cleaned_cntry) = 'UNITEDKINGDOM' THEN 'United Kingdom'
        WHEN LEN(cleaned_cntry) = 0 THEN 'n/a'
        ELSE
            -- Add space for other United* countries
            CASE 
                WHEN LEFT(cleaned_cntry,6) = 'United' THEN
                    REPLACE(cleaned_cntry,'United','United ')
                ELSE cleaned_cntry
            END
    END AS cntry_cleaned
FROM (
    -- Step 1: Remove all unprintable characters
    SELECT
        cid,
        REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cntry), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '') AS cleaned_cntry
    FROM bronze.erp_loc_a101
) AS cntry;

-- final result check
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry; -- satisfy

--=========================
--      erp_px_cat_giv2
--=========================
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_giv2

------------------------------------------------------------------
-- delete unprintable characters from maintenance
------------------------------------------------------------------
SELECT
id,
cat,
subcat,
REPLACE(REPLACE(REPLACE(REPLACE(TRIM(maintenance), ' ', ''), CHAR(  9), ''), CHAR(10), ''), CHAR(13), '') AS maintenance
FROM bronze.erp_px_cat_giv2

-- check for unwanted spaces 
SELECT * FROM bronze.erp_px_cat_giv2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance); -- expect no result, satisfy


------------------------------------------------------------------
-- check consistency for id, cat, subcat or maintenance
------------------------------------------------------------------
SELECT DISTINCT id
FROM bronze.erp_px_cat_giv2; -- satisfy 
-- etc for cat, subcat
SELECT DISTINCT
REPLACE(REPLACE(REPLACE(REPLACE(TRIM(maintenance), ' ', ''), CHAR(  9), ''), CHAR(10), ''), CHAR(13), '') AS maintenance_cleaned
FROM bronze.erp_px_cat_giv2; -- saw unwanted spaces and different format, not satisfy


------------------------------------------------------------------
-- insert into silver table 
------------------------------------------------------------------
TRUNCATE TABLE silver.erp_px_cat_giv2;
GO
INSERT INTO silver.erp_px_cat_giv2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
id,
cat,
subcat,
    REPLACE(REPLACE(REPLACE(REPLACE(TRIM(maintenance), ' ', ''), CHAR(  9), ''), CHAR(10), ''), CHAR(13), '') AS maintenance
FROM bronze.erp_px_cat_giv2

--------------------------------------------------------------
--------------------------------------------------------------
-- Store procedure 
--------------------------------------------------------------
--------------------------------------------------------------
GO

CREATE OR ALTER PROCEDURE silver.load_silver 
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        set @batch_start_time = GETDATE();
            PRINT '================================';
            PRINT ' Loading Silver Layer Data';
            PRINT '================================';
            PRINT '--------------------------------';
            PRINT ' Loading CRM Tables';
            PRINT '--------------------------------';
        set @start_time= GETDATE();
        TRUNCATE TABLE silver.crm_cust_info;
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,   
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,

            CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_material_status,
            
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        )t WHERE flag_last = 1
        )s;
        set @end_time = GETDATE();
        PRINT 'crm_cust_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';

        set @start_time= GETDATE();
        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm ,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT prd_id,
            REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, 
            SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
            prd_nm ,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_dt
            FROM bronze.crm_prd_info ; 
        set @end_time = GETDATE();
                PRINT 'crm_prd_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '--------------------------------';

        set @start_time= GETDATE();
        TRUNCATE TABLE silver.crm_sales_details;
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
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* abs(sls_price) 
            THEN sls_quantity * abs(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <=0 
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details ;
        set @end_time = GETDATE();
                PRINT 'crm_sales_details load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '--------------------------------';

        PRINT '--------------------------------';
        PRINT ' Loading ERP Tables';
        PRINT '--------------------------------';
        set @start_time= GETDATE();
        TRUNCATE TABLE silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
            ELSE cid
            END AS cid,
        CASE WHEN bdate > GETDATE() THEN NULL 
            ELSE bdate
            END AS bdate,
        CASE
            WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(REPLACE(REPLACE(REPLACE(REPLACE(gen,' ',''), CHAR(9),''), CHAR(13),''), CHAR(10),'')) IN ('M','MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12
        set @end_time = GETDATE();
                PRINT 'erp_cust_az12 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '--------------------------------';

        set @start_time= GETDATE();
        TRUNCATE TABLE silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
        REPLACE(cid,'-','') AS cid,
            CASE
                -- Standardize country codes after cleaning
                WHEN UPPER(cleaned_cntry) = 'DE' THEN 'Germany'
                WHEN UPPER(cleaned_cntry) IN ('US','USA') THEN 'United States'
                WHEN UPPER(cleaned_cntry) = 'UNITEDKINGDOM' THEN 'United Kingdom'
                WHEN LEN(cleaned_cntry) = 0 THEN 'n/a'
                ELSE
                    -- Add space for other United* countries
                    CASE 
                        WHEN LEFT(cleaned_cntry,6) = 'United' THEN
                            REPLACE(cleaned_cntry,'United','United ')
                        ELSE cleaned_cntry
                    END
            END AS cntry_cleaned
        FROM (
            -- Step 1: Remove all unprintable characters
            SELECT
                cid,
                REPLACE(REPLACE(REPLACE(REPLACE(TRIM(cntry), ' ', ''), CHAR(9), ''), CHAR(10), ''), CHAR(13), '') AS cleaned_cntry
            FROM bronze.erp_loc_a101
        ) AS cntry;
        set @end_time = GETDATE();
                PRINT 'erp_loc_a101 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '--------------------------------';
        
        set @start_time= GETDATE();
        TRUNCATE TABLE silver.erp_px_cat_giv2;
        INSERT INTO silver.erp_px_cat_giv2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
        id,
        cat,
        subcat,
            REPLACE(REPLACE(REPLACE(REPLACE(TRIM(maintenance), ' ', ''), CHAR(  9), ''), CHAR(10), ''), CHAR(13), '') AS maintenance
        FROM bronze.erp_px_cat_giv2
        set @end_time = GETDATE();
                PRINT 'erp_px_cat_giv2 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                PRINT '--------------------------------';


    SET @batch_end_time = GETDATE();
        PRINT '================================';
        PRINT ' Silver Layer Data Load Completed.';
        PRINT ' - Total silver Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================';
        END TRY
    BEGIN CATCH 
        PRINT'================================';
        PRINT 'Error occurred while loading silver layer tables.';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT'================================';
    END CATCH 
END;

EXEC silver.load_silver;
GO
