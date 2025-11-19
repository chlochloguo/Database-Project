
/*=========================================
Store Procedure : Load Bronze Layer Tables
===========================================
excute the stored procedure to load data into bronze tables
example of use:
EXEC bronze.load_bronze;
GO

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    BEGIN TRY
        PRINT '================================';
        PRINT ' Loading Bronze Layer Data';
        PRINT '================================';
        PRINT '--------------------------------';
        PRINT ' Loading CRM Tables';
        PRINT '--------------------------------';

        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
        SET @batch_start_time = GETDATE();
        SET @start_time = GETDATE();
        --- clean table if data already exists
        PRINT '<< Truncating existing data in bronze.crm_cust_info >>';
        TRUNCATE TABLE bronze.crm_cust_info;
        --- load data into the table from csv file
        PRINT '<< Inserting data into bronze.crm_cust_info >>';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'crm_cust_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';   
        PRINT '--------------------------------';

        --- check if data is correctly loaded 
        -- SELECT * FROM bronze.crm_cust_info;
        PRINT '<< Truncating existing data in bronze.crm_prd_info >>';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '<< Inserting data into bronze.crm_prd_info >>';
        SET @start_time = GETDATE();
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'crm_prd_info load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';   
        PRINT '--------------------------------';

        PRINT '<< Truncating existing data in bronze.crm_sales_details >>';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '<< Inserting data into bronze.crm_sales_details >>';
        SET @start_time = GETDATE();
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'crm_sales_details load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';   
        PRINT '--------------------------------';

        PRINT '--------------------------------';
        PRINT ' Loading ERP Tables';
        PRINT '--------------------------------';
        PRINT '<< Truncating existing data in bronze.erp_cust_az12 >>';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '<< Inserting data into bronze.erp_cust_az12 >>';
        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'erp_cust_az12 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';   
        PRINT '--------------------------------';

        PRINT '<< Truncating existing data in bronze.erp_loc_a101 >>';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '<< Inserting data into bronze.erp_loc_a101 >>';
        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'erp_loc_a101 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';   
        PRINT '--------------------------------';

        PRINT '<< Truncating existing data in bronze.erp_px_cat_giv2 >>';
        TRUNCATE TABLE bronze.erp_px_cat_giv2;
        PRINT '<< Inserting data into bronze.erp_px_cat_giv2 >>';
        SET @start_time = GETDATE();
        BULK INSERT bronze.erp_px_cat_giv2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '--------------------------------';
        PRINT 'erp_px_cat_giv2 load duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------------------';
        SET @batch_end_time = GETDATE();
        PRINT '================================';
        PRINT ' Bronze Layer Data Load Completed.';
        PRINT ' - Total Bronze Layer Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================';
    END TRY
    BEGIN CATCH 
        PRINT'================================';
        PRINT 'Error occurred while loading bronze layer tables.';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT'================================';
    END CATCH 
END;




/*=========================================
Store Procedure : Load Silver Layer Tables
===========================================
excute the stored procedure to perform ETL process, load transformed data from bronze into silver tables
example of use:
EXEC silver.load_silver;
GO

*/
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

