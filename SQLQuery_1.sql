USE master;
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO 

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

--- optional code to check if the table aready exists and drop it before creating new tables
--- U stands for user defined table 
--IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    --DROP TABLE bronze.crm_cust_info;

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
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

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_giv2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_giv2;
CREATE TABLE bronze.erp_px_cat_giv2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO


--- Mac can't run sql server so need to use docker to run sql server container
--- Since Using docker with Mac so need to run to load data from local file system to sql server container
--- run this in terminal to copy file from local to container
-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv" sqlserver:/var/opt/mssql/data/
-- Now, path to this file is '/var/opt/mssql/data/cust_info.csv'

-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv" sqlserver:/var/opt/mssql/data/
-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv" sqlserver:/var/opt/mssql/data/
-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv" sqlserver:/var/opt/mssql/data/
-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv" sqlserver:/var/opt/mssql/data/
-- docker cp "/Users/chloe/Downloads/Project/Database Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv" sqlserver:/var/opt/mssql/data/



--- creating stored procedure to load data into bronze tables
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

--- excute the stored procedure to load data
-- EXEC bronze.load_bronze;
-- GO
EXEC bronze.load_bronze;
