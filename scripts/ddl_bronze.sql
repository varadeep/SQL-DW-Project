/*
--------------------------------------------------------------------
DDL Script: Create Bronze tables
--------------------------------------------------------------------
Script Purpose:
  This Script Creates tables in the  'bronze', dropping existing tables
  if they already exist.
  Run this script to re-define the DDL Structure Of  'bronze' tables
--------------------------------------------------------------------
*/

--------------------DDL SCRIPT FOR CRM DATA----------------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info
(
cst_id		INT,
cst_key		VARCHAR(50),
cst_firstname	VARCHAR(50),
cst_lastname	VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gndr	VARCHAR(50),
cst_create_date	DATE
);
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info
(
prd_id		INT,
prd_key		VARCHAR(50),
prd_nm		VARCHAR(50),
prd_cost	INT,
prd_line	VARCHAR(50),
prd_start_dt	TIMESTAMP,
prd_end_dt 	TIMESTAMP
);
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details
(
sls_ord_num		VARCHAR(50),
sls_prd_key		VARCHAR(50),
sls_cust_id		INT,
sls_order_dt	INT,
sls_ship_dt		INT,
sls_due_dt		INT,
sls_sales		INT,
sls_quantity	INT,
sls_price		INT
);
--------------------DDL SCRIPT FOR ERP DATA----------------------------
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12
(
cid		VARCHAR(50),
bdate	DATE,
gen	VARCHAR(50)

);
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101
(
cid		VARCHAR(50),
cntry	VARCHAR(50)
);
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2
(
id	VARCHAR(50),
cat	VARCHAR(50),
subcat	VARCHAR(50),
maintenance VARCHAR(50)
);


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  error_message TEXT;
BEGIN
  RAISE NOTICE '--------------------------------------';
  RAISE NOTICE '---------Loading Bronze Layer---------';
  RAISE NOTICE '--------------------------------------';

  start_time := clock_timestamp(); -- Record start time for the entire process

  -- CRM Data Loading
  RAISE NOTICE '-----------Loading CRM DATA-----------';
  BEGIN  -- Begin a block for CRM data loading (for specific error handling if needed)
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>>Truncated Table: bronze.crm_cust_info.';
    COPY bronze.crm_cust_info
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_cust_info.';

    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>>Truncated Table: bronze.crm_prd_info.';
    COPY bronze.crm_prd_info
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_prd_info.';

    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>>Truncated Table :bronze.crm_sales_details.';
    COPY bronze.crm_sales_details
    FROM 'V:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.crm_sales_details.';
  EXCEPTION WHEN OTHERS THEN
      error_message := SQLERRM;
      RAISE NOTICE 'Error loading CRM data: %', error_message;
      -- You might want to RAISE EXCEPTION here to stop the whole process, or just continue
  END; -- End of CRM data loading block


  -- ERP Data Loading
  RAISE NOTICE '--------------------------------------';
  RAISE NOTICE '-----------Loading ERP DATA-----------';
  RAISE NOTICE '--------------------------------------';
  BEGIN -- Begin a separate block for ERP Data loading
    TRUNCATE TABLE bronze.erp_cust_az12;
    RAISE NOTICE '>>Truncated Table: bronze.erp_cust_az12.';
    COPY bronze.erp_cust_az12
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_cust_az12.';

    TRUNCATE TABLE bronze.erp_loc_a101;
    RAISE NOTICE '>>Truncated Table: bronze.erp_loc_a101.';
    COPY bronze.erp_loc_a101
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_loc_a101.';

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    RAISE NOTICE '>>Truncated Table: bronze.erp_px_cat_g1v2.';
    COPY bronze.erp_px_cat_g1v2
    FROM 'V:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER);
    RAISE NOTICE '>>Inserted Data Into bronze.erp_px_cat_g1v2.';
  EXCEPTION WHEN OTHERS THEN
      error_message := SQLERRM;
      RAISE NOTICE 'Error loading ERP data: %', error_message;
      -- You might want to RAISE EXCEPTION here to stop the whole process, or just continue
  END; -- End of ERP data loading block

  RAISE NOTICE 'Bulk load ERP & CRM completed.';
  end_time := clock_timestamp();

  RAISE NOTICE 'Time taken for bronze layer load: %', end_time - start_time;

EXCEPTION WHEN OTHERS THEN
  error_message := SQLERRM;
  RAISE NOTICE 'General error in bronze.load_bronze: %', error_message;
  end_time := clock_timestamp(); -- Record end time even on error
  RAISE NOTICE 'Total time taken (including error): %', end_time - start_time;
END;
$$;

CALL bronze.load_bronze()
