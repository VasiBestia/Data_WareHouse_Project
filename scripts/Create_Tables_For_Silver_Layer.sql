/* ================================================================================
DESCRIPTION:
    This script initializes the tables for the 'silver_layer' schema.
    The primary goal of this layer is to store cleansed, validated, and 
    standardized data derived from the 'bronze_layer'.

KEY ENHANCEMENTS IN SILVER:
    - Data Standardization: Applied consistent data types and lengths.
    - Metadata Tracking: Added 'dwh_create_date' to track when records are loaded.
    - Data Quality: This layer acts as the "Single Source of Truth" before 
      aggregation in the Gold layer.

TABLE GROUPS:
    - CRM Tables: Refined customer, product, and sales transaction data.
    - ERP Tables: Refined customer demographics, locations, and categories.

WARNING:
    Each section includes a 'DROP TABLE' statement. Running this will 
    delete existing data within these specific tables before recreating them.
================================================================================
*/

-- =============================================================================
-- CRM Tables
-- =============================================================================
Use DataWarehouse
Go

IF OBJECT_ID('silver_layer.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_cust_info;
GO

CREATE TABLE silver_layer.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(60),
    cst_firstname       NVARCHAR(60),
    cst_lastname        NVARCHAR(60),
    cst_marrital_status Varchar(15),
    cst_gender          Varchar(15),
    cst_create_date     DATE,
    dwh_create_date     DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver_layer.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_prd_info;
GO

CREATE TABLE silver_layer.crm_prd_info (
    prd_id          INT,
    cat_id          Varchar(15),
    prd_key         NVARCHAR(60),
    prd_nm          NVARCHAR(60),
    prd_cost        INT,
    prd_line        Varchar(15),
    prd_start_date  DATE,
    prd_end_date    DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver_layer.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver_layer.crm_sales_details;
GO

CREATE TABLE silver_layer.crm_sales_details (
    sls_ord_num     NVARCHAR(30),
    sls_prd_key     NVARCHAR(60),
    sls_cust_id     INT,
    sls_order_dt    Date,
    sls_ship_dt     Date,
    sls_due_dt      Date,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- =============================================================================
-- ERP Tables
-- =============================================================================

IF OBJECT_ID('silver_layer.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_CUST_AZ12;
GO

CREATE TABLE silver_layer.erp_CUST_AZ12 (
    CID             NVARCHAR(50),
    BDate           DATE,
    Gen             NVARCHAR(20),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver_layer.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_LOC_A101;
GO

CREATE TABLE silver_layer.erp_LOC_A101 (
    CID             NVARCHAR(50),
    Cntry           NVARCHAR(30),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver_layer.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE silver_layer.erp_PX_CAT_G1V2;
GO

CREATE TABLE silver_layer.erp_PX_CAT_G1V2 (
    ID              NVARCHAR(20),
    Cat             NVARCHAR(40),
    Subcat          NVARCHAR(40),
    Maintenance     CHAR(5),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO