/* ================================================================================
DESCRIPTION:
    This script initializes the tables for the 'bronze_layer' schema.
    The primary goal of this layer is to store raw data extracted from source 
    systems (CRM and ERP) in its original format.

TABLE GROUPS:
    - CRM Tables: customer info, product info, and sales details.
    - ERP Tables: customer demographics (AZ12), locations (A101), and 
      product categories (PX_CAT).

WARNING:
    Each section includes a 'DROP TABLE' statement. Running this will 
    delete existing data within these specific tables before recreating them.
================================================================================
*/

-- =============================================================================
-- CRM Tables
-- =============================================================================

IF OBJECT_ID('bronze_layer.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_cust_info;
GO

CREATE TABLE bronze_layer.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(60),
    cst_firstname      NVARCHAR(60),
    cst_lastname       NVARCHAR(60),
    cst_marrital_status CHAR(1),
    cst_gender         CHAR(1),
    cst_create_date    DATE
);

IF OBJECT_ID('bronze_layer.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_prd_info;
GO

CREATE TABLE bronze_layer.crm_prd_info (
    prd_id         INT,
    prd_key        NVARCHAR(60),
    prd_nm         NVARCHAR(60),
    prd_cost       INT,
    prd_line       CHAR(5),
    prd_start_date DATE,
    prt_end_date   DATE
);

IF OBJECT_ID('bronze_layer.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze_layer.crm_sales_details;
GO

CREATE TABLE bronze_layer.crm_sales_details (
    sls_ord_num   NVARCHAR(30),
    sls_prd_key   NVARCHAR(60),
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);

-- =============================================================================
-- ERP Tables
-- =============================================================================

IF OBJECT_ID('bronze_layer.erp_CUST_AZ12', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_CUST_AZ12;
GO

CREATE TABLE bronze_layer.erp_CUST_AZ12 (
    CID   NVARCHAR(50),
    BDate DATE,
    Gen   NVARCHAR(20)
);

IF OBJECT_ID('bronze_layer.erp_LOC_A101', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_LOC_A101;
GO

CREATE TABLE bronze_layer.erp_LOC_A101 (
    CID    NVARCHAR(50),
    Cntry  NVARCHAR(30)
);

IF OBJECT_ID('bronze_layer.erp_PX_CAT_G1V2', 'U') IS NOT NULL
    DROP TABLE bronze_layer.erp_PX_CAT_G1V2;
GO

CREATE TABLE bronze_layer.erp_PX_CAT_G1V2 (
    ID          NVARCHAR(20),
    Cat         NVARCHAR(40),
    Subcat      NVARCHAR(40),
    Maintenance CHAR(5)
);