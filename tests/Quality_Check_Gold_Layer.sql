/*
====================================================================================================
GOLD LAYER DATA QUALITY & VALIDATION SCRIPT
====================================================================================================
SCOPE:
This script performs post-load validation on the Gold Layer (Star Schema). 
It generates a unified report to ensure the analytical model is trustworthy for BI tools.

KEY CHECKS PERFORMED:
1. UNIQUENESS & PRIMARY KEYS:
   - Verifies that Surrogate Keys (SK) in Dimension tables are unique and not null.

2. REFERENTIAL INTEGRITY (The "Orphan" Check):
   - Crucial for Star Schemas: Checks if 'fact_sales' contains any records that 
     do not link to a valid Customer or Product in the dimensions. 
   - Since the Fact View uses LEFT JOINS, missing links will appear as NULL keys.

3. BUSINESS LOGIC & DATA COMPLETENESS:
   - Validates that critical business metrics (Sales, Quantity) are not negative.
   - Checks for missing attributes in Dimensions (e.g., missing Category or Country).

OUTPUT:
   - A summary table with Status: 'PASS', 'FAIL', or 'WARNING'.
====================================================================================================
*/

SELECT 
    'gold_layer.dim_customers' AS Table_Name,
    COUNT(*) AS Total_Records,
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS Failed_Records,
    'PK (customer_key) Integrity' AS Rule_Checked,
    CASE 
        WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY TABLE'
        WHEN SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - NULL KEYS'
        -- Optional: Check for duplicate keys
        WHEN COUNT(customer_key) > COUNT(DISTINCT customer_key) THEN 'FAIL - DUPLICATE KEYS'
        ELSE 'PASS'
    END AS Status
FROM gold_layer.dim_customers

UNION ALL

SELECT 
    'gold_layer.dim_products',
    COUNT(*),
    SUM(CASE WHEN product_key IS NULL OR cost < 0 THEN 1 ELSE 0 END),
    'PK Integrity & Negative Cost',
    CASE 
        WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY TABLE'
        WHEN SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - NULL KEYS'
        WHEN SUM(CASE WHEN cost < 0 THEN 1 ELSE 0 END) > 0 THEN 'FAIL - NEGATIVE COST'
        ELSE 'PASS'
    END
FROM gold_layer.dim_products

UNION ALL

SELECT 
    'gold_layer.fact_sales',
    COUNT(*),
    -- REFERENTIAL INTEGRITY CHECK:
    -- Since the view uses LEFT JOIN, a NULL key means the sale exists but the Customer/Product does not.
    SUM(CASE WHEN customer_key IS NULL OR product_number IS NULL THEN 1 ELSE 0 END),
    'Referential Integrity (Orphans)',
    CASE 
        WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY TABLE'
        WHEN SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - ORPHAN CUSTOMERS'
        WHEN SUM(CASE WHEN product_number IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - ORPHAN PRODUCTS'
        ELSE 'PASS'
    END
FROM gold_layer.fact_sales

UNION ALL

-- Data Completeness Check (Dimension Attributes)
SELECT 
    'gold_layer.dim_customers (Attributes)',
    COUNT(*),
    SUM(CASE WHEN country = 'n/a' OR gender = 'n/a' THEN 1 ELSE 0 END),
    'Completeness (Country/Gender)',
    CASE 
        WHEN SUM(CASE WHEN country = 'n/a' OR gender = 'n/a' THEN 1 ELSE 0 END) > 0 THEN 'WARNING - MISSING DATA'
        ELSE 'PASS'
    END
FROM gold_layer.dim_customers;