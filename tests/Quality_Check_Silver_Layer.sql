-- ====================================================================
-- SCRIPT DE VALIDARE (DATA QUALITY CHECK) - SILVER LAYER
-- ====================================================================

SELECT 
    'silver_layer.crm_cust_info' AS Nume_Tabel,
    COUNT(*) AS Total_Randuri,
    SUM(CASE WHEN cst_id IS NULL OR cst_firstname IS NULL THEN 1 ELSE 0 END) AS Randuri_Invalide,
    'Verificare Chei Nule' AS Regula_Verificata,
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY' 
         WHEN SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - NULL KEYS'
         ELSE 'PASS' 
    END AS Status
FROM silver_layer.crm_cust_info

UNION ALL

SELECT 
    'silver_layer.crm_prd_info',
    COUNT(*),
    SUM(CASE WHEN prd_end_date < prd_start_date THEN 1 ELSE 0 END),
    'Data Final < Data Inceput',
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY'
         WHEN SUM(CASE WHEN prd_end_date < prd_start_date THEN 1 ELSE 0 END) > 0 THEN 'FAIL - DATE LOGIC'
         ELSE 'PASS'
    END
FROM silver_layer.crm_prd_info

UNION ALL

SELECT 
    'silver_layer.crm_sales_details',
    COUNT(*),
    SUM(CASE 
        -- Verificam daca Sales este egal cu Qty * Price (acceptam o mica marja de eroare pt float)
        WHEN ABS(sls_sales - (sls_quantity * sls_price)) > 0.01 THEN 1 
        WHEN sls_order_dt IS NULL THEN 1
        ELSE 0 
    END),
    'Calcul Sales Incorect sau Date Nule',
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY'
         WHEN SUM(CASE WHEN ABS(sls_sales - (sls_quantity * sls_price)) > 0.01 THEN 1 ELSE 0 END) > 0 THEN 'WARNING - CALCULATION'
         ELSE 'PASS'
    END
FROM silver_layer.crm_sales_details

UNION ALL

SELECT 
    'silver_layer.erp_CUST_AZ12',
    COUNT(*),
    SUM(CASE WHEN CID IS NULL OR BDate IS NULL THEN 1 ELSE 0 END),
    'CID sau BDate Nul (Invalid)',
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY'
         WHEN SUM(CASE WHEN CID IS NULL THEN 1 ELSE 0 END) > 0 THEN 'FAIL - NULL KEYS'
         ELSE 'PASS'
    END
FROM silver_layer.erp_CUST_AZ12

UNION ALL

SELECT 
    'silver_layer.erp_LOC_A101',
    COUNT(*),
    SUM(CASE WHEN Cntry = 'n/a' OR Cntry IS NULL THEN 1 ELSE 0 END),
    'Tari invalide (n/a)',
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY'
         WHEN SUM(CASE WHEN Cntry = 'n/a' THEN 1 ELSE 0 END) > 0 THEN 'WARNING - MISSING COUNTRY'
         ELSE 'PASS'
    END
FROM silver_layer.erp_LOC_A101

UNION ALL

SELECT 
    'silver_layer.erp_PX_CAT_G1V2',
    COUNT(*),
    SUM(CASE WHEN ID IS NULL THEN 1 ELSE 0 END),
    'ID Categorie Lipsa',
    CASE WHEN COUNT(*) = 0 THEN 'FAIL - EMPTY'
         ELSE 'PASS'
    END
FROM silver_layer.erp_PX_CAT_G1V2;