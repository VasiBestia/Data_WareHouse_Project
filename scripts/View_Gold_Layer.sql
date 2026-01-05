/*
====================================================================================================
GOLD LAYER ARCHITECTURE - STAR SCHEMA DEFINITION
====================================================================================================
SCOPE & LOGIC SUMMARY:
This script defines the 'Gold Layer' of the Data Warehouse, representing the final, 
business-ready consumption layer. It implements a Star Schema model to support 
Business Intelligence (BI) and reporting.

KEY COMPONENTS:
1. gold_layer.dim_customers (Customer Dimension):
   - INTEGRATION: Merges CRM data ('crm_cust_info') with ERP data ('erp_CUST_AZ12' and 'erp_LOC_A101').
   - DATA REFINEMENT: Resolves gender discrepancies using COALESCE to fill 'n/a' values from 
     secondary ERP sources.
   - SURROGATE KEY: Generates 'customer_key' using ROW_NUMBER() for unique identification.

2. gold_layer.dim_products (Product Dimension):
   - FILTERING: Only active products are included (Where prd_end_date Is NULL), reflecting the 
     latest version of product metadata.
   - ENRICHMENT: Joins product info with category and maintenance details from ERP sources.

3. gold_layer.fact_sales (Sales Fact Table):
   - CENTRAL HUB: Connects transactional sales details with the created Dimensions.
   - LINKAGE: Bridges 'silver_layer.crm_sales_details' to Gold Dimensions using product 
     numbers and surrogate customer keys.

====================================================================================================
  QUALITY NOTE:
These Views act as the "Single Source of Truth." The logic ensures that data cleaning 
performed in the Silver Layer is now structured into a highly performant model for 
analytical queries.
====================================================================================================
*/

Create View gold_layer.dim_customers As 
Select
ROW_NUMBER() Over(Order by cst_id ) As customer_key,
ci.cst_id As customer_id,
ci.cst_key As customer_number,
ci.cst_firstname As firstname,
ci.cst_lastname As lastname,
la.Cntry As country,
ci.cst_marrital_status As marrital_status,
Case When ci.cst_gender !='n/a' Then ci.cst_gender
     Else Coalesce(ca.Gen,'n/a')
End As gender,
ca.BDate As birthdate,
ci.cst_create_date As create_date
From silver_layer.crm_cust_info As ci
Left Join silver_layer.erp_CUST_AZ12 As ca
ON ci.cst_key=ca.CID
Left Join silver_layer.erp_LOC_A101 As la
On ci.cst_key=la.CID


Create View gold_layer.dim_products
As
Select
ROW_NUMBER() Over(Order By pn.prd_start_date,pn.prd_key) As product_key,
pn.prd_id As product_id,
pn.prd_key As product_number,
pn.prd_nm As product_name,
pn.cat_id As category_number,
pc.Cat As category,
pc.Subcat As subcategory,
pc.Maintenance,
pn.prd_cost As cost,
pn.prd_line As product_line,
pn.prd_start_date As fabrication_date
From silver_layer.crm_prd_info pn
Left Join silver_layer.erp_PX_CAT_G1V2 pc
on pn.cat_id=pc.ID
Where pn.prd_end_date Is NULL


Create View gold_layer.fact_sales
As
Select
sd.sls_ord_num As order_number,
pr.product_number,
dc.customer_key,
sd.sls_order_dt As order_date,
sd.sls_ship_dt As ship_date,
sd.sls_due_dt As due_date,
sd.sls_sales As sales,
sd.sls_quantity As quantity,
sd.sls_price As priceperquantity
From silver_layer.crm_sales_details As sd
Left Join gold_layer.dim_products pr 
on sd.sls_prd_key=pr.product_number
Left Join gold_layer.dim_customers dc 
on sd.sls_cust_id=dc.customer_id

