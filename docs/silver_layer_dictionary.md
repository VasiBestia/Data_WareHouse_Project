# 📘 Data Dictionary: Silver Layer (Standardized & Cleansed)

**Layer:** Silver (Transformation & Data Quality)  
**Database:** DataWarehouse  
**Description:** The Silver Layer holds data that has been cleaned, deduplicated, and standardized from the Bronze (Raw) layer. Null values are handled, dates are validated, and business rules are applied to prepare data for the Gold Layer.

---

## 1. Table: `silver_layer.crm_cust_info`
**Description:** Customer master data cleaned from CRM.  
**Source:** `bronze_layer.crm_cust_info`  
**Key Transformation:** Deduplication applied using `ROW_NUMBER()` to keep the most recent record per `cst_id`.

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `cst_id` | INT | PK | Unique Customer ID. | `ISNULL` check (defaults to 1000). |
| `cst_key` | NVARCHAR | | Functional Key. | Direct mapping. |
| `cst_firstname` | NVARCHAR | | First Name. | Whitespace trimmed. |
| `cst_lastname` | NVARCHAR | | Last Name. | Whitespace trimmed. |
| `cst_marrital_status` | NVARCHAR | | Marital Status. | Mapped: 'S'→'Single', 'M'→'Married', else 'n/a'. |
| `cst_gender` | NVARCHAR | | Gender. | Mapped: 'F'→'Female', 'M'→'Male', else 'n/a'. |
| `cst_create_date` | DATE | | Creation Date. | Direct mapping. |

---

## 2. Table: `silver_layer.crm_prd_info`
**Description:** Product catalog with historical tracking.  
**Source:** `bronze_layer.crm_prd_info`  
**Key Transformation:** Implements **SCD Type 2** logic to calculate `prd_end_date`.

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `prd_id` | INT | PK | Product ID. | Direct mapping. |
| `cat_id` | NVARCHAR | FK | Category ID. | Extracted from `prd_key` (substring 1-5, '-' replaced with '_'). |
| `prd_key` | NVARCHAR | | Product Functional Key. | Extracted from `prd_key` (substring 7+). |
| `prd_nm` | NVARCHAR | | Product Name. | Direct mapping. |
| `prd_cost` | INT | | Product Cost. | `ISNULL` check (defaults to 0). |
| `prd_line` | NVARCHAR | | Product Line Name. | Mapped: M→Mountain, R→Road, S→Other Sales, T→Touring. |
| `prd_start_date` | DATE | | Validity Start Date. | Cast to Date. |
| `prd_end_date` | DATE | | Validity End Date. | Calculated via `LEAD()` function (Start Date of next record - 1 day). |

---

## 3. Table: `silver_layer.crm_sales_details`
**Description:** Transactional sales data with validated financials.  
**Source:** `bronze_layer.crm_sales_details`  

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `sls_ord_num` | NVARCHAR | PK | Order Number. | Direct mapping. |
| `sls_prd_key` | NVARCHAR | FK | Product Key. | Direct mapping. |
| `sls_cust_id` | INT | FK | Customer ID. | Direct mapping. |
| `sls_order_dt` | DATE | | Order Date. | converted to NULL if '0' or invalid length. |
| `sls_ship_dt` | DATE | | Ship Date. | converted to NULL if '0' or invalid length. |
| `sls_due_dt` | DATE | | Due Date. | converted to NULL if '0' or invalid length. |
| `sls_sales` | DECIMAL | | Total Sales Value. | Recalculated as `Qty * Price` if NULL, negative, or mathematically incorrect. |
| `sls_quantity` | INT | | Quantity Sold. | Direct mapping. |
| `sls_price` | DECIMAL | | Unit Price. | Recalculated as `Sales / Qty` if NULL or negative. |

---

## 4. Table: `silver_layer.erp_CUST_AZ12`
**Description:** ERP Customer attributes (Birthdates, Gender).  
**Source:** `bronze_layer.erp_CUST_AZ12`  

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `CID` | NVARCHAR | PK | Customer Integration ID. | 'Nas' prefix removed if present. |
| `BDate` | DATE | | Birth Date. | Set to NULL if older than 1934 or in the future. |
| `Gen` | NVARCHAR | | Gender. | Standardized: {F, Female}→Female, {M, Male}→Male, else 'n/a'. |

---

## 5. Table: `silver_layer.erp_LOC_A101`
**Description:** ERP Location data (Country mappings).  
**Source:** `bronze_layer.erp_LOC_A101`  

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `CID` | NVARCHAR | PK | Customer Integration ID. | Dashes removed (`REPLACE(CID,'-','')`). |
| `Cntry` | NVARCHAR | | Country Name. | Standardized: DE→Germany, US/USA→United States, empty→n/a. |

---

## 6. Table: `silver_layer.erp_PX_CAT_G1V2`
**Description:** ERP Product Category definitions.  
**Source:** `bronze_layer.erp_PX_CAT_G1V2`  

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `ID` | NVARCHAR | PK | Category ID. | Direct load. |
| `Cat` | NVARCHAR | | Category Name. | Direct load. |
| `Subcat` | NVARCHAR | | Sub-Category Name. | Direct load. |
| `Maintenance` | NVARCHAR | | Maintenance Notes. | Direct load. |

---

## Data Flow Diagram (Mermaid)

```mermaid
flowchart LR
    subgraph BRONZE_LAYER
    B_CUST[crm_cust_info]
    B_PRD[crm_prd_info]
    B_SALES[crm_sales_details]
    B_ERP_C[erp_CUST_AZ12]
    B_ERP_L[erp_LOC_A101]
    B_ERP_CAT[erp_PX_CAT_G1V2]
    end

    subgraph SILVER_LAYER
    S_CUST[crm_cust_info]
    S_PRD[crm_prd_info]
    S_SALES[crm_sales_details]
    S_ERP_C[erp_CUST_AZ12]
    S_ERP_L[erp_LOC_A101]
    S_ERP_CAT[erp_PX_CAT_G1V2]
    end

    B_CUST -->|Deduplicate & Clean| S_CUST
    B_PRD -->|Calc End Date & Split Key| S_PRD
    B_SALES -->|Validate Dates & Calcs| S_SALES
    B_ERP_C -->|Norm Gender & Dates| S_ERP_C
    B_ERP_L -->|Norm Country| S_ERP_L
    B_ERP_CAT -->|Direct Load| S_ERP_CAT