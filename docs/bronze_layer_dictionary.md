# 📙 Data Dictionary: Bronze Layer (Raw Ingestion)

**Layer:** Bronze (Raw / Staging)  
**Database:** DataWarehouse  
**Description:** The Bronze Layer serves as the landing zone for raw data. Tables here are direct replicas of the source CSV files with no transformations applied. Data types are loose to prevent ingestion failures.

---

## 1. Table: `bronze_layer.crm_cust_info`
**Source File:** `cust_info.csv` (CRM System)  
**Description:** Raw customer data containing names and basic status codes.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `cst_id` | INT | Unique Customer ID. |
| `cst_key` | NVARCHAR | Business Key / Code. |
| `cst_firstname` | NVARCHAR | First Name (may contain whitespace). |
| `cst_lastname` | NVARCHAR | Last Name (may contain whitespace). |
| `cst_marrital_status`| NVARCHAR | Marital Status Code (e.g., 'S', 'M'). |
| `cst_gender` | NVARCHAR | Gender Code (e.g., 'F', 'M'). |
| `cst_create_date` | DATETIME | Record creation timestamp. |

---

## 2. Table: `bronze_layer.crm_prd_info`
**Source File:** `prd_info.csv` (CRM System)  
**Description:** Raw product list. Note that `prd_key` here contains composite data (Category + ID) which is split in the Silver layer.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `prd_id` | INT | Product ID. |
| `prd_key` | NVARCHAR | Composite Product Key (e.g., 'CAT-ID'). |
| `prd_nm` | NVARCHAR | Product Name. |
| `prd_cost` | INT | Product Cost (Raw). |
| `prd_line` | NVARCHAR | Product Line Code (e.g., 'M', 'R'). |
| `prd_start_date` | DATETIME | Start Date of validity. |
| `prd_end_date` | DATETIME | End Date of validity (often NULL in source). |

---

## 3. Table: `bronze_layer.crm_sales_details`
**Source File:** `sales_details.csv` (CRM System)  
**Description:** Transactional sales logs. Dates are stored as integers/strings in the source (e.g., '20240101') and require casting in Silver.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `sls_ord_num` | NVARCHAR | Order Number Key. |
| `sls_prd_key` | NVARCHAR | Product Foreign Key. |
| `sls_cust_id` | INT | Customer Foreign Key. |
| `sls_order_dt` | INT/NVARCHAR | Order Date (Integer format YYYYMMDD). |
| `sls_ship_dt` | INT/NVARCHAR | Ship Date (Integer format YYYYMMDD). |
| `sls_due_dt` | INT/NVARCHAR | Due Date (Integer format YYYYMMDD). |
| `sls_sales` | INT | Total Sales Amount (Raw). |
| `sls_quantity` | INT | Quantity Sold. |
| `sls_price` | INT | Unit Price (Raw). |

---

## 4. Table: `bronze_layer.erp_CUST_AZ12`
**Source File:** `CUST_AZ12.csv` (ERP System - Legacy)  
**Description:** Legacy customer attributes. IDs often have prefixes (e.g., 'NAS') that need removal.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `CID` | NVARCHAR | Customer Integration ID (Raw, e.g., 'NAS-1001'). |
| `BDate` | DATE | Birth Date. |
| `Gen` | NVARCHAR | Gender (Mixed formats: 'Male', 'M', 'Female', 'F'). |

---

## 5. Table: `bronze_layer.erp_LOC_A101`
**Source File:** `LOC_A101.csv` (ERP System)  
**Description:** Customer country mappings.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `CID` | NVARCHAR | Customer Integration ID (may contain dashes). |
| `Cntry` | NVARCHAR | Country Code/Name (Mixed formats: 'US', 'USA', 'DE'). |

---

## 6. Table: `bronze_layer.erp_PX_CAT_G1V2`
**Source File:** `PX_CAT_G1V2.csv` (ERP System)  
**Description:** Product category definitions.

| Column Name | Data Type (Est.) | Description |
| :--- | :--- | :--- |
| `ID` | NVARCHAR | Category ID. |
| `Cat` | NVARCHAR | Category Name. |
| `Subcat` | NVARCHAR | Subcategory Name. |
| `Maintenance` | NVARCHAR | Maintenance notes. |

---

## Data Flow (Ingestion)

```mermaid
graph LR
    subgraph CSV_FILES [Flat Files Source]
    A[cust_info.csv]
    B[prd_info.csv]
    C[sales_details.csv]
    D[CUST_AZ12.csv]
    E[LOC_A101.csv]
    F[PX_CAT_G1V2.csv]
    end

    subgraph BRONZE_LAYER [SQL Server - Raw]
    T1[(crm_cust_info)]
    T2[(crm_prd_info)]
    T3[(crm_sales_details)]
    T4[(erp_CUST_AZ12)]
    T5[(erp_LOC_A101)]
    T6[(erp_PX_CAT_G1V2)]
    end

    A -->|Bulk Insert| T1
    B -->|Bulk Insert| T2
    C -->|Bulk Insert| T3
    D -->|Bulk Insert| T4
    E -->|Bulk Insert| T5
    F -->|Bulk Insert| T6