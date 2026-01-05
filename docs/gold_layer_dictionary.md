# 📘 Data Dictionary: Gold Layer (Star Schema)

**Layer:** Gold (Reporting & Analytics)  
**Schema Type:** Star Schema  
**Database:** DataWarehouse  
**Description:** The Gold Layer represents the final, cleansed, and modeled data ready for consumption by BI tools (Power BI, Tableau) and analysts. It follows a Star Schema architecture consisting of Dimension tables (`dim_`) and Fact tables (`fact_`).

---

## 1. Dimension: `gold_layer.dim_customers`
**Type:** Slowly Changing Dimension (SCD) Type 1 (Current State)  
**Source:** `silver_layer.crm_cust_info`, `silver_layer.erp_CUST_AZ12`, `silver_layer.erp_LOC_A101`

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `customer_key` | INT | **PK, SK** | Unique Surrogate Key for the customer dimension. | Generated via `ROW_NUMBER()`. Decouples analytics from source IDs. |
| `customer_id` | INT | BK | Original Business Key from the CRM system. | Direct mapping from `cst_id`. |
| `customer_number` | NVARCHAR | | Functional customer code (e.g., 'CST-294'). | Direct mapping from `cst_key`. |
| `firstname` | NVARCHAR | | Customer's first name. | Cleaned and trimmed in Silver Layer. |
| `lastname` | NVARCHAR | | Customer's last name. | Cleaned and trimmed in Silver Layer. |
| `country` | NVARCHAR | | Country of residence. | Derived from ERP Location (`erp_LOC_A101`). |
| `marrital_status` | NVARCHAR | | Marital status (Single/Married). | Standardized in Silver Layer. |
| `gender` | NVARCHAR | | Customer's gender. | **Logic:** Uses CRM value if valid; otherwise falls back to ERP value via `COALESCE`. |
| `birthdate` | DATE | | Date of birth. | Sourced from ERP (`erp_CUST_AZ12`). |
| `create_date` | DATE | | Date the customer record was created. | Direct mapping. |

> **Keys:** **PK** = Primary Key, **SK** = Surrogate Key, **BK** = Business Key, **FK** = Foreign Key.

---

## 2. Dimension: `gold_layer.dim_products`
**Type:** Dimension  
**Source:** `silver_layer.crm_prd_info`, `silver_layer.erp_PX_CAT_G1V2`  
**Filter:** Includes only **active** products (`prd_end_date IS NULL`).

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `product_key` | INT | **PK, SK** | Unique Surrogate Key for the product dimension. | Generated via `ROW_NUMBER()`. |
| `product_id` | INT | BK | Original Business Key from the CRM system. | Direct mapping. |
| `product_number` | NVARCHAR | | Product SKU or Code (e.g., 'PR-332'). | Direct mapping. |
| `product_name` | NVARCHAR | | Commercial name of the product. | Direct mapping. |
| `category_number` | NVARCHAR | | Code representing the product category. | Formatted in Silver (hyphens replaced with underscores). |
| `category` | NVARCHAR | | Full name of the main category. | Enriched via ERP lookup. |
| `subcategory` | NVARCHAR | | Full name of the sub-category. | Enriched via ERP lookup. |
| `maintenance` | NVARCHAR | | Maintenance requirements/notes. | Enriched via ERP lookup. |
| `cost` | DECIMAL | | Manufacturing or acquisition cost. | `NULL` values converted to 0 in Silver. |
| `product_line` | NVARCHAR | | Product line segment (Mountain, Road, etc.). | Mapped from codes in Silver. |
| `fabrication_date`| DATE | | Date the product started being active. | Direct mapping. |

---

## 3. Fact Table: `gold_layer.fact_sales`
**Type:** Transactional Fact Table  
**Source:** `silver_layer.crm_sales_details` joined with Gold Dimensions.

| Column Name | Data Type | Key Type | Description | Transformation / Logic |
| :--- | :--- | :--- | :--- | :--- |
| `order_number` | NVARCHAR | **PK** | Unique identifier for the sales order. | Direct mapping. |
| `product_number` | NVARCHAR | FK | Reference to the Product. | Link to `dim_products`. |
| `customer_key` | INT | FK | Reference to the Customer (Surrogate Key). | Derived by joining CRM ID with `dim_customers`. |
| `order_date` | DATE | | The date the order was placed. | Standardized Date. |
| `ship_date` | DATE | | The date the order was shipped. | Standardized Date. |
| `due_date` | DATE | | The expected due date. | Standardized Date. |
| `sales` | DECIMAL | | Total monetary value of the line item. | Validated in Silver (`Qty * Price`). |
| `quantity` | INT | | Number of units purchased. | Direct mapping. |
| `priceperquantity`| DECIMAL | | Unit price of the product. | Derived/Validated in Silver. |

---

## 4. Entity Relationship Diagram (Text Representation)

```mermaid
erDiagram
    FACT_SALES }|..|| DIM_CUSTOMERS : "customer_key"
    FACT_SALES }|..|| DIM_PRODUCTS : "product_number"

    DIM_CUSTOMERS {
        int customer_key PK
        string firstname
        string country
        string gender
    }

    DIM_PRODUCTS {
        int product_key PK
        string product_number
        string category
        decimal cost
    }

    FACT_SALES {
        string order_number PK
        int customer_key FK
        string product_number FK
        decimal sales
        int quantity
    }