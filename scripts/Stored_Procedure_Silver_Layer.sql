/*
====================================================================================================
SCOPE & LOGIC SUMMARY:
====================================================================================================
This procedure manages the ETL (Extract, Transform, Load) process from the Bronze (Raw) Layer 
to the Silver (Standardized) Layer. The primary objective is to clean, validate, and 
transform data into a trusted format for downstream analytics.

KEY TRANSFORMATIONS & VALIDATIONS PERFORMED:
1. DATA INTEGRITY (De-duplication):
   - Used ROW_NUMBER() in 'crm_cust_info' to keep only the most recent record per customer ID,
     effectively removing historical duplicates.

2. DATA CLEANING & STANDARDIZATION:
   - Trimmed whitespace from strings and mapped short codes (M, F, S) to full descriptive 
     values (Male, Female, Single, Married).
   - Normalized country codes (e.g., 'DE' to 'Germany', 'US' to 'United States').

3. DATA QUALITY & VALIDATION:
   - CALENDAR DATES: Handled "0" dates or invalid string lengths by converting them to NULL.
   - AGE FILTERING: Filtered unrealistic birth dates (e.g., older than 1934 or future dates).
   - FINANCIALS: Recalculated Sales and Price where values were NULL, negative, or inconsistent 
     with the (Quantity x Price) formula.

4. HISTORICAL TRACKING (SCD Type 2):
   - Implemented LEAD() window function for products to automatically calculate record 
     expiration dates ('prd_end_date'), ensuring a continuous chronological timeline.

5. PERFORMANCE MONITORING:
   - Embedded execution time logging at both the individual table level and the overall 
     batch level for performance auditing.

====================================================================================================
WARNING:
This procedure uses 'TRUNCATE TABLE' before each insertion. Running this script will 
PERMANENTLY DELETE all existing data in the Silver Layer tables before reloading them. 
Ensure Bronze Layer sources are populated correctly before execution.
====================================================================================================
*/

Use DataWarehouse
Go

Create Or Alter Procedure silver_layer.loadin_silver_layer 
As
Begin
    Declare @start_time Datetime,@end_time DateTime,@batch_start_time Datetime,@batch_end_time Datetime;
     Begin Try

            Set @batch_start_time=GETDATE();
            Print  '============================================================================='
            Print ' Loading Silver Layer'
            Print '============================================================================='

            Set @start_time=GETDATE()
            --============================
            --First table loading Script
            --============================
            Print('>>Truncating silver_layer.crm_cust_info')
            Truncate Table silver_layer.crm_cust_info;
            Print('>>Inserting into silver_layer.crm_cust_info')
            Insert Into silver_layer.crm_cust_info(
              cst_id,
              cst_key,
              cst_firstname,
              cst_lastname,
              cst_marrital_status,
              cst_gender,
              cst_create_date
            )

            Select
            IsNULL(cst_id,1000),
            cst_key,
            Trim(cst_firstname) As cst_firstname,
            Trim(cst_lastname) As cst_lastname,
            Case When cst_marrital_status='S' Then 'Single'
                 When cst_marrital_status='M' Then 'Married'
                 Else 'n/a'
            End cst_marrital_status,
            Case When cst_gender='F' Then 'Female'
                 When cst_gender='M' Then 'Male'
                 Else 'n/a'
            End cst_gender,
            cst_create_date
            From(
            Select
            *,
            ROW_NUMBER() Over(Partition By cst_id Order by cst_create_date Desc) as flag
            From bronze_layer.crm_cust_info
            ) t Where flag=1;
            
            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';
            --============================
            --Second table loading Script
            --============================
            Print('>>Truncating silver_layer.crm_prd_info')
            Truncate Table silver_layer.crm_prd_info;
            Print('>>Inserting into silver_layer.crm_prd_info')
            Insert into silver_layer.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_date,
            prd_end_date
            )
            Select 
            prd_id,
            Replace(SUBSTRING(prd_key,1,5),'-','_') As cat_id,
            SUBSTRING(prd_key,7,Len(prd_key)) As prd_key,
            prd_nm,
            ISNULL(prd_cost,0),
            Case When Upper(Trim(prd_line))='M' Then 'Mountain'
                 When Upper(Trim(prd_line))='R' Then 'Road'
                 When Upper(Trim(prd_line))='S' Then 'other Sales'
                 When Upper(Trim(prd_line))='T' Then 'Touring'
                 Else 'n/a'
            End As prd_line,
            Cast(prd_start_date As Date) prd_start_date,
            Cast(DATEADD(DAY, -1, LEAD(prd_start_date) OVER(PARTITION BY prd_key ORDER BY prd_start_date))As date) AS prd_end_date
            From bronze_layer.crm_prd_info;

            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';
            --============================
            --Third table loading Script
            --============================
            Print('>>Truncating silver_layer.crm_sales_details')
            Truncate Table silver_layer.crm_sales_details;
            Print('>>Inserting into silver_layer.crm_sales_details')
            Insert into silver_layer.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
            )

            Select
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            Case When sls_order_dt=0 or Len(sls_order_dt)!=8 Then Null
                 Else Cast(Cast(sls_order_dt As varchar) As Date)
            End sls_order_dt,
            Case When sls_ship_dt=0 or Len(sls_ship_dt)!=8 Then Null
                 Else Cast(Cast(sls_ship_dt As varchar) As Date)
            End sls_ship_dt,
            Case When sls_due_dt=0 or Len(sls_due_dt)!=8 Then Null
                 Else Cast(Cast(sls_due_dt As varchar) As Date)
            End sls_due_dt,
            Case When sls_sales is Null or sls_sales<=0 or sls_sales!= sls_quantity*Abs(sls_price)
                 Then sls_quantity*Abs(sls_price)
                 Else sls_sales
            End As sls_sales,
            sls_quantity,
            Case When sls_price is Null or sls_price<=0
                 Then sls_sales/Nullif(sls_quantity,0)
                 Else sls_price
            End sls_price
            From bronze_layer.crm_sales_details;
            
            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';
            --============================
            --Fourth table loading Script
            --============================
            Print('>>Truncating silver_layer.erp_CUST_AZ12')
            Truncate Table silver_layer.erp_CUST_AZ12;
            Print('>>Inserting into silver_layer.erp_CUST_AZ12')
            Insert Into silver_layer.erp_CUST_AZ12(
               CID,
               BDate,
               Gen
            )
            Select
            Case When CID like 'Nas%' Then Substring(CID,4,Len(CID))
                 Else CID
            End As CID,
            Case When BDate <='1934-01-01' or BDate>GETDATE() Then Null
                 Else BDate
            End As BDate,
            Case When Upper(Trim(Gen)) In ('F','FEMALE') Then 'Female'
                 When Upper(Trim(Gen)) In ('M','MALE') Then 'Male'    
                Else 'n/a'
            End As Gen
            From bronze_layer.erp_CUST_AZ12;
            
            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';
            --============================
            --Fifth table loading Script
            --============================
            Print('>>Truncating silver_layer.erp_LOC_A101')
            Truncate Table silver_layer.erp_LOC_A101;
            Print('>>Inserting into silver_layer.erp_LOC_A101')
            Insert into silver_layer.erp_LOC_A101(
               CID,
               Cntry
            )
            Select
            REPLACE(CID,'-','') CID,
            Case When Trim(Cntry)='DE' Then 'Germany'
                 When Trim(Cntry) in ('US','USA') Then 'United States'
                 When Trim(Cntry)='' Or Cntry is NULL Then 'n/a'
                 Else Trim(Cntry)
            End As Cntry
            From bronze_layer.erp_LOC_A101;
            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';
            --============================
            --Sixth table loading Script
            --============================

            Insert Into silver_layer.erp_PX_CAT_G1V2(
            ID,
            Cat,
            Subcat,
            Maintenance
            )
            Select 
            ID,
            Cat,
            Subcat,
            Maintenance
            From bronze_layer.erp_PX_CAT_G1V2;

          Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

             Set @batch_end_time=GETDATE();
            Print '>>>Loading Time Batch:'+Cast(Datediff(second,@batch_start_time,@batch_end_time) As Nvarchar)+'seconds';
End Try
Begin Catch
            Print '===================================================='
            Print 'Error Ocurred While Loading Bronze Layer'
            Print 'Error Message is:'+Error_Message();
            Print 'Error Number is:'+Cast(Error_Number() As Nvarchar);
            Print '===================================================='
    End Catch
End

Exec silver_layer.loadin_silver_layer