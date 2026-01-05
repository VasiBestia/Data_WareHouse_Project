/* ================================================================================
DESCRIPTION:
    This script populates the 'bronze_layer' tables by importing raw data 
    from CSV files located in the source directories (CRM and ERP).

PROCESS:
    1. Truncate: Clears existing data from each table to ensure a fresh load 
       (Full Load pattern).
    2. Bulk Insert: Efficiently loads data from external .csv files into the 
       SQL Server tables.

NOTES:
    - FirstRow = 2: Skips the header row in the CSV files.
    - FieldTerminator = ',': Assumes standard comma-separated values.
    - TABLOCK: Used to minimize logging and improve performance during bulk load.
================================================================================
*/

USE DataWarehouse;
GO

Create Or Alter Procedure bronze_layer.loading_bronze_layer As
Begin
     Declare @start_time Datetime,@end_time DateTime,@batch_start_time Datetime,@batch_end_time Datetime;
     Begin Try

            Set @batch_start_time=GETDATE();
            Print  '============================================================================='
            Print ' Loading Bronze Layer'
            Print '============================================================================='

            Print  '============================================================================='
            Print ' Loading CRM DataTables'
            Print '============================================================================='

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:crm_cust_info.csv'
            TRUNCATE TABLE bronze_layer.crm_cust_info;

            Print '>>>Inserting data into table:bronze_layer.crm_cust_info'
            BULK INSERT bronze_layer.crm_cust_info
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_crm\cust_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:crm_prd_info.csv'
            TRUNCATE TABLE bronze_layer.crm_prd_info;

            Print '>>>Inserting data into table:bronze_layer.crm_prd_info'
            BULK INSERT bronze_layer.crm_prd_info
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_crm\prd_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
            
            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:crm_sales_details.csv'
            TRUNCATE TABLE bronze_layer.crm_sales_details;

            Print '>>>Inserting data into table:bronze_layer.crm_sales_details'
            BULK INSERT bronze_layer.crm_sales_details
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_crm\sales_details.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

            Print  '============================================================================='
            Print ' Loading ERP DataTables'
            Print '============================================================================='

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:erp_CUST_AZ12.csv'
            TRUNCATE TABLE bronze_layer.erp_CUST_AZ12;

            Print '>>>Inserting data into table:bronze_layer.erp_CUST_AZ12'
            BULK INSERT bronze_layer.erp_CUST_AZ12
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_erp\CUST_AZ12.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:erp_LOC_A101csv'
            TRUNCATE TABLE bronze_layer.erp_LOC_A101;

            Print '>>>Inserting data into table:bronze_layer.erp_LOC_A101'
            BULK INSERT bronze_layer.erp_LOC_A101
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_erp\LOC_A101.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            Set @end_time=GETDATE();
            Print '>>>Loading Time:'+Cast(Datediff(second,@start_time,@end_time) As Nvarchar)+'seconds';

            Set @start_time=GETDATE();
            Print '>>>Truncating data from:erp_PX_CAT_G1V2.csv'
            TRUNCATE TABLE bronze_layer.erp_PX_CAT_G1V2;

            Print '>>>Inserting data into table:bronze_layer.erp_PX_CAT_G1V2'
            BULK INSERT bronze_layer.erp_PX_CAT_G1V2
            FROM 'D:\DataWareHouse_Sql_Project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
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

Exec bronze_layer.loading_bronze_layer