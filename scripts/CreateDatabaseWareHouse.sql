/*
=================================================================================================================================================
Whats Script Do:
===================================================================================================================================================================

Full Reset: Checks for the existence of the DataWareHouse database. If it exists, the script forces all users (Single_User) to log out and permanently deletes it.
Build: Creates a new, clean database.
Medallion Architecture: Implements the three essential layers for a modern data warehouse:
Bronze: Stores data in raw format (exactly as it comes from the source).
Silver: Validated, cleaned, and transformed data.
Gold: Final data, optimized for performance and business intelligence.
========================================================================
WARNING: CRITICAL
========================================================================
This script includes a 'DROP DATABASE' command with 'ROLLBACK IMMEDIATE'. 
Executing this script will PERMANENTLY DELETE all data, tables, schemas, and 
configurations within the 'DataWareHouse' database. 

Ensure you have a verified backup before execution. This action is irreversible.
================================================================================
*/

Use master;
Go

if Exists(Select 1 from sys.databases Where name='DataWareHouse')
Begin
Alter Database DataWarehouse Set Single_User With RollBack Immediate;
Drop Database DataWarehouse;
End
Go

Create Database DataWarehouse;
Go

Use DataWarehouse;

Create Schema bronze_layer;
Go

Create Schema silver_layer;
Go

Create Schema gold_layer;