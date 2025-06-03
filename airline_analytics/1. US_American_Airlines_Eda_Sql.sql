/* ============================================================================================================
--- ----------------------------   US AMERICAN AIRLINES FINANCIALS EDA  -----------------------------------
This dataset contains quarterly financial information from the largest US American Airlines from 2000 - 2019. 
*/
-- ============================================================================================================

-- Creating the 'AmericanAirlinesDB' database if not exists
USE master;
GO

IF NOT EXISTS (SELECT 1
FROM sys.databases
WHERE name = 'AmericanAirlinesDB')   
CREATE DATABASE AmericanAirlinesDB;
GO

USE AmericanAirlinesDB;
GO

-- ==================================== 3- Layer Data Architecture ===============================================
-- Creating Schemas if not exists

-- Bronze Layer: bronze.USAmericanAirlinesFinancials
-- This layer contains the raw data imported from the source system.
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
END
GO

-- Silver Layer: silver.USAmericanAirlinesFinancials
-- This layer contains the cleaned and transformed data ready for analysis.
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- Gold Layer: gold.USAmericanAirlinesFinancials
-- This layer contains the aggregated data for reporting and analysis.
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

-- ================================================================================================================
/*
##  The US Airlines financial data ('airlines_financials_2010_2019.csv') imported directly into SQLSERVER through 
    SSMS 'flat file import wizard' into the Bronze Layer.

    -   The table named "USAmericanAirlinesFinancials" was created in the SQL Server database.
    -   The table contains the following columns:
            UNIQUE_CARRIER_NAME, YEAR, QUARTER, Overall_OP_EXPENSES, Overall_OP_PROFIT_LOSS, Overall_OP_REVENUES,
            Rev_CHARTER_PAX, Rev_CHARTER_PROP, Rev_MAIL, Rev_MISC_OP_REV, Rev_PROP_BAG, Rev_PROP_FREIGHT,
            Rev_PUB_SVC_REVENUE, Rev_RES_CANCEL_FEES, Rev_TOTAL_CHARTER, Rev_TOTAL_MISC_REV, Rev_TOTAL_PROPERTY,
            Rev_TRANS_REVENUE, Rev_TRANS_REV_PAX, Exp_AIRCFT_SERVICES, Exp_FLYING_OPS, Exp_GENERAL_ADMIN, 
            Exp_GENERAL_SERVICES, Exp_MAINTENANCE, Exp_PAX_SERVICE, Exp_PROMOTION_SALES, Exp_TRANS_EXPENSES

    Bronze Layer Table: bronze.USAmericanAirlinesFinancials
*/
-- ================================================================================================================

/* =======================================================================================================   */
/* --------------------------------- Data Cleaning and Transformation -------------------------------------  */
/* =======================================================================================================   */

-- Checking the first 10 rows of the AmericanAirlinesFinancials table
SELECT TOP 10
    *
FROM bronze.USAmericanAirlinesFinancials;

-- Checking the data types of the columns in the AmericanAirlinesFinancials table
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'USAmericanAirlinesFinancials' AND TABLE_SCHEMA = 'bronze';

-- Checking for number of null values in the columns 
SELECT
    SUM(CASE WHEN UNIQUE_CARRIER_NAME IS NULL THEN 1 ELSE 0 END) AS Null_Unique_Carrier_Name,
    SUM(CASE WHEN YEAR IS NULL THEN 1 ELSE 0 END) AS Null_Year,
    SUM(CASE WHEN QUARTER IS NULL THEN 1 ELSE 0 END) AS Null_Quarter,
    SUM(CASE WHEN Overall_OP_EXPENSES IS NULL THEN 1 ELSE 0 END) AS Null_Overall_OP_EXPENSES,
    SUM(CASE WHEN Overall_OP_PROFIT_LOSS IS NULL THEN 1 ELSE 0 END) AS Null_Overall_OP_PROFIT_LOSS,
    SUM(CASE WHEN Overall_OP_REVENUES IS NULL THEN 1 ELSE 0 END) AS Null_Overall_OP_REVENUES,
    SUM(CASE WHEN Rev_CHARTER_PAX IS NULL THEN 1 ELSE 0 END) AS Null_Rev_CHARTER_PAX,
    SUM(CASE WHEN Rev_CHARTER_PROP IS NULL THEN 1 ELSE 0 END) AS Null_Rev_CHARTER_PROP,
    SUM(CASE WHEN Rev_MAIL IS NULL THEN 1 ELSE 0 END) AS Null_Rev_MAIL,
    SUM(CASE WHEN Rev_MISC_OP_REV IS NULL THEN 1 ELSE 0 END) AS Null_Rev_MISC_OP_REV,
    SUM(CASE WHEN Rev_PROP_BAG IS NULL THEN 1 ELSE 0 END) AS Null_Rev_PROP_BAG,
    SUM(CASE WHEN Rev_PROP_FREIGHT IS NULL THEN 1 ELSE 0 END) AS Null_Rev_PROP_FREIGHT,
    SUM(CASE WHEN Rev_PUB_SVC_REVENUE IS NULL THEN 1 ELSE 0 END) AS Null_Rev_PUB_SVC_REVENUE,
    SUM(CASE WHEN Rev_RES_CANCEL_FEES IS NULL THEN 1 ELSE 0 END) AS Null_Rev_RES_CANCEL_FEES,
    SUM(CASE WHEN Rev_TOTAL_CHARTER IS NULL THEN 1 ELSE 0 END) AS Null_Rev_TOTAL_CHARTER,
    SUM(CASE WHEN Rev_TOTAL_MISC_REV IS NULL THEN 1 ELSE 0 END) AS Null_Rev_TOTAL_MISC_REV,
    SUM(CASE WHEN Rev_TOTAL_PROPERTY IS NULL THEN 1 ELSE 0 END) AS Null_Rev_TOTAL_PROPERTY,
    SUM(CASE WHEN Rev_TRANS_REVENUE IS NULL THEN 1 ELSE 0 END) AS Null_Rev_TRANS_REVENUE,
    SUM(CASE WHEN Rev_TRANS_REV_PAX IS NULL THEN 1 ELSE 0 END) AS Null_Rev_TRANS_REV_PAX,
    SUM(CASE WHEN Exp_AIRCFT_SERVICES IS NULL THEN 1 ELSE 0 END) AS Null_Exp_AIRCFT_SERVICES,
    SUM(CASE WHEN Exp_FLYING_OPS IS NULL THEN 1 ELSE 0 END) AS Null_Exp_FLYING_OPS,
    SUM(CASE WHEN Exp_GENERAL_ADMIN IS NULL THEN 1 ELSE 0 END) AS Null_Exp_GENERAL_ADMIN,
    SUM(CASE WHEN Exp_GENERAL_SERVICES IS NULL THEN 1 ELSE 0 END) AS Null_Exp_GENERAL_SERVICES,
    SUM(CASE WHEN Exp_MAINTENANCE IS NULL THEN 1 ELSE 0 END) AS Null_Exp_MAINTENANCE,
    SUM(CASE WHEN Exp_PAX_SERVICE IS NULL THEN 1 ELSE 0 END) AS Null_Exp_PAX_SERVICE,
    SUM(CASE WHEN Exp_PROMOTION_SALES IS NULL THEN 1 ELSE 0 END) AS Null_Exp_PROMOTION_SALES,
    SUM(CASE WHEN Exp_TRANS_EXPENSES IS NULL THEN 1 ELSE 0 END) AS Null_Exp_TRANS_EXPENSES
FROM bronze.USAmericanAirlinesFinancials;


-- Checking for empty, null or non relative values in columns
-- Checking for non-numeric values in the columns
-- This query will return rows where the conversion to the specified type fails
SELECT *
FROM
    bronze.USAmericanAirlinesFinancials
WHERE 

    TRY_CAST(UNIQUE_CARRIER_NAME AS NVARCHAR) IS NULL
    AND UNIQUE_CARRIER_NAME IS NOT NULL

    OR TRY_CAST(YEAR AS datetime2) IS NULL
    AND YEAR IS NOT NULL

    OR TRY_CAST(QUARTER AS INT) IS NULL
    AND QUARTER IS NOT NULL

    OR TRY_CAST(Overall_OP_EXPENSES AS DECIMAL(18,2)) IS NULL
    AND Overall_OP_EXPENSES IS NOT NULL

    OR TRY_CAST(Overall_OP_PROFIT_LOSS AS DECIMAL(18,2)) IS NULL
    AND Overall_OP_PROFIT_LOSS IS NOT NULL

    OR TRY_CAST(Overall_OP_REVENUES AS DECIMAL(18,2)) IS NULL
    AND Overall_OP_REVENUES IS NOT NULL

    OR TRY_CAST(Rev_CHARTER_PAX AS DECIMAL(18,2)) IS NULL
    AND Rev_CHARTER_PAX IS NOT NULL

    OR TRY_CAST(Rev_CHARTER_PROP AS DECIMAL(18,2)) IS NULL
    AND Rev_CHARTER_PROP IS NOT NULL

    OR TRY_CAST(Rev_MAIL AS DECIMAL(18,2)) IS NULL
    AND Rev_MAIL IS NOT NULL

    OR TRY_CAST(Rev_MISC_OP_REV AS DECIMAL(18,2)) IS NULL
    AND Rev_MISC_OP_REV IS NOT NULL

    OR TRY_CAST(Rev_PROP_BAG AS DECIMAL(18,2)) IS NULL
    AND Rev_PROP_BAG IS NOT NULL

    OR TRY_CAST(Rev_PROP_FREIGHT AS DECIMAL(18,2)) IS NULL
    AND Rev_PROP_FREIGHT IS NOT NULL

    OR TRY_CAST(Rev_PUB_SVC_REVENUE AS DECIMAL(18,2)) IS NULL
    AND Rev_PUB_SVC_REVENUE IS NOT NULL

    OR TRY_CAST(Rev_RES_CANCEL_FEES AS DECIMAL(18,2)) IS NULL
    AND Rev_RES_CANCEL_FEES IS NOT NULL

    OR TRY_CAST(Rev_TOTAL_CHARTER AS DECIMAL(18,2)) IS NULL
    AND Rev_TOTAL_CHARTER IS NOT NULL

    OR TRY_CAST(Rev_TOTAL_MISC_REV AS DECIMAL(18,2)) IS NULL
    AND Rev_TOTAL_MISC_REV IS NOT NULL

    OR TRY_CAST(Rev_TOTAL_PROPERTY AS DECIMAL(18,2)) IS NULL
    AND Rev_TOTAL_PROPERTY IS NOT NULL

    OR TRY_CAST(Rev_TRANS_REVENUE AS DECIMAL(18,2)) IS NULL
    AND Rev_TRANS_REVENUE IS NOT NULL

    OR TRY_CAST(Rev_TRANS_REV_PAX AS DECIMAL(18,2)) IS NULL
    AND Rev_TRANS_REV_PAX IS NOT NULL

    OR TRY_CAST(Exp_AIRCFT_SERVICES AS DECIMAL(18,2)) IS NULL
    AND Exp_AIRCFT_SERVICES IS NOT NULL

    OR TRY_CAST(Exp_FLYING_OPS AS DECIMAL(18,2)) IS NULL
    AND Exp_FLYING_OPS IS NOT NULL

    OR TRY_CAST(Exp_GENERAL_ADMIN AS DECIMAL(18,2)) IS NULL
    AND Exp_GENERAL_ADMIN IS NOT NULL

    OR TRY_CAST(Exp_GENERAL_SERVICES AS DECIMAL(18,2)) IS NULL
    AND Exp_GENERAL_SERVICES IS NOT NULL

    OR TRY_CAST(Exp_MAINTENANCE AS DECIMAL(18,2)) IS NULL
    AND Exp_MAINTENANCE IS NOT NULL

    OR TRY_CAST(Exp_PAX_SERVICE AS DECIMAL(18,2)) IS NULL
    AND Exp_PAX_SERVICE IS NOT NULL

    OR TRY_CAST(Exp_PROMOTION_SALES AS DECIMAL(18,2)) IS NULL
    AND Exp_PROMOTION_SALES IS NOT NULL

    OR TRY_CAST(Exp_TRANS_EXPENSES AS DECIMAL(18,2)) IS NULL
    AND Exp_TRANS_EXPENSES IS NOT NULL;

-- ==============================================================================================================
-- Creating the silver layer table for data cleaning and transformation
-- ==============================================================================================================
DROP TABLE IF EXISTS silver.USAmericanAirlinesFinancials;

SELECT *
INTO silver.USAmericanAirlinesFinancials
FROM bronze.USAmericanAirlinesFinancials;


-- If there are any non-numeric values, you will need to clean them up before changing the data type
-- Replacing non-numeric values with 0 as most of the columns are financial related columns with numeric values
UPDATE silver.USAmericanAirlinesFinancials
SET 
    Overall_OP_EXPENSES      = COALESCE(TRY_CAST(Overall_OP_EXPENSES AS DECIMAL(18,2)), 0),
    Overall_OP_PROFIT_LOSS   = COALESCE(TRY_CAST(Overall_OP_PROFIT_LOSS AS DECIMAL(18,2)), 0),
    Overall_OP_REVENUES      = COALESCE(TRY_CAST(Overall_OP_REVENUES AS DECIMAL(18,2)), 0),
    Rev_CHARTER_PAX          = COALESCE(TRY_CAST(Rev_CHARTER_PAX AS DECIMAL(18,2)), 0),
    Rev_CHARTER_PROP         = COALESCE(TRY_CAST(Rev_CHARTER_PROP AS DECIMAL(18,2)), 0),
    Rev_MAIL                 = COALESCE(TRY_CAST(Rev_MAIL AS DECIMAL(18,2)), 0),
    Rev_MISC_OP_REV          = COALESCE(TRY_CAST(Rev_MISC_OP_REV AS DECIMAL(18,2)), 0),
    Rev_PROP_BAG             = COALESCE(TRY_CAST(Rev_PROP_BAG AS DECIMAL(18,2)), 0),
    Rev_PROP_FREIGHT         = COALESCE(TRY_CAST(Rev_PROP_FREIGHT AS DECIMAL(18,2)), 0),
    Rev_PUB_SVC_REVENUE      = COALESCE(TRY_CAST(Rev_PUB_SVC_REVENUE AS DECIMAL(18,2)), 0),
    Rev_RES_CANCEL_FEES      = COALESCE(TRY_CAST(Rev_RES_CANCEL_FEES AS DECIMAL(18,2)), 0),
    Rev_TOTAL_CHARTER        = COALESCE(TRY_CAST(Rev_TOTAL_CHARTER AS DECIMAL(18,2)), 0),
    Rev_TOTAL_MISC_REV       = COALESCE(TRY_CAST(Rev_TOTAL_MISC_REV AS DECIMAL(18,2)), 0),
    Rev_TOTAL_PROPERTY       = COALESCE(TRY_CAST(Rev_TOTAL_PROPERTY AS DECIMAL(18,2)), 0),
    Rev_TRANS_REVENUE        = COALESCE(TRY_CAST(Rev_TRANS_REVENUE AS DECIMAL(18,2)), 0),
    Rev_TRANS_REV_PAX        = COALESCE(TRY_CAST(Rev_TRANS_REV_PAX AS DECIMAL(18,2)), 0),
    Exp_AIRCFT_SERVICES      = COALESCE(TRY_CAST(Exp_AIRCFT_SERVICES AS DECIMAL(18,2)), 0),
    Exp_FLYING_OPS           = COALESCE(TRY_CAST(Exp_FLYING_OPS AS DECIMAL(18,2)), 0),
    Exp_GENERAL_ADMIN        = COALESCE(TRY_CAST(Exp_GENERAL_ADMIN AS DECIMAL(18,2)), 0),
    Exp_GENERAL_SERVICES     = COALESCE(TRY_CAST(Exp_GENERAL_SERVICES AS DECIMAL(18,2)), 0),
    Exp_MAINTENANCE          = COALESCE(TRY_CAST(Exp_MAINTENANCE AS DECIMAL(18,2)), 0),
    Exp_PAX_SERVICE          = COALESCE(TRY_CAST(Exp_PAX_SERVICE AS DECIMAL(18,2)), 0),
    Exp_PROMOTION_SALES      = COALESCE(TRY_CAST(Exp_PROMOTION_SALES AS DECIMAL(18,2)), 0),
    Exp_TRANS_EXPENSES       = COALESCE(TRY_CAST(Exp_TRANS_EXPENSES AS DECIMAL(18,2)), 0);


-- ** AGAIN -- Check for empty, null or non relative values in columns by exicuting the query avove with TRY_CAST().
-- If there are any non-numeric values or null values, data will apear.
-- Here all data was cleaned up and no data is apeared

-- Transformaing the data types of the columns

ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN UNIQUE_CARRIER_NAME NVARCHAR(255);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN YEAR DATETIME2;
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN QUARTER INT;
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Overall_OP_EXPENSES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Overall_OP_PROFIT_LOSS DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Overall_OP_REVENUES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_CHARTER_PAX DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_CHARTER_PROP DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_MAIL DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_MISC_OP_REV DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_PROP_BAG DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_PROP_FREIGHT DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_PUB_SVC_REVENUE DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_RES_CANCEL_FEES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_TOTAL_CHARTER DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_TOTAL_MISC_REV DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_TOTAL_PROPERTY DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_TRANS_REVENUE DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Rev_TRANS_REV_PAX DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_AIRCFT_SERVICES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_FLYING_OPS DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_GENERAL_ADMIN DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_GENERAL_SERVICES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_MAINTENANCE DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_PAX_SERVICE DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_PROMOTION_SALES DECIMAL(18,2);
ALTER TABLE silver.USAmericanAirlinesFinancials ALTER COLUMN Exp_TRANS_EXPENSES DECIMAL(18,2);

-- Check the data types of the columns again to confirm the changes
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'USAmericanAirlinesFinancials' AND TABLE_SCHEMA = 'silver';

/*
Rev_TOTAL_CHARTER, Rev_TOTAL_MISC_REV, Rev_TOTAL_PROPERTY can be calculated from the other 
columns with approximate values as we don't have complete data.

Records with missing or zeroed total values were updated using component-level values
       for ensuring more accurate financial aggregation and reporting.
*/


-- Updating Rev_TOTAL_CHARTER if it's 0
UPDATE silver.USAmericanAirlinesFinancials
SET Rev_TOTAL_CHARTER = Rev_CHARTER_PAX + Rev_CHARTER_PROP
WHERE Rev_TOTAL_CHARTER = 0;

-- Updating Rev_TOTAL_MISC_REV if it's 0
UPDATE silver.USAmericanAirlinesFinancials
SET Rev_TOTAL_MISC_REV = Rev_MISC_OP_REV
WHERE Rev_TOTAL_MISC_REV = 0;

-- Updating Rev_TOTAL_PROPERTY if it's 0
UPDATE silver.USAmericanAirlinesFinancials
SET Rev_TOTAL_PROPERTY = Rev_PROP_BAG + Rev_PROP_FREIGHT
WHERE Rev_TOTAL_PROPERTY = 0;


-- Checking for duplicate records in the silver layer table
SELECT
    UNIQUE_CARRIER_NAME,
    YEAR,
    QUARTER,
    COUNT(*) AS Duplicate_Count
FROM silver.USAmericanAirlinesFinancials
GROUP BY 
    UNIQUE_CARRIER_NAME, YEAR, QUARTER
HAVING COUNT(*) > 1

-- Analysing the duplicate records for the data quality check.
-- CASE 1: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM silver.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR = '2000-01-01'
    AND QUARTER = 1;

-- CASE 2: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM silver.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'Delta Air Lines Inc.'
    AND YEAR = '2000-01-01'
    AND QUARTER = 1;

-- CASE 3: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM silver.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'Federal Express Corporation'
    AND YEAR = '2009-01-01'
    AND QUARTER = 2;

/*
=============================================================================================================
Every duplicate record for a particiular airline in a particular year and quarter have different values for the financial columns.
To proceed further, we will remove the duplicate records and keeping the highest Overall_OP_REVENUES and Overall_OP_PROFIT_LOSS value 
for each airline in a specific year and quarter.
In real-world scenarios, we consult the business stakeholders to decide on the best approach for handling duplicates.
=============================================================================================================
*/

-- =============================================================================================================
-- Exporting the cleaned data and transformed data into the Gold Layer
-- Creating a new table in the gold layer
-- =============================================================================================================
DROP TABLE IF EXISTS gold.USAmericanAirlinesFinancials;

SELECT *,
       CAST(GETDATE() AS DATE) AS Data_Extract_Date,
       CAST(GETDATE() AS DATE) AS Data_Load_Date
INTO gold.USAmericanAirlinesFinancials
FROM silver.USAmericanAirlinesFinancials;

-- Checking for duplicate records again in the gold layer table
SELECT
    UNIQUE_CARRIER_NAME,
    YEAR,
    QUARTER,
    COUNT(*) AS Duplicate_Count
FROM gold.USAmericanAirlinesFinancials
GROUP BY 
    UNIQUE_CARRIER_NAME, YEAR, QUARTER
HAVING COUNT(*) > 1

-- Checking the total number of records before and after removing duplicates
SELECT COUNT(*) AS Total_Records_Before_Deduplication
FROM gold.USAmericanAirlinesFinancials;

-- Analysing the duplicate records for the data quality check in gold layer before deleting.
-- CASE 1: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR = '2000-01-01'
    AND QUARTER = 1;

-- CASE 2: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'Federal Express Corporation'
    AND YEAR = '2000-01-01'
    AND QUARTER = 4;

-- CASE 3: Checking for duplicate records for a specific airline in a specific year and quarter
SELECT * FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'Federal Express Corporation'
    AND YEAR = '2009-01-01'
    AND QUARTER = 2;

-- Deleting duplicate records and the highest Overall_OP_REVENUES and Overall_OP_PROFIT_LOSS value 
-- for each airline in a specific year and quarter.
WITH RankedRecords AS
    (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY UNIQUE_CARRIER_NAME, YEAR, QUARTER 
                               ORDER BY Overall_OP_REVENUES DESC, Overall_OP_PROFIT_LOSS DESC) AS RowNum
        FROM gold.USAmericanAirlinesFinancials
    )
DELETE FROM RankedRecords
WHERE RowNum > 1;

-- Checking the total number of records after removing duplicates
SELECT COUNT(*) AS Total_Records_After_Deduplication
FROM gold.USAmericanAirlinesFinancials;
-- 624 Records final count after removing duplicates


-- ==============================================================================================================
-- Creating a table for aggregated data in the gold layer for carrier profit summary over the years
-- ==============================================================================================================
DROP TABLE IF EXISTS gold.CarrierProfitSummary;
SELECT 
    agg.yyyy,
    minTbl.UNIQUE_CARRIER_NAME AS min_carrier,
    maxTbl.UNIQUE_CARRIER_NAME AS max_carrier,
    agg.max_profit,
    agg.min_profit
INTO gold.CarrierProfitSummary
FROM (
    SELECT 
        YEAR([YEAR]) AS yyyy,
        MIN(Overall_OP_PROFIT_LOSS) AS min_profit,
        MAX(Overall_OP_PROFIT_LOSS) AS max_profit
    FROM gold.USAmericanAirlinesFinancials
    GROUP BY YEAR([YEAR])
) agg
INNER JOIN gold.USAmericanAirlinesFinancials minTbl
    ON agg.yyyy = YEAR(minTbl.[YEAR]) AND agg.min_profit = minTbl.Overall_OP_PROFIT_LOSS
INNER JOIN gold.USAmericanAirlinesFinancials maxTbl
    ON agg.yyyy = YEAR(maxTbl.[YEAR]) AND agg.max_profit = maxTbl.Overall_OP_PROFIT_LOSS;

-- Checking the aggregated data in the gold layer
SELECT * FROM gold.CarrierProfitSummary
ORDER BY yyyy;

-- ==============================================================================================================
-- Creating a aggigated table for detailed carrier expenses in the gold layer
-- ==============================================================================================================
DROP TABLE IF EXISTS gold.CarrierExpenseDetails;
CREATE TABLE gold.CarrierExpenseDetails (
    yyyy INT,
    quarter INT,
    UNIQUE_CARRIER_NAME NVARCHAR(100),
    Overall_OP_EXPENSES DECIMAL(18,2),
    Expense_Value DECIMAL(18,2),
    Expense_Category NVARCHAR(50)
);

INSERT INTO gold.CarrierExpenseDetails (yyyy, quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Expense_Value, Expense_Category)
SELECT YEAR(year) AS yyyy, quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_AIRCFT_SERVICES AS Expense_Value, 'EXP_AIRCFT_SERVICES' AS Expense_Category
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_FLYING_OPS, 'EXP_FLYING_OPS'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_GENERAL_ADMIN, 'EXP_GENERAL_ADMIN'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_GENERAL_SERVICES, 'EXP_GENERAL_SERVICES'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_MAINTENANCE, 'EXP_MAINTENANCE'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_PAX_SERVICE, 'EXP_PAX_SERVICE'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_PROMOTION_SALES, 'EXP_PROMOTION_SALES'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_EXPENSES, Exp_TRANS_EXPENSES, 'EXP_TRANS_EXPENSES'
FROM gold.USAmericanAirlinesFinancials;

-- Checking the CarrierExpenseDetails table in the gold layer
SELECT * FROM gold.CarrierExpenseDetails;

-- ==============================================================================================================
-- Checking the CarrierExpenseDetails table in the gold layer for aggregated expenses
-- ==============================================================================================================
DROP TABLE IF EXISTS gold.CarrierRevenueDetails
CREATE TABLE gold.CarrierRevenueDetails (
    yyyy INT,
    quarter INT,
    UNIQUE_CARRIER_NAME NVARCHAR(100),
    Overall_OP_REVENUE DECIMAL(18,2),
    Revenue_Value DECIMAL(18,2),
    Revenue_Category NVARCHAR(50)
);

INSERT INTO gold.CarrierRevenueDetails (yyyy, quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUE, Revenue_Value, Revenue_Category)

SELECT YEAR(year) AS yyyy, quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_CHARTER_PAX AS Revenue_Value, 'REV_CHARTER_PAX' AS Revenue_Category
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_CHARTER_PROP, 'REV_CHARTER_PROP'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_MAIL, 'REV_MAIL'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_MISC_OP_REV, 'REV_MISC_OP_REV'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_PROP_BAG, 'REV_PROP_BAG'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_PROP_FREIGHT, 'REV_PROP_FREIGHT'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_PUB_SVC_REVENUE, 'REV_PUB_SVC_REVENUE'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_RES_CANCEL_FEES, 'REV_RES_CANCEL_FEES'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_TRANS_REVENUE, 'REV_TRANS_REVENUE'
FROM gold.USAmericanAirlinesFinancials
UNION ALL
SELECT YEAR(year), quarter, UNIQUE_CARRIER_NAME, Overall_OP_REVENUES, Rev_TRANS_REV_PAX, 'REV_TRANS_REV_PAX'
FROM gold.USAmericanAirlinesFinancials;

-- Checking the CarrierRevenueDetails table in the gold layer
SELECT * FROM gold.CarrierRevenueDetails;

/* =======================================================================================================   
   --------------------------------- Exploratory Data Analysis - SCENARIO 1 ------------------------------   
   =======================================================================================================   */

-- Calculating overall revenue, profit/loss and expenses throughout the years
SELECT
    UNIQUE_CARRIER_NAME AS Airline,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
FROM gold.USAmericanAirlinesFinancials
GROUP BY UNIQUE_CARRIER_NAME
ORDER BY Total_Profit_Loss DESC;


-- Annual Revenue vs. Expense vs. Profit/Loss
SELECT
    YEAR(YEAR) AS Year,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
FROM gold.USAmericanAirlinesFinancials
GROUP BY YEAR(YEAR)
ORDER BY Year;


-- Calculating Airline Financials by Year & Quarter with Profit Margin percentage
SELECT
    UNIQUE_CARRIER_NAME AS Airline,
    YEAR(YEAR) AS Reporting_Year,
    QUARTER,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss,
    CASE 
        WHEN SUM(Overall_OP_REVENUES) = 0 THEN NULL
        ELSE ROUND((SUM(Overall_OP_PROFIT_LOSS) * 100.0) / SUM(Overall_OP_REVENUES), 2)
    END AS Profit_Margin_Percent
FROM gold.USAmericanAirlinesFinancials
GROUP BY UNIQUE_CARRIER_NAME, YEAR(YEAR), QUARTER
ORDER BY UNIQUE_CARRIER_NAME, Reporting_Year, QUARTER;


-- Calculating Aggrregation Year wise
SELECT
    UNIQUE_CARRIER_NAME AS Airline,
    YEAR(YEAR) AS Reporting_Year,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss,
    ROUND(
        CASE 
            WHEN SUM(Overall_OP_REVENUES) = 0 THEN NULL
            ELSE (SUM(Overall_OP_PROFIT_LOSS) * 100.0) / SUM(Overall_OP_REVENUES)
        END, 2
    ) AS Profit_Margin_Percent
FROM gold.USAmericanAirlinesFinancials
GROUP BY UNIQUE_CARRIER_NAME, YEAR(YEAR)
ORDER BY UNIQUE_CARRIER_NAME, Reporting_Year;


-- Yearly Running Totals of Revenue and Profit/Loss
-- This query calculates the running total of revenue and profit/loss for each airline by year
WITH
    YearlyFinancials
    AS
    (
        SELECT
            UNIQUE_CARRIER_NAME AS Airline,
            YEAR(YEAR) AS Reporting_Year,
            SUM(Overall_OP_REVENUES) AS Total_Revenue,
            SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
        FROM gold.USAmericanAirlinesFinancials
        GROUP BY UNIQUE_CARRIER_NAME, YEAR(YEAR)
    )
SELECT
    Airline,
    Reporting_Year,
    Total_Revenue,
    Total_Profit_Loss,
    SUM(Total_Revenue) OVER (PARTITION BY Airline ORDER BY Reporting_Year) AS Running_Total_Revenue,
    SUM(Total_Profit_Loss) OVER (PARTITION BY Airline ORDER BY Reporting_Year) AS Running_Total_Profit
FROM YearlyFinancials
ORDER BY Airline, Reporting_Year;


-- Yearly Revenue, Expenses and Profit/Loss with Year-over-Year (YoY_Change) Change
--  Using LAG function to get the previous year's values for comparison
-- YoY_Change = Current_Year_Value - Previous_Year_Value
-- YoY_Percent = (Current_Year_Value - Previous_Year_Value) / Previous_Year_Value * 100
WITH
    YearlyData
    AS
    (
        SELECT
            YEAR([YEAR]) AS Year,
            SUM(Overall_OP_REVENUES) AS Total_Revenue,
            SUM(Overall_OP_EXPENSES) AS Total_Expenses,
            SUM(Overall_OP_PROFIT_LOSS) AS Profit_Loss
        FROM gold.USAmericanAirlinesFinancials
        GROUP BY YEAR([YEAR])
    ),
    YoY_Change
    AS
    (
        SELECT
            Year,
            Total_Revenue,
            LAG(Total_Revenue) OVER (ORDER BY Year) AS Prev_Year_Revenue,
            Total_Expenses,
            LAG(Total_Expenses) OVER (ORDER BY Year) AS Prev_Year_Expenses,
            Profit_Loss,
            LAG(Profit_Loss) OVER (ORDER BY Year) AS Prev_Year_Profit
        FROM YearlyData
    )
SELECT
    Year,
    Total_Revenue,
    Prev_Year_Revenue,
    Total_Revenue - Prev_Year_Revenue AS YoY_Revenue_Change,
    ROUND(100.0 * (Total_Revenue - Prev_Year_Revenue) / NULLIF(Prev_Year_Revenue, 0), 2) AS YoY_Revenue_Percent,

    Total_Expenses,
    Prev_Year_Expenses,
    Total_Expenses - Prev_Year_Expenses AS YoY_Expenses_Change,
    ROUND(100.0 * (Total_Expenses - Prev_Year_Expenses) / NULLIF(Prev_Year_Expenses, 0), 2) AS YoY_Expenses_Percent,

    Profit_Loss,
    Prev_Year_Profit,
    Profit_Loss - Prev_Year_Profit AS YoY_Profit_Change,
    ROUND(100.0 * (Profit_Loss - Prev_Year_Profit) / NULLIF(Prev_Year_Profit, 0), 2) AS YoY_Profit_Percent
FROM YoY_Change
ORDER BY Year;


--  Calculating Revenue Breakdown by Type (Yearly Total)
SELECT
    YEAR([YEAR]) AS Year,
    SUM(Rev_TRANS_REV_PAX) AS Passenger_Revenue,
    SUM(Rev_PROP_BAG) AS Baggage_Fees,
    SUM(Rev_RES_CANCEL_FEES) AS Cancelation_Fees,
    SUM(Rev_MISC_OP_REV) AS Misc_Revenue,
    SUM(Rev_PROP_FREIGHT) AS Freight_Revenue,
    SUM(Rev_CHARTER_PAX) AS Charter_Passenger_Revenue,
    SUM(Rev_CHARTER_PROP) AS Charter_Property_Revenue,
    SUM(Rev_MAIL) AS Mail_Revenue,
    SUM(Rev_PUB_SVC_REVENUE) AS Public_Service_Revenue,
    SUM(Rev_TOTAL_CHARTER) AS Total_Charter_Revenue,
    SUM(Rev_TOTAL_PROPERTY) AS Total_Property_Revenue,
    SUM(Rev_TOTAL_MISC_REV) AS Total_Misc_Revenue,
    SUM(Rev_TRANS_REVENUE) AS Total_Transportation_Revenue,
    SUM(Overall_OP_REVENUES) AS Total_Revenue
FROM gold.USAmericanAirlinesFinancials
GROUP BY YEAR([YEAR])
ORDER BY Year;


--  Calculating Expenses Breakdown by Type (Yearly Total) with Profit Margin percentage
SELECT
    YEAR(YEAR) AS Reporting_Year,
    SUM(Exp_AIRCFT_SERVICES) AS Total_Aircraft_Services_Expenses,
    SUM(Exp_FLYING_OPS) AS Total_Flying_Operations_Expenses,
    SUM(Exp_GENERAL_ADMIN) AS Total_General_Admin_Expenses,
    SUM(Exp_GENERAL_SERVICES) AS Total_General_Services_Expenses,
    SUM(Exp_MAINTENANCE) AS Total_Maintenance_Expenses,
    SUM(Exp_PAX_SERVICE) AS Total_Passenger_Service_Expenses,
    SUM(Exp_PROMOTION_SALES) AS Total_Promotion_Sales_Expenses,
    SUM(Exp_TRANS_EXPENSES) AS Total_Transportation_Expenses,

    -- Profit Margin % = (Profit / Revenue) * 100
    CASE
        WHEN SUM(Overall_OP_REVENUES) = 0 THEN NULL
        ELSE CAST(ROUND((SUM(Overall_OP_PROFIT_LOSS) * 100.0) / SUM(Overall_OP_REVENUES), 2) AS decimal(10, 2))
    END AS Profit_Margin_Percent

FROM gold.USAmericanAirlinesFinancials
GROUP BY YEAR(YEAR)
ORDER BY Reporting_Year;


-- Calculating Cost Breakdown by percentage of Total Expenses (Yearly Total)
SELECT
    YEAR(YEAR) AS Year,
    SUM(Exp_MAINTENANCE) / SUM(Overall_OP_REVENUES) * 100 AS Maintenance_Percent,
    SUM(Exp_FLYING_OPS) / SUM(Overall_OP_REVENUES) * 100 AS FlyingOps_Percent,
    SUM(Exp_GENERAL_ADMIN) / SUM(Overall_OP_REVENUES) * 100 AS Admin_Percent,
    SUM(Exp_PAX_SERVICE) / SUM(Overall_OP_REVENUES) * 100 AS PaxService_Percent,
    SUM(Exp_PROMOTION_SALES) / SUM(Overall_OP_REVENUES) * 100 AS PromotionSales_Percent,
    SUM(Exp_GENERAL_SERVICES) / SUM(Overall_OP_REVENUES) * 100 AS GeneralServices_Percent,
    SUM(Exp_AIRCFT_SERVICES) / SUM(Overall_OP_REVENUES) * 100 AS AircraftServices_Percent,
    SUM(Exp_TRANS_EXPENSES) / SUM(Overall_OP_REVENUES) * 100 AS TransportationExpenses_Percent
FROM gold.USAmericanAirlinesFinancials
GROUP BY YEAR(YEAR)
ORDER BY Year;


-- Calculating Ancillary Revenue Breakdown over time (Yearly Total)
-- Assuming Ancillary Revenue includes Baggage Fees, Cancelation Fees, Miscellaneous Operational Revenue, Freight Revenue, and Mail Revenue
SELECT
    YEAR(YEAR) AS Year,
    SUM(Rev_PROP_BAG + Rev_RES_CANCEL_FEES + Rev_MISC_OP_REV + Rev_PROP_FREIGHT + Rev_MAIL) AS Ancillary_Revenue,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    (SUM(Rev_PROP_BAG + Rev_RES_CANCEL_FEES + Rev_MISC_OP_REV+ Rev_PROP_FREIGHT + Rev_MAIL) * 100.0) / SUM(Overall_OP_REVENUES) AS Ancillary_Percent
FROM gold.USAmericanAirlinesFinancials
GROUP BY YEAR(YEAR)
ORDER BY Year;


-- Calculating Profit Trend per airline over the years
SELECT
    UNIQUE_CARRIER_NAME,
    YEAR(YEAR) AS Year,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit
FROM gold.USAmericanAirlinesFinancials
GROUP BY UNIQUE_CARRIER_NAME, YEAR(YEAR)
ORDER BY UNIQUE_CARRIER_NAME, Year;


-- Calculating the top 5 Airlines by Total by average profit/loss in last 5 years
SELECT TOP 5
    UNIQUE_CARRIER_NAME,
    CAST(ROUND(AVG(Overall_OP_PROFIT_LOSS), 2) AS DECIMAL(10, 2)) AS Avg_Profit_Last_5_Years
FROM gold.USAmericanAirlinesFinancials
WHERE YEAR(YEAR) BETWEEN 
    (SELECT YEAR(MAX(YEAR)) - 4
FROM gold.USAmericanAirlinesFinancials)
        AND
    (SELECT YEAR(MAX(YEAR))
FROM gold.USAmericanAirlinesFinancials)
GROUP BY UNIQUE_CARRIER_NAME
ORDER BY Avg_Profit_Last_5_Years DESC;


-- Calculating Quarterly Revenue Seasonality wise to show the average revenue per quarter
SELECT
    QUARTER,
    CAST(ROUND(AVG(Overall_OP_REVENUES),2) AS decimal(10,2)) AS Avg_Revenue
FROM gold.USAmericanAirlinesFinancials
GROUP BY QUARTER
ORDER BY AVG(Overall_OP_REVENUES) DESC;


/* ================================================================================================================  
      -------------------- SCENARIO 2: UNIQUE_CARRIER_NAME = 'American Airlines Inc.' ----------------------         
   ================================================================================================================  */

-- Calculating the overall financials of American Airlines Inc. for the years 2010 to 2019
SELECT
    YEAR(YEAR) AS Reporting_Year,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss,
    ROUND(
        CASE 
            WHEN SUM(Overall_OP_REVENUES) = 0 THEN NULL
            ELSE (SUM(Overall_OP_PROFIT_LOSS) * 100.0) / SUM(Overall_OP_REVENUES)
        END, 2
    ) AS Profit_Margin_Percent
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR)
ORDER BY YEAR(YEAR);


-- Calculating quarterly revenue and profit/loss trend for American Airlines Inc. for the years 2010 to 2019
SELECT
    YEAR(YEAR) AS Reporting_Year,
    QUARTER,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR), QUARTER
ORDER BY Reporting_Year, QUARTER;


-- Calculating Expense breaking down by type for American Airlines Inc. for the years 2010 to 2019
SELECT
    YEAR(YEAR) AS Reporting_Year,
    SUM(Exp_AIRCFT_SERVICES) AS Aircraft_Services,
    SUM(Exp_FLYING_OPS) AS Flying_Operations,
    SUM(Exp_GENERAL_ADMIN) AS General_Admin,
    SUM(Exp_GENERAL_SERVICES) AS General_Services,
    SUM(Exp_MAINTENANCE) AS Maintenance,
    SUM(Exp_PAX_SERVICE) AS Passenger_Services,
    SUM(Exp_PROMOTION_SALES) AS Promotion_Sales,
    SUM(Exp_TRANS_EXPENSES) AS Transportation_Expenses,
    SUM(Overall_OP_EXPENSES) AS Total_Expenses
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR)
ORDER BY YEAR(YEAR);


-- Calculating top 5 years with highest profit/loss for American Airlines Inc. for the years 2010 to 2019
SELECT TOP 5
    YEAR(YEAR) AS Reporting_Year,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR)
ORDER BY Total_Profit_Loss DESC;


-- Revenue composition for the most recent year
SELECT
    SUM(Rev_CHARTER_PAX) AS Charter_Passenger_Revenue,
    SUM(Rev_CHARTER_PROP) AS Charter_Property_Revenue,
    SUM(Rev_MAIL) AS Mail_Revenue,
    SUM(Rev_MISC_OP_REV) AS Misc_Operational_Revenue,
    SUM(Rev_PROP_BAG) AS Property_Baggage_Revenue,
    SUM(Rev_PROP_FREIGHT) AS Property_Freight_Revenue,
    SUM(Rev_PUB_SVC_REVENUE) AS Public_Service_Revenue,
    SUM(Rev_RES_CANCEL_FEES) AS Reservation_Cancellation_Fees,
    SUM(Rev_TOTAL_CHARTER) AS Total_Charter_Revenue,
    SUM(Rev_TOTAL_MISC_REV) AS Total_Misc_Revenue,
    SUM(Rev_TOTAL_PROPERTY) AS Total_Property_Revenue,
    SUM(Rev_TRANS_REVENUE) AS Total_Transportation_Revenue,
    SUM(Rev_TRANS_REV_PAX) AS Transportation_Passenger_Revenue
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) = (SELECT MAX(YEAR(YEAR))
    FROM gold.USAmericanAirlinesFinancials)
GROUP BY UNIQUE_CARRIER_NAME;


-- Revenue composition for the all the years with percentage contribution to its total revenue
SELECT
    YEAR([YEAR]) AS Reporting_Year,
    SUM(Rev_TRANS_REVENUE) AS Total_Transportation_Revenue,
    SUM(Rev_TRANS_REV_PAX) AS Transportation_Passenger_Revenue,
    SUM(Rev_CHARTER_PAX) AS Charter_Passenger_Revenue,
    SUM(Rev_CHARTER_PROP) AS Charter_Property_Revenue,
    SUM(Rev_MAIL) AS Mail_Revenue,
    SUM(Rev_MISC_OP_REV) AS Misc_Operational_Revenue,
    SUM(Rev_PROP_BAG) AS Property_Baggage_Revenue,
    SUM(Rev_PROP_FREIGHT) AS Property_Freight_Revenue,
    SUM(Rev_PUB_SVC_REVENUE) AS Public_Service_Revenue,
    SUM(Rev_RES_CANCEL_FEES) AS Reservation_Cancellation_Fees,
    SUM(Rev_TOTAL_CHARTER) AS Total_Charter_Revenue,
    SUM(Rev_TOTAL_MISC_REV) AS Total_Misc_Revenue,
    SUM(Rev_TOTAL_PROPERTY) AS Total_Property_Revenue,
    SUM(Overall_OP_REVENUES) AS Total_Revenue,

    ROUND(SUM(Rev_TRANS_REVENUE) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Transportation_Revenue,
    ROUND(SUM(Rev_TRANS_REV_PAX) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Transportation_Passenger_Revenue,
    ROUND(SUM(Rev_CHARTER_PAX) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Charter_Passenger_Revenue,
    ROUND(SUM(Rev_CHARTER_PROP) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Charter_Property_Revenue,
    ROUND(SUM(Rev_MAIL) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Mail_Revenue,
    ROUND(SUM(Rev_MISC_OP_REV) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Misc_Operational_Revenue,
    ROUND(SUM(Rev_PROP_BAG) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Property_Baggage_Revenue,
    ROUND(SUM(Rev_PROP_FREIGHT) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Property_Freight_Revenue,
    ROUND(SUM(Rev_PUB_SVC_REVENUE) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Public_Service_Revenue,
    ROUND(SUM(Rev_RES_CANCEL_FEES) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Reservation_Cancellation_Fees,
    ROUND(SUM(Rev_TOTAL_CHARTER) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Total_Charter_Revenue,
    ROUND(SUM(Rev_TOTAL_MISC_REV) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Total_Misc_Revenue,
    ROUND(SUM(Rev_TOTAL_PROPERTY) * 100.0 / NULLIF(SUM(Overall_OP_REVENUES), 0), 2) AS Pct_Total_Property_Revenue

FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
GROUP BY YEAR([YEAR])
ORDER BY Reporting_Year;


-- Identifying the years with negative profit/loss for American Airlines Inc.
SELECT
    YEAR(YEAR) AS Reporting_Year,
    SUM(Overall_OP_PROFIT_LOSS) AS Total_Profit_Loss
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR)
HAVING SUM(Overall_OP_PROFIT_LOSS) < 0
ORDER BY Reporting_Year;


-- Calculating Yearly Average Profit Margin Percentage
SELECT
    YEAR(YEAR) AS Reporting_Year,
    AVG(
        CASE 
            WHEN Overall_OP_REVENUES = 0 THEN NULL
            ELSE CAST(ROUND((Overall_OP_PROFIT_LOSS * 100.0) / Overall_OP_REVENUES, 2) AS DECIMAL(10, 2))
        END
    ) AS Avg_Profit_Margin_Percent
FROM gold.USAmericanAirlinesFinancials
WHERE UNIQUE_CARRIER_NAME = 'American Airlines Inc.'
    AND YEAR(YEAR) BETWEEN 2010 AND 2019
GROUP BY YEAR(YEAR)
ORDER BY Reporting_Year;


/* ================================================================================================================
    --------------------------------- Insights Based on the SQL Query Analysis--------------------------------------
    ================================================================================================================

1. Top Performing Airlines:
    - Airlines such as Federal Express Corporation and United Parcel Service Inc. consistently show the highest total profit/loss, 
      indicating strong operational efficiency and profitability in the cargo sector.
    - Among passenger airlines, Delta Air Lines Inc. and Southwest Airlines Co. are notable for their sustained profitability over the years.

2. Industry Trends (2010-2019):
    - The overall industry revenue, expenses, and profit/loss have shown a steady increase, with some fluctuations during 2008-2009.
    - Profit margins generally improved post-2010, reflecting better cost management and revenue optimization.

3. Profit Margin Analysis:
    - Most airlines operate on thin profit margins, often in the single digits, highlighting the competitive and cost-sensitive nature of the airline industry.
    - Years with negative profit/loss for major carriers correspond to periods of industry-wide challenges, such as fuel price spikes or economic recessions.

4. Revenue and Expense Breakdown:
    - Passenger revenue (Rev_TRANS_REV_PAX) is the largest contributor to total revenue, followed by ancillary revenues such as baggage fees and cancellation fees.
    - Maintenance, flying operations, and general administration are the largest expense categories, with maintenance costs forming a significant portion of total expenses.

5. Ancillary Revenue Growth:
    - Ancillary revenues (baggage, cancellation, and miscellaneous fees) have grown as a percentage of total revenue, reflecting a shift in airline business models to diversify income streams beyond ticket sales.

6. Seasonality and Quarterly Trends:
    - Revenue and profit/loss exhibit clear seasonality, with Q2 and Q3 typically being the strongest quarters, likely due to increased travel demand during summer months.

7. American Airlines Inc. (2010-2019) Focus:
    - American Airlines Inc. saw revenue and profit growth post-2013, with improved profit margins in most years.
    - The airline experienced negative profit/loss in 2011 and 2012 years.
    - Expense breakdown shows maintenance and flying operations as the largest cost drivers.
    - Ancillary and miscellaneous revenues have become increasingly important for overall profitability.

8. Duplicate Record Handling:
    - Deduplication by retaining records with the highest revenues and profit/loss ensures more accurate financial reporting and analysis.

9. Year-over-Year (YoY) Changes:
    - YoY analysis reveals periods of strong growth as well as years with declining revenues or profits, providing insight into industry cycles and airline-specific performance.

10. Top 5 Airlines by Recent Profitability:
     - The top 5 airlines by average profit in the last 5 years are dominated by cargo carriers and efficient passenger airlines
     
================================================================================================================
*/