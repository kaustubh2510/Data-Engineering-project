------------------------
-- CREATE VIEW purchaseamount
------------------------
CREATE VIEW gold.purchaseamount
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://kdecommercedatalake.blob.core.windows.net/silver/brand_purchase_amount/',
            FORMAT = 'PARQUET'
        ) as QUER1

------------------------
-- CREATE VIEW purchase
------------------------

CREATE VIEW gold.purchase
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://kdecommercedatalake.blob.core.windows.net/silver/brand_purchases/',
            FORMAT = 'PARQUET'
        ) as QUER2

------------------------
-- CREATE VIEW bview
------------------------
CREATE VIEW gold.bview
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://kdecommercedatalake.blob.core.windows.net/silver/brand_views/',
            FORMAT = 'PARQUET'
        ) as QUER2



----------------------------
-- CREATE PASSWORD FOR DATA
---------------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD ='Kaustubh@2004'

------------------------
-- CREATE CREDENTIAL
-----------------------
CREATE DATABASE SCOPED CREDENTIAL cred_ecomm
WITH
    IDENTITY ="Managed IDENTITY"
            
--------------------------------------
-- CREATE EXTERNAL DATA SOURCE 
--------------------------------------
CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION ='https://kdecommercedatalake.blob.core.windows.net/silver',
    CREDENTIAL = cred_ecomm
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION ='https://kdecommercedatalake.blob.core.windows.net/gold',
    CREDENTIAL = cred_ecomm
)
----------------------------
-- DEFINING THE FILE FORMATE
----------------------------
CREATE EXTERNAL FILE FORMAT formate_parquet
WITH
(
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
)


----------------------------
-- SAVE ALL FILE IN GOLD CONTAINER
----------------------------
CREATE EXTERNAL TABLE gold.extpurchaseamount
WITH
(
    LOCATION='extpurchaseamount',
    DATA_SOURCE= source_gold,
    FORMAT_TYPE= formate_parquet
)
AS
SELECT * FROM gold.purchaseamount

CREATE EXTERNAL TABLE gold.extpurchase
WITH
(
    LOCATION='extpurchase',
    DATA_SOURCE= source_gold,
    FORMAT_TYPE= formate_parquet
)
AS
SELECT * FROM gold.extpurchase


CREATE EXTERNAL TABLE gold.extbview
WITH
(
    LOCATION='extbview',
    DATA_SOURCE= source_gold,
    FORMAT_TYPE= formate_parquet
)
AS
SELECT * FROM gold.extbview