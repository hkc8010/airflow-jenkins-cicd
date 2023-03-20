CREATE TABLE IF NOT EXISTS payments_recon.rent_axisupi
(
    rrn LONG
    ,txnid STRING
    ,orderid STRING
    ,amount DOUBLE
    ,mobile_no LONG
    ,bankname STRING
    ,maskedaccountnumber STRING
    ,ifsc STRING
    ,vpa STRING
    ,account_cust_name STRING
    ,respcode STRING
    ,response STRING
    ,txn_date TIMESTAMP
    ,creditvpa STRING
    ,remarks STRING
    ,surcharge DOUBLE
    ,tax DOUBLE
    ,debit_amount DOUBLE
    ,mdr_tax DOUBLE
    ,merchant_id STRING
    ,unq_cust_id STRING
    ,etl__file_name STRING
    ,etl__load_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES ( 
    'delta.autoOptimize.optimizeWrite' = 'true', 
    'delta.autoOptimize.autoCompact' = 'true');
