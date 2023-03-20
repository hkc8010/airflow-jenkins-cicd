DROP TABLE IF EXISTS payments_recon_staging.stg__rent_axisupi;
CREATE TABLE payments_recon_staging.stg__rent_axisupi
(
	autoid BIGINT NOT NULL IDENTITY(1,1) ENCODE az64
	,rrn BIGINT   ENCODE az64
	,txnid VARCHAR(500)   ENCODE lzo
	,orderid VARCHAR(500)   ENCODE lzo
	,amount NUMERIC(16,4)   ENCODE az64
	,mobile_no BIGINT   ENCODE az64
	,bankname VARCHAR(500)   ENCODE lzo
	,maskedaccountnumber VARCHAR(500)   ENCODE lzo
	,ifsc VARCHAR(500)   ENCODE lzo
	,vpa VARCHAR(500)   ENCODE lzo
	,account_cust_name VARCHAR(500)   ENCODE lzo
	,respcode VARCHAR(500)   ENCODE lzo
	,response VARCHAR(500)   ENCODE lzo
	,txn_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,creditvpa VARCHAR(500)   ENCODE lzo
	,remarks VARCHAR(500)   ENCODE lzo
	,surcharge NUMERIC(16,4)   ENCODE az64
	,tax NUMERIC(16,4)   ENCODE az64
	,debit_amount NUMERIC(16,4)   ENCODE az64
	,mdr_tax NUMERIC(16,4)   ENCODE az64
	,merchant_id VARCHAR(500)   ENCODE lzo
	,unq_cust_id VARCHAR(500)   ENCODE lzo
	,PRIMARY KEY (autoid)
	,etl__file_name VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;
