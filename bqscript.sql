CREATE OR REPLACE TABLE wd31we42-01.retaildb.tbltrans(txnid INT64,txndate string,custid int64,txnamount float64,
category string,product string,city string,state string,payment string);


CREATE OR REPLACE TABLE wd31we42-01.retaildb.tbltrans_stg(txnid INT64,txndate string,custid int64,txnamount float64,
category string,product string,city string,state string,payment string);


CREATE OR REPLACE TABLE wd31we42-01.retaildb.tblcustomer(custid INT64,fname string,lname string,age int64,prof string);

CREATE OR REPLACE TABLE wd31we42-01.retaildb.tbltxnsmry(product string,category string,txndate string,custtype string,txncount INT64,txnamount float64,created_ts timestamp);


CREATE OR REPLACE PROCEDURE retaildb.sp_processdata()
BEGIN

INSERT INTO wd31we42-01.retaildb.tbltxnsmry
SELECT PRODUCT,CATEGORY,TXNDATE,CUSTTYPE,COUNT(TXNID),ROUND(SUM(TXNAMOUNT),2),CURRENT_TIMESTAMP() 
FROM  wd31we42-01.retaildb.tbltrans_stg TS INNER JOIN 
(SELECT CUSTID,PROF,
CASE WHEN AGE <25 THEN 'Young Adults'
WHEN AGE < 60 THEN 'Adults'
ELSE 'Senior' end as CUSTTYPE 
FROM wd31we42-01.retaildb.tblcustomer) C
ON TS.CUSTID = C.CUSTID 
WHERE PROF IS NOT NULL
GROUP BY PRODUCT,CATEGORY,TXNDATE,CUSTTYPE;

INSERT INTO wd31we42-01.retaildb.tbltrans select * from wd31we42-01.retaildb.tbltrans_stg;


END;


