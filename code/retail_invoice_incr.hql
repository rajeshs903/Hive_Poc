CREATE TABLE IF NOT EXISTS rajeshs_task_db.retail_invoice_incr_text (
InvoiceNo INT ,
StockCode INT,
Description STRING,
Quantity INT,
InvoiceDate TIMESTAMP,
UnitPrice DECIMAL(9,2),
CustomerID DOUBLE,
Country STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/rajeshs/external_table_location/wh/retail_invoice_incr_text'
tblproperties ("skip.header.line.count"="1");
