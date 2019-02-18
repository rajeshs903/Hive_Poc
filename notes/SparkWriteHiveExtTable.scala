create external table CustomerInvoice(
InvoiceNo INT,
StockCode INT,
Description STRING,
Quantity INT,
InvoiceDate TIMESTAMP,
UnitPrice FLOAT,
CustomerID FLOAT,
Country STRING
)
ROW FORMAT DELIMITED
fields terminated by ','
lines terminated by '\n'
location '/user/rajeshs/external_table_location/by-day/'
tblproperties ("skip.header.line.count"="1");

invoiceno       stockcode       description                     quantity        invoicedate             unitprice       customerid      country
581000            22961         JAM MAKING SET PRINTED          12               2011-12-07 08:03:00     1.45           12720.0         Germany
581000            21531         RED RETROSPOT SUGAR JAM BOWL    6                2011-12-07 08:03:00     2.55           12720.0         Germany
581000            22627         MINT KITCHEN SCALES             2                2011-12-07 08:03:00     8.5            12720.0         Germany



select  from CustomerInvoice limit 4;

 cast(from_unixtime(unix_timestamp('2017-07-03','yyyy-MM-dd'),'yyyyMMdd') as int
msck repair table CustomerInvoice;

drop table CustomerInvoice;

Alter table CustomerInvoice set location '/user/rajeshs/external_table_location/by-day/';
Alter table CustomerInvoice set location '/user/rajeshs/external_table_location/Daily';
SELECT * from CustomerInvoice limit 5;

MSCK REPAIR TABLE CustomerInvoice;

SET hive.resultset.use.unique.column.names=false;
SET hive.cli.print.header = true;
SET hive.exec.dynamic.partition=true; 
SET hive.exec.dynamic.partition.mode=nonstrict;
    


hdfs dfs -cp /user/rajeshs/external_table_location/by-day/2011-12-07.csv /user/rajeshs/external_table_location/Daily


create external table CustomerInvoice_partitioned(
InvoiceNo INT,
StockCode INT,
Description STRING,
Quantity INT,
UnitPrice FLOAT,
CustomerID FLOAT,
Country STRING
)
PARTITIONED BY (InvoiceDate INT)
ROW FORMAT DELIMITED
fields terminated by ','
lines terminated by '\n'
location '/user/rajeshs/external_table_location/Daily/'
tblproperties ("skip.header.line.count"="1");


msck repair table CustomerInvoice_partitioned;
drop table CustomerInvoice_partitioned;


insert overwrite table CustomerInvoice_partitioned partition(InvoiceDate) select invoiceno,stockcode,description,quantity,unitprice,customerid,country,cast(from_unixtime(unix_timestamp(to_date(InvoiceDate),'yyyy-MM-dd'),'yyyyMMdd') as int) from CustomerInvoice;



select * from CustomerInvoice_partitioned limit 5;
invoiceno       stockcode       description    						quantity        unitprice       customerid      country 		invoicedate
581000  		21531   		RED RETROSPOT SUGAR JAM BOWL    			6       2.55    			12720.0 		Germany 	20111207
581000  		22627   		MINT KITCHEN SCALES     					2       8.5     			12720.0 		Germany 	20111207
581000  		22625   		RED KITCHEN SCALES      					2       8.5     			12720.0 		Germany 	20111207
581000  		22960   		JAM MAKING SET WITH JARS        			12      3.75    			12720.0 		Germany 	20111207
581000  		22554   		PLASTERS IN TIN WOODLAND ANIMALS       		12      1.65    			12720.0 		Germany 	20111207

  
  
  alter table CustomerInvoice_partitioned drop if exists partition (invoicedate<>20111207);

  
  
  
  
  case class CustomerInvoice_partitioned(InvoiceNo: Integer, month: Integer, id: Integer, cat: Integer)

  
  
case class CustomerInvoice (InvoiceNo: Integer,StockCode: Integer,Description: String,Quantity: Integer,InvoiceDate:java.sql.Timestamp ,UnitPrice: Double,CustomerID: Double,Country: String)

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

scala> val schema = ScalaReflection.schemaFor[CustomerInvoice].dataType.asInstanceOf[StructType]
schema: org.apache.spark.sql.types.StructType = StructType(StructField(InvoiceNo,IntegerType,true), StructField(StockCode,IntegerType,true), StructField(Description,StringType,true), StructField(Quantity,IntegerType,true), StructField(InvoiceDate,TimestampType,true), StructField(UnitPrice,DoubleType,false), StructField(CustomerID,DoubleType,false), StructField(Country,StringType,true))



InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
581000,22961,JAM MAKING SET PRINTED,12,2011-12-07 08:03:00,1.45,12720.0,Germany
581000,21531,RED RETROSPOT SUGAR JAM BOWL,6,2011-12-07 08:03:00,2.55,12720.0,Germany


val custDF = spark.read.format("csv").option("inferSchema","false").option("header","true").schema(schema).load("/user/rajeshs/external_table_location/by-day/")
+---------+---------+----------------------------+--------+-------------------+---------+----------+-------+
|InvoiceNo|StockCode|Description                 |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country|
+---------+---------+----------------------------+--------+-------------------+---------+----------+-------+
|581000   |22961    |JAM MAKING SET PRINTED      |12      |2011-12-07 08:03:00|1.45     |12720.0   |Germany|
|581000   |21531    |RED RETROSPOT SUGAR JAM BOWL|6       |2011-12-07 08:03:00|2.55     |12720.0   |Germany|
|581000   |22627    |MINT KITCHEN SCALES         |2       |2011-12-07 08:03:00|8.5      |12720.0   |Germany|
+---------+---------+----------------------------+--------+-------------------+---------+----------+-------+
only showing top 3 rows
scala> val custDF = spark.read.format("csv").option("inferSchema","false").option("header","true").schema(schema).load("/user/rajeshs/external_table_location/by-day/")
custDF: org.apache.spark.sql.DataFrame = [InvoiceNo: int, StockCode: int ... 6 more fields]

scala> custDF.printSchema
root
 |-- InvoiceNo: integer (nullable = true)
 |-- StockCode: integer (nullable = true)
 |-- Description: string (nullable = true)
 |-- Quantity: integer (nullable = true)
 |-- InvoiceDate: timestamp (nullable = true)
 |-- UnitPrice: double (nullable = true)
 |-- CustomerID: double (nullable = true)
 |-- Country: string (nullable = true)


 
 
