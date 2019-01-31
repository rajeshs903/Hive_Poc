scala> val df1 = spark.read.format("csv").option("header","true").option("sep",";").load("/user/rajeshs/datasets/scenarios/lic.csv")
df1: org.apache.spark.sql.DataFrame = [id: string, Name: string ... 6 more fields]

 df1.show
+---+-----------+----------+--------+-----------+--------------+----------+----------+
| id|       Name|   Address|    type|enrolled_on|          bank|       dob|     phone|
+---+-----------+----------+--------+-----------+--------------+----------+----------+
|  1|Ram Chandra|Ayodhya UP| Primium| 01-10-2010|retail-banking|02-10-1980|9999999999|
|  2|     Laxman|Ayodhya UP|Standard| 01-11-2011|    Creditcard|01-11-1966|9999999910|
+---+-----------+----------+--------+-----------+--------------+----------+----------+

{"requests"[
	
	mask{
	"on":[name];"algo","ReplaceVowelByX"
	}
]
}

