CREATE TABLE employees_JSON (
  name         STRING,
  salary       FLOAT,
  subordinates ARRAY<STRING>,
  deductions   MAP<STRING, FLOAT>,
  address      STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'

