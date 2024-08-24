CREATE EXTERNAL TABLE IF NOT EXISTS `device`.`machine_learning` (
  `serialnumber` string,
  `sharewithpublicasofdate` bigint,
  `birthday` date,
  `registrationdate` bigint,
  `sharewithresearchasofdate` bigint,
  `customername` string,
  `email` string,
  `lastupdatedate` bigint,
  `phone` bigint,
  `sharewithfriendsasofdate` bigint,
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float,
   `sensorReadingTime` bigint,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-data-euhoro-lakehouse/machine_learning/curated5/'
TBLPROPERTIES ('classification' = 'json');