CREATE EXTERNAL TABLE IF NOT EXISTS `device`.`accelerometer` (
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float
) COMMENT "accelerometer"
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-data-euhoro-lakehouse/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');