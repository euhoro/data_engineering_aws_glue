import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724523232717 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1724523232717")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724523247152 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724523247152")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724523265971 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724523265971")

# Script generated for node SQL Query
SqlQuery0 = '''
select myCustomer.*,myAccelerator.*,myStepTrainer.sensorReadingTime,myStepTrainer.distanceFromObject from myCustomer inner join
myAccelerator on myCustomer.email = myAccelerator.user
inner join myStepTrainer on myAccelerator.timestamp =
myStepTrainer.sensorreadingtime
'''
SQLQuery_node1724523283212 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myStepTrainer":AWSGlueDataCatalog_node1724523265971, "myAccelerator":AWSGlueDataCatalog_node1724523247152, "myCustomer":AWSGlueDataCatalog_node1724523232717}, transformation_ctx = "SQLQuery_node1724523283212")

# Script generated for node Amazon S3
AmazonS3_node1724523660479 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1724523283212, connection_type="s3", format="json", connection_options={"path": "s3://udacity-data-euhoro-lakehouse/machine_learning/curated5/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724523660479")

job.commit()