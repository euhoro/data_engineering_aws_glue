import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724478885657 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="accelerometer", transformation_ctx="AWSGlueDataCatalog_node1724478885657")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724478852994 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724478852994")

# Script generated for node Join
Join_node1724478930722 = Join.apply(frame1=AWSGlueDataCatalog_node1724478852994, frame2=AWSGlueDataCatalog_node1724478885657, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724478930722")

# #Script generated for node Amazon S3
#AmazonS3_node1724479036315 = glueContext.write_dynamic_frame.from_options(frame=Join_node1724478930722, connection_type="s3", format="json", connection_options={"path": "s3://udacity-data-euhoro-lakehouse/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724479036315")
# Script generated for node AWS Glue Data Catalog
AmazonS3_node1724479036315 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1724478930722, database="device", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AmazonS3_node1724479036315")

job.commit()