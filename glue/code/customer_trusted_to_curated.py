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
AWSGlueDataCatalog_node1724478885657 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724478885657")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1724478852994 = glueContext.create_dynamic_frame.from_catalog(database="device", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1724478852994")

# Script generated for node Join
Join_node1724478930722 = Join.apply(frame1=AWSGlueDataCatalog_node1724478852994, frame2=AWSGlueDataCatalog_node1724478885657, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724478930722")

# Script generated for node Change Schema
ChangeSchema_node1724604848983 = ApplyMapping.apply(frame=Join_node1724478930722, mappings=[("serialNumber", "string", "serialNumber", "string"), ("`.customerName`", "string", "`.customerName`", "string"), ("birthDay", "string", "birthDay", "string"), ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"), ("`.email`", "string", "`.email`", "string"), ("shareWithResearchAsOfDate", "long", "shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "registrationDate", "long"), ("customerName", "string", "customerName", "string"), ("`.phone`", "string", "`.phone`", "string"), ("`.shareWithPublicAsOfDate`", "long", "`.shareWithPublicAsOfDate`", "long"), ("shareWithFriendsAsOfDate", "long", "shareWithFriendsAsOfDate", "long"), ("`.birthDay`", "string", "`.birthDay`", "string"), ("`.lastUpdateDate`", "long", "`.lastUpdateDate`", "long"), ("`.shareWithFriendsAsOfDate`", "long", "`.shareWithFriendsAsOfDate`", "long"), ("`.registrationDate`", "long", "`.registrationDate`", "long"), ("`.serialNumber`", "string", "`.serialNumber`", "string"), ("email", "string", "email", "string"), ("lastUpdateDate", "long", "lastUpdateDate", "long"), ("`.shareWithResearchAsOfDate`", "long", "`.shareWithResearchAsOfDate`", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1724604848983")

# Script generated for node Amazon S3
AmazonS3_node1724604967789 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1724604848983, connection_type="s3", format="json", connection_options={"path": "s3://udacity-data-euhoro-lakehouse/customer/trusted_home_work/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724604967789")

job.commit()