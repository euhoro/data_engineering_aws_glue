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

# Script generated for node Amazon S3
AmazonS3_node1724521379544 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-euhoro-lakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1724521379544")

# Script generated for node Amazon S3
AmazonS3_node1724521416840 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1724521379544, connection_type="s3", format="json", connection_options={"path": "s3://udacity-data-euhoro-lakehouse/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724521416840")

job.commit()