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

# Script generated for node Amazon S3
AmazonS3_node1724519760657 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-euhoro-lakehouse/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1724519760657")

# Script generated for node Amazon S3
AmazonS3_node1724519824890 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://udacity-data-euhoro-lakehouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1724519824890")

# Script generated for node SQL Query
SqlQuery0 = '''
select myStepTrainer.serialnumber as serialnumber ,myStepTrainer.sensorReadingTime as sensorreadingtime,myStepTrainer.distanceFromObject 
from myStepTrainer inner join myCustomer on myStepTrainer.serialnumber= myCustomer.serialnumber
'''
SQLQuery_node1724519787756 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myCustomer":AmazonS3_node1724519760657, "myStepTrainer":AmazonS3_node1724519824890}, transformation_ctx = "SQLQuery_node1724519787756")

# Script generated for node Amazon S3
AmazonS3_node1724520703044 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1724519787756, connection_type="s3", format="json", connection_options={"path": "s3://udacity-data-euhoro-lakehouse/step_trainer/trusted5/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1724520703044")

job.commit()