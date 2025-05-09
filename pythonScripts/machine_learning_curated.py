import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1746820767113 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1746820767113")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1746725849528 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1746725849528")

# Script generated for node Filter Step Trainer Trusted
SqlQuery0 = '''
SELECT user,x,y,z,stt.*
FROM  stt JOIN at on stt.sensorreadingtime = at.timestamp
'''
FilterStepTrainerTrusted_node1746729486050 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"at":AccelerometerTrusted_node1746725849528, "stt":StepTrainerTrusted_node1746820767113}, transformation_ctx = "FilterStepTrainerTrusted_node1746729486050")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterStepTrainerTrusted_node1746729486050, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746725818992", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1746726108990 = glueContext.getSink(path="s3://udacity-stedi-lake/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1746726108990")
AmazonS3_node1746726108990.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1746726108990.setFormat("json")
AmazonS3_node1746726108990.writeFrame(FilterStepTrainerTrusted_node1746729486050)
job.commit()