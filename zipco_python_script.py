import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, to_date

# Parse arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths
s3_input_path = 's3://zipco-bank-project-bucket/raw_data/zipco_bank_data.csv'
s3_output_path = 's3://zipco-bank-project-bucket/Output/'

# ETL logic
# Reading data from S3
bank_data = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv", 
    transformation_ctx="bank_data"
)

# Example: Transformations (Add your data processing logic here)
mapped_data = ApplyMapping.apply(
    frame=bank_data, 
    mappings=[
        ("customerid", "string", "customerid", "string"),
        ("customername", "string", "customername", "string"),
        ("emailaddress", "string", "emailaddress", "string"),
        ("phonenumber", "string", "phonenumber", "string"),
        ("accountid", "string", "accountid", "string"),
        ("balance", "double", "balance", "double"),
        ("transactionid", "string", "transactionid", "string"),
        ("transactiondate", "string", "transactiondate", "string"),
        ("transactiontype", "string", "transactiontype", "string"),
        ("amount", "double", "amount", "double"),
        ("categoryid", "string", "categoryid", "string"),
        ("transactiondescription", "string", "transactiondescription", "string"),
        ("marketdataid", "string", "marketdataid", "string"),
        ("price", "double", "price", "double")
    ],
    transformation_ctx="mapped_data"
)

# Convert to DataFrame for processing
df = mapped_data.toDF()

# Data cleaning transformation (remove null values)
df = df.filter(df.customerid.isNotNull() & df.accountid.isNotNull())

# Trim whitespaces from all columns
for column in df.columns:
    if df.schema[column].dataType == 'StringType':
        df = df.withColumn(column, trim(df[column]))

# Convert transaction dates to datetype and filter out invalid dates
df = df.withColumn("transactiondate", to_date(col("transactiondate"), "yyyy-MM-dd")) 
df = df.filter(col("transactiondate").isNotNull())

# Set negative balance to zero
df = df.withColumn("balance", f.when(f.col("balance") < 0, 0).otherwise(f.col("balance")))

# Convert back to DynamicFrame
dynamic_frame_cleaned = DynamicFrame.fromDF(df, glueContext, "dynamic_frame_cleaned")

# Save as CSV
csv_sink = glueContext.getSink(
    path=s3_output_path + "csv/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="csv_sink"
)
csv_sink.setCatalogInfo(
    catalogDatabase="zipco_database", catalogTableName="zipco_csv_formatted_dataset"
)
csv_sink.setFormat("csv")
csv_sink.writeFrame(dynamic_frame_cleaned)

# Save as Parquet
parquet_sink = glueContext.getSink(
    path=s3_output_path + "parquet/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="parquet_sink"
)
parquet_sink.setCatalogInfo(
    catalogDatabase="zipco_database", catalogTableName="zipco_parquet_formatted_dataset"
)
parquet_sink.setFormat("glueparquet")
parquet_sink.writeFrame(dynamic_frame_cleaned)

# Save as JSON
json_sink = glueContext.getSink(
    path=s3_output_path + "json/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="json_sink"
)
json_sink.setCatalogInfo(
    catalogDatabase="zipco_database", catalogTableName="zipco_json_formatted_dataset"
)
json_sink.setFormat("json")
json_sink.writeFrame(dynamic_frame_cleaned)

# Commit the job
job.commit()
