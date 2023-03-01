import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
reviews_table = glueContext.create_dynamic_frame.from_catalog(
    database='steam_reviews',
    table_name='reviews'
)
reviews_table.printSchema()
# convert the dynamic dataframe to a Spark dataframe
df = reviews_table.toDF()

import pyspark.sql.functions as F
from pyspark.sql.functions import isnan, when, count, col

df = df.dropna()


df = df.filter((df["review_text"] != "") & (df["review_text"] != ' ') & df["review_text"].isNotNull() & ~isnan(df["review_text"]))
df = df.filter((df["app_name"] != "") & (df["app_name"] != ' ') & df["app_name"].isNotNull() & ~isnan(df["app_name"]))

from pyspark.sql.functions import trim, lower
df_records = df.withColumn("review_text", trim(df.review_text))
df_records = df_records.filter(df_records["review_text"] != "Early Access Review")

top20_games_list = list(df_records.groupby('app_name').count().sort('count', ascending = False).select('app_name').toPandas()['app_name'][:20])

df_records = df_records.filter(F.col('app_name').isin(top20_games_list)).sort('app_name')

df_records = df_records.withColumn("review_text", lower(df_records.review_text))

from awsglue.dynamicframe import DynamicFrame

dyf_records = DynamicFrame.fromDF(df_records, glueContext, "nested")
# write down the data in a Dynamic Frame to S3 location. 
glueContext.write_dynamic_frame.from_options(
                        frame = dyf_records, # dynamic frame
                        connection_type="s3", 
                        connection_options = {"path": "s3://siads696-wi23-steam-data/write_down_dyf_to_s3/"}, 
                        format = "csv", # write as a csv
                        format_options={
                            "separator": ","
                            },
                        transformation_ctx = "datasink2")
job.commit()