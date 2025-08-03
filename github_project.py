from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, regexp_extract, input_file_name
import os

# 1. create SparkSession
spark = SparkSession.builder \
    .appName("GitHub Repos Project") \
    .config("spark.jars", "file:///E:/drivers/postgresql-42.7.3.jar") \
    .getOrCreate()
print("SparkSession Created")

# 2. read all json files
data_dir = "E:/data-engineering/github_project/raw-data"
json_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".json")]


# 3.when reading data creat search_term column
df = spark.read.option("multiLine", True).json(json_files)
df = df.withColumn("search_term", regexp_extract(input_file_name(), r'([^\\/]+)$', 1))

# 4. cleaning data
df = df.filter(col("language").isNotNull() & col("stars").isNotNull())

# 5. creating the required tables :
#   programming_lang table
programming_lang_df = df.groupBy("language").agg(count("*").alias("repo_count"))

#################   organizations_stars table
organizations_stars_df = df.filter(col("type") == "Organization") \
    .groupBy("username") \
    .agg(_sum("stars").alias("total_stars")) \
    .withColumnRenamed("username", "org_name")

################   search_terms_relevance table
search_terms_df = df.withColumn("relevance_score", 
    1.5 * col("forks") + 1.32 * col("subscribers") + 1.04 * col("stars")
).groupBy("search_term") \
 .agg(_sum("relevance_score").alias("relevance_score"))

############### PostgreSQL connection
pg_url = "jdbc:postgresql://localhost:5432/github_data"
pg_properties = {
    "user": "postgres",       
    "password": "1234", 
    "driver": "org.postgresql.Driver"
}

# 7   save new tables in PostgreSQL database
programming_lang_df.write.jdbc(url=pg_url, table="programming_lang", mode="overwrite", properties=pg_properties)
organizations_stars_df.write.jdbc(url=pg_url, table="organizations_stars", mode="overwrite", properties=pg_properties)
search_terms_df.write.jdbc(url=pg_url, table="search_terms_relevance", mode="overwrite", properties=pg_properties)

