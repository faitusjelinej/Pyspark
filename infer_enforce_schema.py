# Infer Schema for CSV

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV with Inferred Schema").getOrCreate()

# Correct DBFS paths
csv_path = "dbfs:/FileStore/shared_uploads/faitusjeline@gmail.com/sample_csv.csv"


# Read CSV with inferSchema
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_path)
df1.show()
df1.printSchema()

# Enforce Schema for CSV

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Correct DBFS path
csv_path = "dbfs:/FileStore/shared_uploads/faitusjeline@gmail.com/sample_csv.csv"

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Read CSV with enforced schema
df1 = spark.read.format("csv").option("header", "true").schema(schema).load(csv_path)
df1.show()
df1.printSchema()
