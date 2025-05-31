from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


spark = SparkSession.builder.appName("WhenOtherwiseDemo").getOrCreate()


data = [
    ("John", 25),
    ("Alice", 17),
    ("Bob", 15),
    ("Diana", 30),
    ("Chris", 12),
    ("Ella", 8),
    ("Frank", 19),
    ("Grace", 65)
]


df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

df = df.withColumn("Category",
                   when(col("Age") <= 12, "Child")
                    .when((col("Age") >= 13) & (col("Age") <= 19) , "Teen")
                    .when((col("Age")  >= 20) & (col("Age") <= 59) , "Adult")
                    .otherwise("Senior")      
)
df.show() 
