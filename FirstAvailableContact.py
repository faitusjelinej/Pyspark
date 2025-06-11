from pyspark.sql import SparkSession

from pyspark.sql.functions import coalesce


# Start Spark session
spark = SparkSession.builder.appName("FirstContact").getOrCreate()

# Sample data
data = [
    ("Alice", "9876543210", "alice@gmail.com", "alice@company.com"),
    ("Bob", None, "bob@gmail.com", "bob@company.com"),
    ("Charlie", None, None, "charlie@company.com"),
    ("David", None, None, None)
]

columns = ["CustomerName", "Phone", "PersonalEmail", "WorkEmail"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df.show()

result_df = df.select("CustomerName",
                      coalesce("Phone", "PersonalEmail", "WorkEmail").alias("FirstAvailableContact"))

result_df.show()


CREATE TABLE CustomerContacts (
    CustomerName VARCHAR(50),
    Phone VARCHAR(20),
    PersonalEmail VARCHAR(100),
    WorkEmail VARCHAR(100)
);

INSERT INTO CustomerContacts (CustomerName, Phone, PersonalEmail, WorkEmail) VALUES
('Alice', '9876543210', 'alice@gmail.com', 'alice@company.com'),
('Bob', NULL, 'bob@gmail.com', 'bob@company.com'),
('Charlie', NULL, NULL, 'charlie@company.com'),
('David', NULL, NULL, NULL);

select CustomerName,
coalesce(Phone, PersonalEmail, WorkEmail) as FirstAvailableContact
from CustomerContacts
;



