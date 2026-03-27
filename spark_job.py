from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ETL").getOrCreate()

# Load CSV
df = spark.read.csv("/opt/spark/employees_raw.csv", header=True, inferSchema=True)

# Remove duplicates
df = df.dropDuplicates(["employee_id"])

# Clean names
df = df.withColumn("first_name", initcap("first_name")) \
       .withColumn("last_name", initcap("last_name"))

# Clean email
df = df.withColumn("email", lower("email"))
df = df.filter(col("email").contains("@"))

# Clean salary
df = df.withColumn("salary",
    regexp_replace("salary", "[$,]", "").cast("double")
)

# Remove future hire_date
df = df.filter(col("hire_date") <= current_date())

# Age
df = df.withColumn("age", year(current_date()) - year("birth_date"))

# Tenure
df = df.withColumn("tenure_years",
    year(current_date()) - year("hire_date")
)

# Salary band
df = df.withColumn("salary_band",
    when(col("salary") < 50000, "Junior")
    .when(col("salary") <= 80000, "Mid")
    .otherwise("Senior")
)

# Full name
df = df.withColumn("full_name", concat_ws(" ", "first_name", "last_name"))

# Email domain
df = df.withColumn("email_domain", split(col("email"), "@")[1])

# PostgreSQL connection
url = "jdbc:postgresql://postgres_db:5432/sparkdb"

properties = {
    "user": "sparkuser",
    "password": "sparkpassword",
    "driver": "org.postgresql.Driver"
}

# Write to PostgreSQL
df.write.jdbc(
    url=url,
    table="employees_clean",
    mode="append",
    properties=properties
)

df.show()