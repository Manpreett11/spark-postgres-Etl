from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ETL").getOrCreate()

print("Starting ETL process...")

# Load CSV
df = spark.read.csv("/opt/spark/employees_raw.csv", header=True, inferSchema=True)
print("CSV loaded")

# 🔥 FIX TYPES
df = df.withColumn("employee_id", col("employee_id").cast("int"))
df = df.withColumn("manager_id", col("manager_id").cast("int"))

# 🔥 FIX DATE TYPES
df = df.withColumn("birth_date", to_date(col("birth_date"), "yyyy-MM-dd"))
df = df.withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))

# Remove duplicates
df = df.dropDuplicates(["employee_id"])
df = df.dropDuplicates(["email"])
print("Duplicates removed")

# Clean names
df = df.withColumn("first_name", initcap("first_name")) \
       .withColumn("last_name", initcap("last_name"))
print("Names cleaned")

# Clean email
df = df.withColumn("email", lower("email"))
df = df.filter(col("email").contains("@"))
print("Email cleaned")

# Clean salary
df = df.withColumn(
    "salary",
    regexp_replace("salary", "[$,]", "").cast("double")
)
print("Salary cleaned")

# 🔥 HANDLE FUTURE DATES (SAFE FIX)
df = df.withColumn(
    "hire_date",
    when(col("hire_date") > current_date(), current_date())
    .otherwise(col("hire_date"))
)

# Age
df = df.withColumn("age", year(current_date()) - year("birth_date"))

# Tenure
df = df.withColumn(
    "tenure_years",
    year(current_date()) - year("hire_date")
)

# Salary band
df = df.withColumn(
    "salary_band",
    when(col("salary") < 50000, "Junior")
    .when(col("salary") <= 80000, "Mid")
    .otherwise("Senior")
)

# Full name
df = df.withColumn("full_name", concat_ws(" ", "first_name", "last_name"))

# Email domain
df = df.withColumn("email_domain", split(col("email"), "@")[1])

print("Transformations complete")

# PostgreSQL connection
url = "jdbc:postgresql://postgres_db:5432/sparkdb"

properties = {
    "user": "sparkuser",
    "password": "sparkpassword",
    "driver": "org.postgresql.Driver"
}

# Write to PostgreSQL
print("Writing to database...")
df.write.jdbc(
    url=url,
    table="employees_clean",
    mode="append",
    properties=properties
)

print("Data successfully written to PostgreSQL")

df.show(5)