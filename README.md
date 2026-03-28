# Employee Data Pipeline — Spark + PostgreSQL + Docker

## What This Project Does

I built this pipeline to take messy, raw employee data and turn it into something clean and usable. The data goes through Apache Spark for cleaning and transformation, then lands in a PostgreSQL database — all running inside Docker so it works the same on any machine.

---

## How It Works

```
Raw CSV → Apache Spark → Cleaning & Transformation → PostgreSQL
```

Nothing fancy — just a straightforward ETL pipeline that gets the job done.

---

## Tools Used

- **Apache Spark (PySpark)** — handles the heavy lifting for data processing
- **PostgreSQL** — stores the final cleaned data
- **Docker & Docker Compose** — keeps everything containerized and reproducible
- **Python** — used for data generation (Faker, Pandas)

---

## Project Structure

```
spark-postgre-project/
├── docker-compose.yml
├── Dockerfile.spark
├── spark_job.py
├── generate_data.py
├── employees_raw.csv
├── init-db.sql
└── README.md
```

---

## Getting Started

Make sure Docker Desktop is running, then follow these steps in order.

**1. Start the containers**
```bash
docker compose up --build
```

**2. Generate the sample employee data**
```bash
The sample dataset was generated using Python libraries such as Faker and Pandas in Google Colab.
Faker was used to create realistic synthetic employee data, including names, emails, addresses, and job details, while Pandas was used to structure and export the data into a CSV file.The generated dataset (`employees_raw.csv`) is used as the input for the Spark ETL pipeline.
```

**3. Run the Spark ETL job**
```bash
docker exec -it spark_master bash
/opt/spark/bin/spark-submit /opt/spark/spark_job.py
```

**4. Check the data landed correctly in PostgreSQL**
```bash
docker exec -it postgres_db psql -U sparkuser -d sparkdb
```
Then run:
```sql
SELECT COUNT(*) FROM employees_clean;
SELECT * FROM employees_clean LIMIT 5;
```

---

## What the Cleaning Step Actually Does

The raw data is pretty messy, so here's what gets fixed:

**Removing bad data**
- Duplicate records (matched on `employee_id`)
- Emails that aren't valid format
- Any hire dates set in the future

**Fixing formatting**
- Names get converted to proper case
- Salary values get stripped of `$` signs and commas so they're actual numbers

**Calculated fields**
- `age` — derived from `birth_date`
- `tenure` — how long they've been at the company, from `hire_date`
- `salary_band` — bucketed into Junior (under 50k), Mid (50k–80k), or Senior (above 80k)

**Added fields**
- `full_name` — first + last combined
- `email_domain` — extracted from the email address

---

## Database Schema

The cleaned data goes into a table called `employees_clean`.

A few things worth noting about the schema:
- `employee_id` is the primary key
- `email` has a unique constraint
- `status` defaults to `Active`
- `created_at` and `updated_at` are auto-set on insert

---

## Example

Raw input:
```
employee_id,first_name,last_name,email,salary
1001,john,DOE,John.Doe@company.com,"$75,000"
```

After the pipeline runs:
```
employee_id | full_name | email                | salary | salary_band
1001        | John Doe  | john.doe@company.com | 75000  | Mid
```

---

## Logging

Logging is implemented using simple print statements to track the progress of the ETL pipeline. 

The job outputs status messages at each major stage, including data loading, transformation, and database write operations. It also reports any errors encountered during execution.

This approach ensures basic traceability and helps in debugging and monitoring the pipeline execution.

---

## Author

Manpreet Singh Saini
