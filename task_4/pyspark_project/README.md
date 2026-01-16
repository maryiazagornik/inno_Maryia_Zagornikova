#  Spark

This project implements a modular ETL pipeline to analyze the **Sakila PostgreSQL database** using **Apache Spark (PySpark)**.
The application is fully containerized with Docker, ensuring a consistent environment for data processing and reporting.

##  Project Overview

The application connects to a PostgreSQL database, extracts data across multiple tables (films, actors, rentals, customers), performs complex transformations and aggregations, and outputs the results as formatted reports in the console.

**Key Features:**
* **Containerized Execution:** Runs in a managed Docker environment with Python 3.10, Java, and Spark.
* **Modular Design:** Clear separation of configuration, business logic (`jobs`), and utility functions.
* **Professional Reporting:** Results are presented in clean, readable ASCII tables.
* **Secure Configuration:** Database credentials are managed via environment variables.

## Project Structure

```text
pyspark-project/
├── config/
│   └── config.yaml          # Application and Database configuration
├── jobs/
│   └── analytics.py         # Core analytical logic and transformations
├── utils/
│   ├── formatting.py        # Table formatting utilities (Tabulate)
│   ├── logger.py            # Custom logging configuration
│   └── spark.py             # SparkSession management
├── Dockerfile               # Image definition
├── docker-compose.yml       # Service orchestration
├── main.py                  # Application entry point
├── requirements.txt         # Python dependencies
└── .env                     # Secrets (not committed to Git)
```

## Prerequisites

* **Docker** & **Docker Compose** installed.
* A running instance of **PostgreSQL** with the `sakila` sample database.

## Setup & Run

### 1. Configuration
Create a `.env` file in the root directory to store your database credentials.

> **Note:** The application uses `host.docker.internal` to connect to a database running on your host machine.

```bash
# .env
DB_USER=postgres
DB_PASSWORD=your_password
```

### 2. Execution
Run the following command to build the Docker image and execute the pipeline:

```bash
docker-compose run spark-job
```

## Analytical Tasks

The pipeline executes the following 7 tasks and prints the results:

1. **Category Analysis:** Counts the number of films in each category (sorted descending).
2. **Top Actors:** Identifies the top 10 actors whose movies generated the most rentals.
3. **Cost Analysis:** Finds the movie category with the highest total replacement cost.
4. **Inventory Check:** Lists movies that exist in the catalog but are currently missing from the inventory.
5. **Genre-Specific Actors:** Finds the top actors appearing in the "Children" category.
6. **Customer Geography:** Aggregates active and inactive customers by city.
7. **Rental Duration Trends:** Compares total rental hours for cities starting with **'a'** versus cities containing a hyphen **'-'**.

## Technology Stack

* **Language:** Python 3.10
* **Engine:** Apache Spark 3.5 (PySpark)
* **Database Driver:** PostgreSQL JDBC
* **Infrastructure:** Docker
* **Libraries:** PyYAML, Tabulate