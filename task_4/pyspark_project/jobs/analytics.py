from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.logger import setup_logger
from utils.formatting import print_pretty


class SakilaAnalyzer:
    def __init__(self, spark, db_config, db_user, db_password):
        self.spark = spark
        self.logger = setup_logger("Analyzer")
        self.jdbc_url = db_config['url']
        self.properties = {
            "user": db_user,
            "password": db_password,
            "driver": db_config['driver']
        }
        self.dfs = {}

    def _read_table(self, table_name) -> DataFrame:
        if table_name not in self.dfs:
            self.logger.info(f"Reading table: {table_name}")
            self.dfs[table_name] = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=table_name,
                properties=self.properties
            )
        return self.dfs[table_name]

    def run_category_film_count(self):
        self.logger.info("Task 1: Film count by category")
        fc = self._read_table("film_category")
        c = self._read_table("category")

        df = fc.join(c, "category_id") \
            .groupBy(F.col("name").alias("category_name")) \
            .agg(F.count("film_id").alias("film_count")) \
            .orderBy(F.col("film_count").desc())

        print_pretty(df, title="Film Count by Category")

    def run_top_actors_by_rental(self):
        self.logger.info("Task 2: Top 10 actors by rentals")
        rental = self._read_table("rental")
        inv = self._read_table("inventory")
        fa = self._read_table("film_actor")
        actor = self._read_table("actor")

        df = rental.join(inv, "inventory_id") \
            .join(fa, "film_id") \
            .join(actor, "actor_id") \
            .groupBy("actor_id", "first_name", "last_name") \
            .agg(F.count("rental_id").alias("rental_count")) \
            .orderBy(F.col("rental_count").desc()) \
            .limit(10) \
            .withColumn("actor_name", F.concat_ws(" ", "first_name", "last_name")) \
            .select("actor_name", "rental_count")

        print_pretty(df, title="Top 10 Actors by Rentals")

    def run_category_most_spent(self):
        self.logger.info("Task 3: Category with most money spent (replacement cost)")
        film = self._read_table("film")
        fc = self._read_table("film_category")
        c = self._read_table("category")

        df = film.join(fc, "film_id") \
            .join(c, "category_id") \
            .groupBy("name") \
            .agg(F.sum("replacement_cost").alias("total_replacement_cost")) \
            .orderBy(F.col("total_replacement_cost").desc()) \
            .limit(1)

        print_pretty(df, title="Category with Most Replacement Cost")

    def run_films_not_in_inventory(self):
        self.logger.info("Task 4: Films not in inventory")
        film = self._read_table("film")
        inv = self._read_table("inventory")

        df = film.join(inv, "film_id", "left_anti") \
            .select("film_id", "title") \
            .sort("title")

        print_pretty(df, title="Films Not In Inventory")

    def run_top_actors_children_category(self):
        self.logger.info("Task 5: Top actors in Children category")
        actor = self._read_table("actor")
        fa = self._read_table("film_actor")
        fc = self._read_table("film_category")
        c = self._read_table("category")

        df = actor.join(fa, "actor_id") \
            .join(fc, "film_id") \
            .join(c, "category_id") \
            .filter(F.col("name") == "Children") \
            .groupBy("actor_id", "first_name", "last_name") \
            .agg(F.count("film_id").alias("amount"))

        window = Window.orderBy(F.desc("amount"))

        df_result = df.withColumn("rnk", F.dense_rank().over(window)) \
            .filter(F.col("rnk") <= 3) \
            .select("rnk", "first_name", "last_name", "amount") \
            .orderBy("rnk", F.desc("amount"))

        print_pretty(df_result, title="Top Actors in 'Children' Category")

    def run_city_customer_stats(self):
        self.logger.info("Task 6: Active/Inactive customers by city")
        city = self._read_table("city")
        addr = self._read_table("address")
        cust = self._read_table("customer")

        df = city.join(addr, "city_id").join(cust, "address_id") \
            .groupBy("city") \
            .agg(
            F.sum(F.when(F.col("active") == 0, 1).otherwise(0)).alias("inactive_customers"),
            F.sum(F.when(F.col("active") == 1, 1).otherwise(0)).alias("active_customers")
        ) \
            .orderBy(F.col("inactive_customers").desc())

        print_pretty(df, title="Customer Stats by City")

    def run_complex_rental_analysis(self):
        self.logger.info("Task 7: Complex aggregation (A vs Dash cities)")

        # Подгружаем все нужные таблицы
        tables = ["city", "address", "customer", "rental", "inventory", "film_category", "category"]
        dfs = {t: self._read_table(t) for t in tables}

        df_joined = dfs["city"] \
            .join(dfs["address"], "city_id") \
            .join(dfs["customer"], "address_id") \
            .join(dfs["rental"], "customer_id") \
            .join(dfs["inventory"], "inventory_id") \
            .join(dfs["film_category"], "film_id") \
            .join(dfs["category"], "category_id") \
            .select(
            F.col("name").alias("category_name"),
            F.col("city"),
            F.col("rental_date"),
            F.col("return_date")
        ) \
            .filter(F.col("return_date").isNotNull()) \
            .withColumn("rental_seconds", F.unix_timestamp("return_date") - F.unix_timestamp("rental_date"))

        # Агрегация
        df_agg = df_joined.groupBy("category_name").agg(
            F.sum(F.when(F.lower(F.col("city")).like("a%"), F.col("rental_seconds"))).alias("sec_a"),
            F.sum(F.when(F.col("city").contains("-"), F.col("rental_seconds"))).alias("sec_dash")
        )

        # Конвертация в часы и округление
        df_final = df_agg.select(
            "category_name",
            F.round(F.col("sec_a") / 3600, 2).alias("hours_a"),
            F.round(F.col("sec_dash") / 3600, 2).alias("hours_dash")
        )

        # Вывод результатов отдельно, как требовалось в задаче
        res_a = df_final.orderBy(F.desc_nulls_last("hours_a")).select("category_name", "hours_a").limit(1)
        print_pretty(res_a, title="Most hours in 'A%' cities")

        res_dash = df_final.orderBy(F.desc_nulls_last("hours_dash")).select("category_name", "hours_dash").limit(1)
        print_pretty(res_dash, title="Most hours in '-' cities")