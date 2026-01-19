from asyncio.windows_events import NULL
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
import os
from dotenv import load_dotenv
load_dotenv()

#  Initialization SparkSession
spark = SparkSession.builder \
    .appName("PostgresConnection") \
    .config("spark.driver.extraClassPath", "C:/Users/asus/Downloads/spark/postgresql-42.7.8.jar") \
    .master("local[*]") \
    .getOrCreate()

password = os.getenv("SP_PASSWORD")
user = os.getenv("SP_USER")
url = os.getenv("SP_URL")
properties = {
    "user": user,
    "password": password,
    "url": url

}

def load_table(table, alias):
    return spark.read.format("jdbc")\
            .option("dbtable", table)\
            .options(**properties)\
            .load()\
            .alias(alias)

print("SparkSession успешно создана!")

# 1 query
df_category = load_table('category')
df_film = load_table('film_category')
df_result_1 = df_category.join(df_film, how='left', on=['category_id', 'category_id'])\
            .groupBy(df_category['name'])\
            .agg(count(df_film['film_id']).alias('film_count'))\
            .orderBy('film_count', ascending=False)\
            .show()

# 2 query
df_actor = load_table('actor', 't1')
df_film_actor = load_table('film_actor', 't2')
df_film = load_table('film').alias('t3')
df_inventory = load_table('inventory', 't4')
df_rental = load_table('rental', 't5')
df_result_2 = df_actor.join(df_film_actor, on=['actor_id'], how='inner')\
                        .join(df_film, on=['film_id'], how='inner')\
                        .join(df_inventory, on=['film_id'], how='inner')\
                        .join(df_rental, on=['inventory_id'], how='inner')\
                        .groupBy('t1.first_name', 't1.last_name')\
                        .agg(count('t5.rental_id').alias('most_rent_count'))\
                        .orderBy('most_rent_count', ascending=False)\
                        .limit(10)\
                        .show()

# 3 query
df_category = load_table('category', 't1')
df_film_category = load_table('film_category', 't2')
df_film = load_table('film', 't3')
df_inventory = load_table('inventory', 't4')
df_rental = load_table('rental', 't5')
df_payment = load_table('payment', 't6')

df_result_3 = df_category.join(df_film_category, on='category_id', how='inner')\
                            .join(df_film, on='film_id',how='inner')\
                            .join(df_inventory, on='film_id', how='inner')\
                            .join(df_rental, on='inventory_id', how='inner')\
                            .join(df_payment, on='rental_id', how='inner')\
                            .groupBy('t1.name')\
                            .agg(round(sum(datediff(col('t5.return_date'), col('t5.rental_date')) * col('t6.amount'))).alias('total_money_spent'))\
                            .orderBy('total_money_spent', ascending=False)\
                            .limit(1)\
                            .show()

# 4 query
df_result_4 = df_film.join(df_inventory, on='film_id', how='left')\
                     .filter(df_inventory['film_id'].isNull())\
                     .select('title')\
                     .distinct()\
                     .orderBy('title').show(43)

# 5 query
df_actor = load_table('actor', 't1')
df_film_actor = load_table('film_actor', 't2')
df_film = load_table('film', 't3')
df_film_category = load_table('film_category', 't4')
df_category = load_table('category', 't5')

df_joined = df_actor.join(df_film_actor, on='actor_id', how='left')\
             .join(df_film, on='film_id', how='left')\
             .join(df_film_category, on='film_id', how='left')\
             .join(df_category, on='category_id', how='left')\
             .filter(col("t5.name") == "Children")

df_new = df_joined.groupBy(col('t1.first_name'), col('t1.last_name'))\
                  .agg(count('film_id').alias('count_of_film'),
                         lit('Children').alias('category'))

wind = Window.partitionBy('category').orderBy(col('count_of_film').desc())

df_last = df_new.withColumn('rank', dense_rank().over(wind)).filter(col('rank')<=3)\
                   .select('first_name', 'last_name', 'category', 'count_of_film', 'rank').show()

# 6 query
df_city = load_table('city', 't1')
df_address = load_table('address', 't2')
df_customer = load_table('customer', 't3')

df_joined = df_city.join(df_address, on='city_id', how='inner')\
            .join(df_customer, on='address_id', how='inner')\

df_filter = df_joined.groupBy('city')\
                     .agg(sum(when(col('active')==1, 1).otherwise(0)).alias('active'),
                          sum(when(col('active')==0, 1).otherwise(0)).alias('inactive'))\
                     .orderBy(col('inactive').desc())\
                     .show()

# 7 query

df_city = load_table('city', 't1')
df_address = load_table('address', 't2')
df_customer = load_table('customer', 't3')
df_rental = load_table('rental', 't4')
df_inventory = load_table('inventory', 't5')
df_film = load_table('film', 't6')
df_film_category = load_table('film_category', 't7')
df_category = load_table('category', 't8')

df = df_city.join(df_address, on='city_id', how='inner')\
            .join(df_customer, on='address_id', how='inner')\
            .join(df_rental, on='customer_id', how='inner')\
            .join(df_inventory, on='inventory_id', how='inner')\
            .join(df_film, on='film_id', how='inner')\
            .join(df_film_category, on='film_id', how='inner')\
            .join(df_category, on='category_id', how='inner')

grouped_df = df.groupBy("t1.city", "t8.name") \
    .agg(sum((unix_timestamp("t4.return_date") - unix_timestamp("t4.rental_date")) / 3600).alias("total_rent_time"))\
    .withColumn("group_city",
        when(col("city").like("A%"), "start_a")
         .when(col("city").contains("-"), "have -")
    )
ranked = grouped_df.filter(col("group_city").isNotNull())

wind = Window.partitionBy("group_city").orderBy(col("total_rent_time").desc())

result_df = ranked.withColumn("rank_total_rent", rank().over(wind)) \
    .filter(col("rank_total_rent") == 1) \
    .select(
        col("city"),
        col("name").alias("category"),
        round(col("total_rent_time")).alias("total_rent_time"),
        col("rank_total_rent")
    ) \
    .orderBy(col("total_rent_time").desc())
result_df.show()

input("Нажмите Enter для завершения...")

print("SparkSession остановлена.")
