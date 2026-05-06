from pyspark.sql import functions as F

from src.config import RAW_OUTPUT, PROCESSED_OUTPUT, CURATED_OUTPUT
from src.utils.spark_session import create_spark_session

def run_transformation() -> None:
    spark = create_spark_session()

    df = spark.read.option("multiline", "true").json(str(RAW_OUTPUT))

    neo_map_df = df.select(
        F.explode("near_earth_objects").alias("approach_date", "asteroids")
    )

    exploded_dates_df = neo_map_df.select(
        F.explode("near_earth_objects").alias("approach_date", "asteroids")
    )

    exploded_asteroids_df = exploded_dates_df.select(
        "approach_date",
        F.explode("asteroids").alias("asteroid")
    )
    
    flattened_df = exploded_asteroids_df.select(
        F.col("approach_date"),
        F.col("asteroid.id").alias("id"),
        F.col("asteroid.neo_reference_id").alias("neo_reference_id"),
        F.col("asteroid.name").alias("name"),
        F.col("asteroid.nasa_jpl_url").alias("nasa_jpl_url"),
        F.col("asteroid.absolute_magnitude_h").alias("absolute_magnitude_h"),
        F.col("asteroid.estimated_diameter.meters.estimated_diameter_min").alias("diameter_min_meters"),
        F.col("asteroid.estimated_diameter.meters.estimated_diameter_max").alias("diameter_max_meters"),
        F.col("asteroid.is_potentially_hazardous_asteroid").alias("is_potentially_hazardous_asteroid"),
        F.col("asteroid.is_sentry_object").alias("is_sentry_object"),
        F.col("asteroid.close_approach_data")[0]["close_approach_date"].alias("close_approach_date"),
        F.col("asteroid.close_approach_data")[0]["close_approach_date_full"].alias("close_approach_date_full"),
        F.col("asteroid.close_approach_data")[0]["epoch_date_close_approach"].alias("epoch_date_close_approach"),
        F.col("asteroid.close_approach_data")[0]["relative_velocity"]["kilometers_per_hour"].cast("double").alias("relative_velocity_kph"),
        F.col("asteroid.close_approach_data")[0]["relative_velocity"]["kilometers_per_second"].cast("double").alias("relative_velocity_kps"),
        F.col("asteroid.close_approach_data")[0]["miss_distance"]["kilometers"].cast("double").alias("miss_distance_km"),
        F.col("asteroid.close_approach_data")[0]["miss_distance"]["lunar"].cast("double").alias("miss_distance_lunar"),
        F.col("asteroid.close_approach_data")[0]["orbiting_body"].alias("orbiting_body")
    )

    final_df = (
        flattened_df
        .withColumn("approach_date", F.to_date("approach_date"))
        .withColumn("close_approach_date", F.to_date("close_approach_date"))
        .withColumn("year", F.year("approach_date"))
        .withColumn("year", F.month("approach_date"))
    )
    final_df.show(truncate=False)
    final_df.printSchema()

    final_df.write.mode("overwrite").parquet("data/processed/neo_flattened")