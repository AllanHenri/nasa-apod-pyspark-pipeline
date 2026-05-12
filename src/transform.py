import json

from pyspark.sql import functions as F

from src.config import RAW_OUTPUT, PROCESSED_OUTPUT, CURATED_OUTPUT
from src.utils.spark_session import create_spark_session


def _safe_first_close_approach(asteroid: dict) -> dict:
    close_approach_data = asteroid.get("close_approach_data") or []
    return close_approach_data[0] if close_approach_data else {}


def _flatten_neows_json(raw_json: dict) -> list[dict]:
    rows: list[dict] = []

    near_earth_objects = raw_json.get("near_earth_objects", {})

    for approach_date, asteroids in near_earth_objects.items():
        for asteroid in asteroids:
            close_approach = _safe_first_close_approach(asteroid)
            relative_velocity = close_approach.get("relative_velocity", {})
            miss_distance = close_approach.get("miss_distance", {})
            estimated_diameter = asteroid.get("estimated_diameter", {})
            estimated_diameter_meters = estimated_diameter.get("meters", {})

            row = {
                "approach_date": approach_date,
                "id": asteroid.get("id"),
                "neo_reference_id": asteroid.get("neo_reference_id"),
                "name": asteroid.get("name"),
                "nasa_jpl_url": asteroid.get("nasa_jpl_url"),
                "absolute_magnitude_h": asteroid.get("absolute_magnitude_h"),
                "diameter_min_meters": estimated_diameter_meters.get("estimated_diameter_min"),
                "diameter_max_meters": estimated_diameter_meters.get("estimated_diameter_max"),
                "is_potentially_hazardous_asteroid": asteroid.get("is_potentially_hazardous_asteroid"),
                "is_sentry_object": asteroid.get("is_sentry_object"),
                "close_approach_date": close_approach.get("close_approach_date"),
                "close_approach_date_full": close_approach.get("close_approach_date_full"),
                "epoch_date_close_approach": close_approach.get("epoch_date_close_approach"),
                "relative_velocity_kph": relative_velocity.get("kilometers_per_hour"),
                "relative_velocity_kps": relative_velocity.get("kilometers_per_second"),
                "miss_distance_km": miss_distance.get("kilometers"),
                "miss_distance_lunar": miss_distance.get("lunar"),
                "orbiting_body": close_approach.get("orbiting_body"),
            }
            rows.append(row)

    return rows


def run_transformation() -> None:
    spark = create_spark_session()

    with open(RAW_OUTPUT, "r", encoding="utf-8") as file:
        raw_json = json.load(file)

    flattened_rows = _flatten_neows_json(raw_json)

    if not flattened_rows:
        raise ValueError("No asteroid records found in raw NeoWs file.")

    df = spark.createDataFrame(flattened_rows)

    final_df = (
        df.withColumn("approach_date", F.to_date("approach_date"))
        .withColumn("close_approach_date", F.to_date("close_approach_date"))
        .withColumn("absolute_magnitude_h", F.col("absolute_magnitude_h").cast("double"))
        .withColumn("diameter_min_meters", F.col("diameter_min_meters").cast("double"))
        .withColumn("diameter_max_meters", F.col("diameter_max_meters").cast("double"))
        .withColumn("relative_velocity_kph", F.col("relative_velocity_kph").cast("double"))
        .withColumn("relative_velocity_kps", F.col("relative_velocity_kps").cast("double"))
        .withColumn("miss_distance_km", F.col("miss_distance_km").cast("double"))
        .withColumn("miss_distance_lunar", F.col("miss_distance_lunar").cast("double"))
        .withColumn(
            "diameter_avg_meters",
            (F.col("diameter_min_meters") + F.col("diameter_max_meters")) / 2,
        )
        .withColumn("year", F.year("approach_date"))
        .withColumn("month", F.month("approach_date"))
    )

    curated_df = (
        final_df.groupBy("year", "month")
        .agg(
            F.count("*").alias("total_asteroids"),
            F.sum(
                F.when(F.col("is_potentially_hazardous_asteroid") == True, 1).otherwise(0)
            ).alias("total_hazardous"),
            F.avg("relative_velocity_kph").alias("avg_velocity_kph"),
            F.avg("miss_distance_km").alias("avg_miss_distance_km"),
            F.avg("diameter_avg_meters").alias("avg_diameter_meters"),
        )
    )

    PROCESSED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    CURATED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    final_df.write.mode("overwrite").parquet(str(PROCESSED_OUTPUT))
    curated_df.write.mode("overwrite").parquet(str(CURATED_OUTPUT))

    print(f"Processed dataset saved to: {PROCESSED_OUTPUT}")
    print(f"Curated dataset saved to: {CURATED_OUTPUT}")

    spark.stop()


if __name__ == "__main__":
    run_transformation()