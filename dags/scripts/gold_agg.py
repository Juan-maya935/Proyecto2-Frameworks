"""Gold: cant_post_x_user_hist - metricas por usuario cruzando post_hist y users_hist.

Misma logica para Airflow (spark-submit) y para el notebook de respaldo:

    python gold_agg.py        # -> nessie.gold.cant_post_x_user_hist

Lee las dos tablas historicas de Silver, agrega los posts por usuario (preguntas,
respuestas, puntaje, vistas, comentarios, primera y ultima publicacion), las cruza con
los datos del usuario y aplica una regla de negocio (nivel_actividad). Agrega
fecha_cargue y escribe en Iceberg (bucket gold) con MERGE por user_id.
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CATALOG = "nessie"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"
GOLD_BASE = os.getenv("GOLD_GOLD_BASE", "s3a://gold")
GOLD_TABLE = "cant_post_x_user_hist"


def get_spark(app_name: str) -> SparkSession:
    """Con spark-submit el master lo pone Airflow; en Jupyter corre en modo local.
    El catalogo Nessie y S3/MinIO salen del spark-defaults.conf."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )


def build_gold(users, post):
    """Cruza users_hist con los posts agregados por usuario (LEFT JOIN: tambien
    aparecen los usuarios sin publicaciones, con ceros)."""
    post_cols = set(post.columns)

    aggs = [
        F.count(F.lit(1)).alias("total_posts"),
        F.sum(F.when(F.col("post_type_id") == "Question", 1).otherwise(0)).alias("total_questions"),
        F.sum(F.when(F.col("post_type_id") == "Answer", 1).otherwise(0)).alias("total_answers"),
    ]
    if "score" in post_cols:
        aggs += [F.sum("score").alias("total_score"), F.round(F.avg("score"), 2).alias("avg_score")]
    if "view_count" in post_cols:
        aggs.append(F.sum("view_count").alias("total_views"))
    if "comment_count" in post_cols:
        aggs.append(F.sum("comment_count").alias("total_comments"))
    aggs += [
        F.min("creation_date").alias("first_post_date"),
        F.max("creation_date").alias("last_post_date"),
    ]
    by_user = (
        post.filter(F.col("owner_user_id").isNotNull())
        .groupBy("owner_user_id")
        .agg(*aggs)
    )

    user_cols = [c for c in ("display_name", "reputation", "location") if c in users.columns]
    users_sel = users.select(
        F.col("id").alias("user_id"),
        *user_cols,
        F.col("creation_date").alias("user_creation_date"),
    )

    df = (
        users_sel.join(by_user, users_sel.user_id == by_user.owner_user_id, "left")
        .drop("owner_user_id")
    )

    # Usuarios sin posts: contadores en cero
    counters = [c for c in ("total_posts", "total_questions", "total_answers", "total_score",
                            "avg_score", "total_views", "total_comments") if c in df.columns]
    df = df.na.fill(0, subset=counters)
    for name in ("total_posts", "total_questions", "total_answers"):
        df = df.withColumn(name, F.col(name).cast("long"))

    # Reglas de negocio
    df = df.withColumn(
        "nivel_actividad",
        F.when(F.col("total_posts") == 0, F.lit("Sin actividad"))
        .when(F.col("total_posts") < 5, F.lit("Bajo"))
        .when(F.col("total_posts") < 20, F.lit("Medio"))
        .otherwise(F.lit("Alto")),
    ).withColumn(
        "respuestas_por_pregunta",
        F.when(
            F.col("total_questions") > 0,
            F.round(F.col("total_answers") / F.col("total_questions"), 2),
        ),
    )
    return df


def merge_into_iceberg(spark, df, table_name: str, key: str) -> str:
    """Crea la tabla Iceberg (en el bucket gold) si no existe y hace MERGE por key."""
    full_name = f"{CATALOG}.{GOLD_NAMESPACE}.{table_name}"
    location = f"{GOLD_BASE}/{table_name}"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{GOLD_NAMESPACE}")
    if not spark.catalog.tableExists(full_name):
        (
            df.limit(0).writeTo(full_name).using("iceberg")
            .tableProperty("location", location).create()
        )

    df.createOrReplaceTempView("gold_updates")
    spark.sql(
        f"""
        MERGE INTO {full_name} AS t
        USING gold_updates AS s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return full_name


def main(max_rows: int = None):
    spark = get_spark(f"gold_{GOLD_TABLE}")

    users = spark.table(f"{CATALOG}.{SILVER_NAMESPACE}.users_hist")
    post = spark.table(f"{CATALOG}.{SILVER_NAMESPACE}.post_hist")

    df = build_gold(users, post).withColumn("fecha_cargue", F.current_timestamp())

    # Limite opcional de registros (equipos con poca RAM)
    limit_val = max_rows or os.getenv("GOLD_MAX_ROWS")
    if limit_val:
        df = df.limit(int(limit_val))

    full_name = merge_into_iceberg(spark, df, GOLD_TABLE, key="user_id")
    total = spark.table(full_name).count()
    print(f"Gold listo: {full_name} -> {total} filas")
    return spark


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-rows", type=int, default=None, help="Limite de filas (poca RAM)")
    args = parser.parse_args()
    main(args.max_rows).stop()