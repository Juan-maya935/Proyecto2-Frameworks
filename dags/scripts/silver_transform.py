"""Silver: tablas historicas en Iceberg, escritas con MERGE (no override).

Misma logica para Airflow (spark-submit) y para el notebook de respaldo:

    python silver_transform.py users     # -> nessie.silver.users_hist
    python silver_transform.py post      # -> nessie.silver.post_hist

Lee TODOS los anios que haya en Bronze (users/2020 + users/2021, etc.), cura y
normaliza las columnas, agrega fecha_cargue y hace MERGE por Id: si el registro
ya existe lo actualiza y si no lo inserta. Las tablas viven en el bucket silver.

Los nombres de columna son los que deja dlt en Bronze (snake_case):
CreationDate -> creation_date, OwnerUserId -> owner_user_id, etc.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "nessie"
NAMESPACE = "silver"
BRONZE_BASE = os.getenv("SILVER_BRONZE_BASE", "s3a://bronze")
SILVER_BASE = os.getenv("SILVER_SILVER_BASE", "s3a://silver")

USERS_COLUMNS = [
    "id", "reputation", "creation_date", "display_name", "last_access_date",
    "location", "views", "up_votes", "down_votes", "account_id",
]
POST_COLUMNS = [
    "id", "post_type_id", "accepted_answer_id", "creation_date", "score",
    "view_count", "owner_user_id", "last_activity_date", "title", "tags",
    "answer_count", "comment_count", "favorite_count", "parent_id", "closed_date",
]


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


def _binary_to_string(df):
    """El Parquet de origen trae los textos como BINARY (sin anotacion UTF8):
    se convierten a string para poder limpiarlos y castearlos."""
    for field in df.schema.fields:
        if isinstance(field.dataType, T.BinaryType):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
    return df


def _clean_text(name: str):
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _epoch_to_null(name: str):
    """ClickHouse guarda 1970-01-01 donde no hay fecha: se vuelve NULL."""
    return F.when(F.year(F.col(name)) <= 1970, F.lit(None)).otherwise(F.col(name))


def _dedupe(df, key: str, order_col: str):
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.filter(F.col(key).isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def transform_users(df):
    df = df.select(*[c for c in USERS_COLUMNS if c in df.columns])
    df = _binary_to_string(df)
    df = df.withColumn("id", F.col("id").cast("int"))
    if "reputation" in df.columns:
        df = df.withColumn("reputation", F.expr("try_cast(reputation as bigint)"))
    for name in ("display_name", "location"):
        if name in df.columns:
            df = df.withColumn(name, _clean_text(name))
    order_col = "last_access_date" if "last_access_date" in df.columns else "creation_date"
    return _dedupe(df, "id", order_col)


def transform_post(df):
    df = df.select(*[c for c in POST_COLUMNS if c in df.columns])
    df = _binary_to_string(df)
    df = df.withColumn("id", F.col("id").cast("int"))
    if "post_type_id" in df.columns:
        tipo = F.col("post_type_id").cast("string")
        df = df.withColumn(
            "post_type_id",
            F.when(tipo.isin("Question", "1"), F.lit("Question"))
            .when(tipo.isin("Answer", "2"), F.lit("Answer"))
            .otherwise(tipo),
        )
    # 0 / negativos = "sin dueño" o "sin respuesta aceptada"
    for name in ("owner_user_id", "accepted_answer_id"):
        if name in df.columns:
            df = df.withColumn(
                name, F.when(F.col(name) <= 0, F.lit(None)).otherwise(F.col(name)).cast("int")
            )
    if "parent_id" in df.columns:
        df = df.withColumn("parent_id", F.expr("try_cast(parent_id as int)"))
    if "closed_date" in df.columns:
        df = df.withColumn("closed_date", _epoch_to_null("closed_date"))
    if "title" in df.columns:
        df = df.withColumn("title", _clean_text("title"))
    order_col = "last_activity_date" if "last_activity_date" in df.columns else "creation_date"
    return _dedupe(df, "id", order_col)


DATASETS = {
    "users": {
        "source": f"{BRONZE_BASE}/users/*/users_*.parquet",
        "table": "users_hist",
        "transform": transform_users,
    },
    "post": {
        "source": f"{BRONZE_BASE}/post/*/post_*.parquet",
        "table": "post_hist",
        "transform": transform_post,
    },
}


def merge_into_iceberg(spark, df, table_name: str, key: str = "id") -> str:
    """Crea la tabla Iceberg (en el bucket silver) si no existe y hace MERGE por key."""
    full_name = f"{CATALOG}.{NAMESPACE}.{table_name}"
    location = f"{SILVER_BASE}/{table_name}"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    if not spark.catalog.tableExists(full_name):
        (
            df.limit(0).writeTo(full_name).using("iceberg")
            .tableProperty("location", location).create()
        )

    df.createOrReplaceTempView("silver_updates")
    spark.sql(
        f"""
        MERGE INTO {full_name} AS t
        USING silver_updates AS s
        ON t.{key} = s.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return full_name


def main(dataset: str, max_rows: int = None):
    cfg = DATASETS[dataset]
    spark = get_spark(f"silver_{cfg['table']}")

    raw = spark.read.parquet(cfg["source"])
    df = cfg["transform"](raw).withColumn("fecha_cargue", F.current_timestamp())

    # El enunciado permite limitar registros en Silver (util con poca RAM)
    limit_val = max_rows or os.getenv("SILVER_MAX_ROWS")
    if limit_val:
        df = df.limit(int(limit_val))

    full_name = merge_into_iceberg(spark, df, cfg["table"])
    total = spark.table(full_name).count()
    print(f"Silver listo: {full_name} -> {total} filas")
    return spark


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=sorted(DATASETS))
    parser.add_argument("--max-rows", type=int, default=None, help="Limite de filas (poca RAM)")
    args = parser.parse_args()
    main(args.dataset, args.max_rows).stop()