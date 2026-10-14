"""Bronze: ingesta con dlt -> Parquet en el bucket `bronze` (con override).

Misma logica para Airflow y para el notebook de respaldo:

    python bronze_ingest.py users 2021
    python bronze_ingest.py users 2020
    python bronze_ingest.py post 2020
    python bronze_ingest.py post 2021

Resultado (rutas del enunciado):
    bronze/users/2021/users_2021.parquet
    bronze/post/2020/post_2020.parquet

dlt escribe primero en una carpeta temporal del bucket (dlt_staging/) y
luego el archivo se mueve a su ruta final. Si ya existia, se sobrescribe.
La configuracion del destino (bucket_url y credenciales) sale del
secrets.toml / variables de entorno de dlt, igual que en el corte 1.
"""
import argparse
import os
import posixpath
import time

import aiohttp
import dlt
import fsspec
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

BASE_URL = os.getenv(
    "BRONZE_SOURCE_BASE",
    "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet",
)
PIPELINE_NAME = "bronze_ingest"
STAGING_DATASET = "dlt_staging"
BATCH_SIZE = 50_000

# Descarga robusta: sin limite total por peticion (aiohttp trae 5 min por defecto),
# solo se corta si pasan 10 min sin recibir ningun byte; y cada grupo de filas
# se reintenta si la conexion falla.
READ_TIMEOUT_SECONDS = int(os.getenv("BRONZE_READ_TIMEOUT", "600"))
RETRIES = int(os.getenv("BRONZE_RETRIES", "5"))

# Columnas pesadas que no se llevan a Bronze (ahorra RAM y disco)
EXCLUDE_COLUMNS = {"post": ["Body"]}


def source_url(dataset: str, year: int) -> str:
    if dataset == "users":
        return f"{BASE_URL}/users.parquet"      # un solo archivo con todos los usuarios
    if dataset == "post":
        return f"{BASE_URL}/posts/{year}.parquet"  # un archivo por anio
    raise ValueError(f"dataset desconocido: {dataset}")


def _open_fs(url: str):
    """Sistema de archivos fsspec con timeouts largos para HTTP."""
    if url.startswith("http"):
        timeout = aiohttp.ClientTimeout(
            total=None, sock_connect=60, sock_read=READ_TIMEOUT_SECONDS
        )
        return fsspec.core.url_to_fs(url, client_kwargs={"timeout": timeout})
    return fsspec.core.url_to_fs(url)


def _read_row_group(fs, path: str, index: int, columns):
    """Lee un grupo de filas; si la conexion falla, reabre el archivo y reintenta."""
    for intento in range(1, RETRIES + 1):
        try:
            with fs.open(path, "rb") as f:
                return pq.ParquetFile(f).read_row_group(index, columns=columns)
        except Exception as exc:  # noqa: BLE001
            if intento == RETRIES:
                raise
            espera = 10 * intento
            print(f"  fallo leyendo el grupo {index} ({type(exc).__name__}); "
                  f"reintento {intento}/{RETRIES - 1} en {espera}s", flush=True)
            time.sleep(espera)


def read_batches(dataset: str, year: int):
    """Lee el Parquet remoto grupo de filas por grupo de filas (poca RAM).

    Para users se queda con el anio de CreationDate. Imprime el avance.
    """
    fs, path = _open_fs(source_url(dataset, year))
    with fs.open(path, "rb") as f:
        pf = pq.ParquetFile(f)
        total_groups = pf.num_row_groups
        excluded = EXCLUDE_COLUMNS.get(dataset, [])
        columns = [c for c in pf.schema_arrow.names if c not in excluded]

    kept = 0
    for i in range(total_groups):
        table = _read_row_group(fs, path, i, columns)
        if dataset == "users":
            table = table.filter(pc.equal(pc.year(table["CreationDate"]), year))
        kept += table.num_rows
        print(f"[{dataset} {year}] grupo {i + 1}/{total_groups} - filas guardadas: {kept}",
              flush=True)
        for batch in table.to_batches(max_chunksize=BATCH_SIZE):
            if batch.num_rows:
                yield pa.Table.from_batches([batch])


def move_to_final_path(pipeline, dataset: str, year: int) -> str:
    """Mueve el Parquet que dejo dlt a bronze/<dataset>/<anio>/<dataset>_<anio>.parquet."""
    client = pipeline.destination_client()
    fs = client.fs_client
    staged = sorted(fs.glob(posixpath.join(client.dataset_path, dataset, "*.parquet")))
    if not staged:
        raise RuntimeError("dlt no dejo ningun Parquet en la carpeta temporal")

    final_dir = posixpath.join(client.bucket_path, dataset, str(year))
    final = posixpath.join(final_dir, f"{dataset}_{year}.parquet")
    if "file" in fs.protocol:
        fs.makedirs(final_dir, exist_ok=True)
    if fs.exists(final):
        fs.rm(final)  # override

    if len(staged) == 1:
        fs.mv(staged[0], final)
    else:  # por si dlt partio la carga en varios archivos
        writer = None
        with fs.open(final, "wb") as out:
            for path in staged:
                with fs.open(path, "rb") as fh:
                    for batch in pq.ParquetFile(fh).iter_batches(batch_size=BATCH_SIZE):
                        if writer is None:
                            writer = pq.ParquetWriter(out, batch.schema)
                        writer.write_batch(batch)
            if writer is not None:
                writer.close()
        for path in staged:
            fs.rm(path)
    return final


def main(dataset: str, year: int) -> str:
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination="filesystem",
        dataset_name=STAGING_DATASET,
    )
    resource = dlt.resource(
        read_batches(dataset, year), name=dataset, write_disposition="replace"
    )
    info = pipeline.run(resource, loader_file_format="parquet")
    print(info)
    final = move_to_final_path(pipeline, dataset, year)
    print(f"Bronze listo: {final}")
    return final


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", choices=["users", "post"])
    parser.add_argument("year", type=int)
    args = parser.parse_args()
    main(args.dataset, args.year)