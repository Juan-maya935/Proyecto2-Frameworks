# Proyecto 2 – Arquitectura Lakehouse y Pipeline Medallion

**Materia:** Frameworks y Herramientas para Big Data – UAO

Stack: **MinIO** (S3) · **Apache Iceberg** · **Project Nessie** (catálogo) · **Apache Spark** · **dlt** · **Apache Airflow** · **Trino** · **Dremio** · **Jupyter**

---

## Arquitectura

```
 StackOverflow Parquet (HTTP público)
              │  dlt (override)
              ▼
 ┌──────────────── BRONZE ─────────────────┐
 │ s3://bronze/users/<año>/users_<año>.parquet│
 │ s3://bronze/post/<año>/post_<año>.parquet  │
 └─────────────────────────────────────────┘
              │  Spark + Iceberg MERGE INTO (limpieza, dedupe, fecha_cargue)
              ▼
 ┌──────────────── SILVER (Iceberg / Nessie) ┐
 │ nessie.silver.users_hist  (2020 + 2021)   │
 │ nessie.silver.post_hist   (2020 + 2021)   │
 └───────────────────────────────────────────┘
              │  Spark + Iceberg MERGE INTO (JOIN + KPIs + reglas de negocio)
              ▼
 ┌──────────────── GOLD (Iceberg / Nessie) ──┐
 │ nessie.gold.cant_post_x_user_hist         │
 └───────────────────────────────────────────┘
        │                         │
        ▼                         ▼
  Trino + DBeaver             Dremio
  (Silver → réplica Gold)     (consulta Gold)

 Orquestación: Airflow (DAG lakehouse_medallion_pipeline)
 Respaldo manual: notebooks Jupyter (notebooks/Proyecto corte 2)
```

---

## Servicios

| Servicio | URL | Credenciales |
|---|---|---|
| MinIO consola | http://localhost:9001 | admin / password |
| Nessie | http://localhost:19120 | – |
| Airflow | http://localhost:8085 | admin / admin |
| Spark Master | http://localhost:9090 | – |
| Jupyter | http://localhost:8888 | sin token |
| Trino | localhost:8080 | admin |
| Dremio | http://localhost:9047 | se crea al primer ingreso |

> MinIO usa la imagen `pgsty/minio` porque `minio/minio` fue retirada de Docker Hub. Los buckets `bronze`, `silver`, `gold`, `warehouse` y `my-bucket` se crean al arrancar.

---

## 1. Levantar la infraestructura

```bash
docker compose build spark-master          # imagen base de Spark (Jupyter depende de ella)
docker compose up -d --build
docker compose ps                          # todos en running / healthy
```

> El primer arranque tarda varios minutos (descarga de imágenes y `airflow-init`).

---

## 2. Probar cada script por consola (uno por uno)

```bash
# Bronze: dlt -> s3://bronze/users/2021/users_2021.parquet
docker exec -it airflow_scheduler /home/airflow/dlt-venv/bin/python /opt/airflow/dags/scripts/bronze_ingest.py users 2021

# Silver: MERGE en nessie.silver.users_hist  (--max-rows opcional si falta RAM)
docker exec -it airflow_scheduler python /opt/airflow/dags/scripts/silver_transform.py users --max-rows 200000

# Silver post (carga manual de post 2020 + 2021)
docker exec -it airflow_scheduler python /opt/airflow/dags/scripts/silver_transform.py post --max-rows 200000

# Gold: MERGE en nessie.gold.cant_post_x_user_hist
docker exec -it airflow_scheduler python /opt/airflow/dags/scripts/gold_agg.py --max-rows 200000
```

Bronze de los demás años (carga manual inicial): `... bronze_ingest.py users 2020`, `post 2020`, `post 2021`.

---

## 3. Pipeline en Airflow (lo que se presenta)

DAG: [`dags/dag_lakehouse_pipeline.py`](dags/dag_lakehouse_pipeline.py) → **`lakehouse_medallion_pipeline`**

1. Entrar a http://localhost:8085 → activar el DAG → **Trigger (Play)**.
2. Tareas en orden:
   - `1_bronze_ingesta_dlt` – users 2021 con dlt a Bronze (override).
   - `2_silver_users_hist_merge` – limpieza y `MERGE INTO nessie.silver.users_hist`.
   - `3_gold_cant_post_x_user_hist_merge` – JOIN con `post_hist`, KPIs y `MERGE INTO nessie.gold.cant_post_x_user_hist`.
3. Validar en MinIO (buckets `bronze`, `silver`, `gold`) y en Nessie (`http://localhost:19120/api/v2/trees/main/entries`).

**Poca RAM:** agregar `SILVER_MAX_ROWS: "200000"` y `GOLD_MAX_ROWS: "200000"` al `environment` de `airflow-scheduler` en `docker-compose.yml`.

---

## 4. Notebooks de respaldo (Jupyter)

En `notebooks/Proyecto corte 2/` (abrir http://localhost:8888):

| Notebook | Hace lo mismo que |
|---|---|
| `bronze_ingest.ipynb` | tarea Bronze |
| `silver_transform.ipynb` | tarea Silver (`DATASET = "users"` o `"post"`) |
| `gold_agg.ipynb` | tarea Gold |

Traen `MAX_ROWS = 200000` por defecto para no agotar la RAM (`None` = sin límite). Reutilizan las funciones de `dags/scripts/` para que la lógica sea idéntica a Airflow.

---

## 5. Consumo

- **Trino + DBeaver** (réplica de Gold desde Silver): [`consultas/trino_replica_gold.sql`](consultas/trino_replica_gold.sql)
  - DBeaver → Nueva conexión → Trino → host `localhost`, puerto `8080`, usuario `admin`, sin clave.
- **Dremio** (consulta de Gold): [`consultas/dremio_gold.sql`](consultas/dremio_gold.sql) – incluye la configuración de la fuente Nessie.

---

## Estructura

```
├── docker-compose.yml        Dockerfile.airflow / .spark / .jupyter
├── airflow.env               spark-defaults.conf       requirements*.txt
├── .dlt/secrets.toml         (credenciales locales de MinIO para dlt)
├── dags/
│   ├── dag_lakehouse_pipeline.py
│   └── scripts/ bronze_ingest.py · silver_transform.py · gold_agg.py
├── notebooks/Proyecto corte 2/  (respaldo)
├── consultas/  trino_replica_gold.sql · dremio_gold.sql
└── trino/catalog/iceberg.properties
```
