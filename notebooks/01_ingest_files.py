\
# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("catalog", "electrocasa_dev")
CATALOG = dbutils.widgets.get("catalog")

from datetime import datetime, timezone
from pyspark.sql import functions as F

LANDING_VOLUME = f"/Volumes/{CATALOG}/bronze/landing"
BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print(f"Catalogo: {CATALOG}")
print(f"Volume:   {LANDING_VOLUME}")
print(f"Batch:    {BATCH_ID}")

# COMMAND ----------
# Auto Loader para fuentes incrementales.
# El schemaLocation y el checkpoint quedan dentro del Volume.

def cargar_autoloader(nombre, formato, opciones=None):
    opciones = opciones or {}

    origen = f"{LANDING_VOLUME}/{nombre}/"
    schema_location = f"{LANDING_VOLUME}/_schemas/{nombre}/"
    checkpoint = f"{LANDING_VOLUME}/_checkpoints/{nombre}/"
    tabla = f"{CATALOG}.landing.{nombre}_ingest"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
    )

    for clave, valor in opciones.items():
        reader = reader.option(clave, valor)

    df = (
        reader.load(origen)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file")
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    query = (
        df.writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .toTable(tabla)
    )

    query.awaitTermination()
    print(f"OK Auto Loader -> {tabla}")


cargar_autoloader(
    "ventas",
    "csv",
    {"header": "true"}
)

cargar_autoloader(
    "resenas",
    "json",
    {"multiLine": "true"}
)

cargar_autoloader(
    "devoluciones",
    "csv",
    {"header": "true"}
)

# COMMAND ----------
# COPY INTO para snapshot de catalogo y eventos por lote de RRHH.
# COPY INTO es idempotente: un archivo ya cargado se omite al reejecutar.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.landing.catalogo_ingest (
    producto_id STRING,
    nombre_producto STRING,
    categoria STRING,
    marca STRING,
    precio_lista STRING,
    _ingested_at TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
) USING DELTA
""")

spark.sql(f"""
COPY INTO {CATALOG}.landing.catalogo_ingest
FROM (
    SELECT
        producto_id,
        nombre_producto,
        categoria,
        marca,
        CAST(precio_lista AS STRING) AS precio_lista,
        current_timestamp() AS _ingested_at,
        _metadata.file_path AS _source_file,
        '{BATCH_ID}' AS _batch_id
    FROM '{LANDING_VOLUME}/catalogo/'
)
FILEFORMAT = JSON
FORMAT_OPTIONS (
    'multiLine' = 'true'
)
""")

print(f"OK COPY INTO -> {CATALOG}.landing.catalogo_ingest")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.landing.empleados_ingest (
    id_empleado STRING,
    nombre STRING,
    dni STRING,
    email STRING,
    salario DOUBLE,
    sucursal_id STRING,
    cargo STRING,
    tipo_evento STRING,
    fecha_evento STRING,
    _ingested_at TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
) USING DELTA
""")

spark.sql(f"""
COPY INTO {CATALOG}.landing.empleados_ingest
FROM (
    SELECT
        id_empleado,
        nombre,
        CAST(dni AS STRING) AS dni,
        email,
        CAST(salario AS DOUBLE) AS salario,
        sucursal_id,
        cargo,
        tipo_evento,
        fecha_evento,
        current_timestamp() AS _ingested_at,
        _metadata.file_path AS _source_file,
        '{BATCH_ID}' AS _batch_id
    FROM '{LANDING_VOLUME}/empleados/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true'
)
""")

print(f"OK COPY INTO -> {CATALOG}.landing.empleados_ingest")

# COMMAND ----------
# Validacion rapida de las cinco fuentes basadas en archivos.
tablas = [
    "ventas_ingest",
    "catalogo_ingest",
    "empleados_ingest",
    "resenas_ingest",
    "devoluciones_ingest",
]

for tabla in tablas:
    cantidad = spark.table(f"{CATALOG}.landing.{tabla}").count()
    print(f"{tabla}: {cantidad:,} filas")
