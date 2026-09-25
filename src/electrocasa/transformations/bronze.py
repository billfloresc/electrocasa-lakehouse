from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("electrocasa.catalog")
SQL_FOREIGN_CATALOG = spark.conf.get(
    "electrocasa.sql_foreign_catalog",
    "electrocasa_sql"
)


@dp.table(
    name=f"{CATALOG}.bronze.ventas",
    comment="Ventas raw provenientes de Auto Loader."
)
def bronze_ventas():
    return spark.readStream.table(f"{CATALOG}.landing.ventas_ingest")


@dp.materialized_view(
    name=f"{CATALOG}.bronze.catalogo_productos",
    comment="Snapshot raw del catalogo cargado con COPY INTO."
)
def bronze_catalogo_productos():
    return spark.read.table(f"{CATALOG}.landing.catalogo_ingest")


@dp.materialized_view(
    name=f"{CATALOG}.bronze.empleados",
    comment="Eventos raw de RRHH cargados con COPY INTO."
)
def bronze_empleados():
    return spark.read.table(f"{CATALOG}.landing.empleados_ingest")


@dp.table(
    name=f"{CATALOG}.bronze.resenas",
    comment="Resenas raw semiestructuradas provenientes de Auto Loader."
)
def bronze_resenas():
    return spark.readStream.table(f"{CATALOG}.landing.resenas_ingest")


@dp.table(
    name=f"{CATALOG}.bronze.devoluciones",
    comment="Devoluciones raw provenientes de Auto Loader."
)
def bronze_devoluciones():
    return spark.readStream.table(f"{CATALOG}.landing.devoluciones_ingest")


@dp.materialized_view(
    name=f"{CATALOG}.bronze.tracking_envios",
    comment="Tracking consultado desde Azure SQL mediante Lakehouse Federation."
)
def bronze_tracking_envios():
    return (
        spark.read.table(f"{SQL_FOREIGN_CATALOG}.dbo.TrackingEnvios")
        .select(
            "tracking_id",
            "pedido_id",
            "courier",
            "estado_entrega",
            "sucursal_origen",
            "fecha_actualizacion"
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("Azure SQL: dbo.TrackingEnvios"))
        .withColumn(
            "_batch_id",
            F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
        )
    )
