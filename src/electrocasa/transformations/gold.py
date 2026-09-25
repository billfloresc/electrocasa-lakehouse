from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("electrocasa.catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.ventas_sucursal_mes")
def gold_ventas_sucursal_mes():
    ventas = spark.read.table(f"{CATALOG}.silver.ventas")

    return (
        ventas
        .filter(F.col("fecha_venta").isNotNull())
        .withColumn("mes", F.date_trunc("month", F.col("fecha_venta")))
        .groupBy("sucursal_id", "mes")
        .agg(
            F.sum("monto_total").alias("ventas_total"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
        .withColumn(
            "ticket_promedio",
            F.round(
                F.col("ventas_total") / F.col("cantidad_ventas"),
                2
            )
        )
    )


@dp.materialized_view(name=f"{CATALOG}.gold.productos_desempeno")
def gold_productos_desempeno():
    ventas = spark.read.table(f"{CATALOG}.silver.ventas")
    devoluciones = spark.read.table(f"{CATALOG}.silver.devoluciones")
    productos = spark.read.table(f"{CATALOG}.silver.catalogo_productos")

    ventas_producto = (
        ventas.groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.sum("monto_total").alias("monto_vendido")
        )
    )

    devoluciones_producto = (
        devoluciones.groupBy("producto_id")
        .agg(
            F.countDistinct("devolucion_id").alias("cantidad_devoluciones"),
            F.sum("monto_reembolso").alias("monto_reembolsado")
        )
    )

    resultado = (
        productos.select(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca"
        )
        .join(ventas_producto, "producto_id", "left")
        .join(devoluciones_producto, "producto_id", "left")
        .fillna(
            {
                "unidades_vendidas": 0,
                "monto_vendido": 0.0,
                "cantidad_devoluciones": 0,
                "monto_reembolsado": 0.0
            }
        )
    )

    ranking_ventas = Window.orderBy(F.col("unidades_vendidas").desc())
    ranking_devol = Window.orderBy(F.col("cantidad_devoluciones").desc())

    return (
        resultado
        .withColumn(
            "ranking_ventas",
            F.dense_rank().over(ranking_ventas)
        )
        .withColumn(
            "ranking_devoluciones",
            F.dense_rank().over(ranking_devol)
        )
    )


@dp.materialized_view(name=f"{CATALOG}.gold.dotacion_activa_sucursal")
def gold_dotacion_activa_sucursal():
    empleados = spark.read.table(f"{CATALOG}.silver.empleados_historial")

    return (
        empleados
        .filter(
            (F.col("es_actual") == True)
            & (F.col("tipo_evento") != "baja")
        )
        .groupBy("sucursal_id")
        .agg(F.countDistinct("dni").alias("empleados_activos"))
    )


@dp.materialized_view(name=f"{CATALOG}.gold.resenas_negativas_categoria")
def gold_resenas_negativas_categoria():
    resenas = spark.read.table(f"{CATALOG}.silver.resenas")
    productos = spark.read.table(f"{CATALOG}.silver.catalogo_productos")

    base = (
        resenas.join(
            productos.select("producto_id", "categoria"),
            "producto_id",
            "inner"
        )
    )

    return (
        base.groupBy("categoria")
        .agg(
            F.count("*").alias("total_resenas"),
            F.sum(
                F.when(F.col("calificacion") <= 2, 1).otherwise(0)
            ).alias("resenas_negativas")
        )
        .withColumn(
            "tasa_resenas_negativas",
            F.round(
                F.col("resenas_negativas") / F.col("total_resenas"),
                4
            )
        )
    )


@dp.materialized_view(name=f"{CATALOG}.gold.estado_envios")
def gold_estado_envios():
    tracking = spark.read.table(f"{CATALOG}.silver.tracking_envios")

    return (
        tracking.groupBy("courier", "estado_entrega")
        .agg(F.countDistinct("tracking_id").alias("cantidad_envios"))
    )
