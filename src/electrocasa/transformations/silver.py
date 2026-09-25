from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.conf.get("electrocasa.catalog")


def normalizar_texto(columna):
    texto = F.lower(F.trim(columna))
    texto = F.translate(texto, "áéíóú", "aeiou")
    return texto


def ultimo_por_id(df, id_col, order_cols):
    ventana = Window.partitionBy(id_col).orderBy(
        *[F.col(c).desc_nulls_last() for c in order_cols]
    )
    return (
        df.withColumn("_rn", F.row_number().over(ventana))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dp.materialized_view(name=f"{CATALOG}.silver.ventas")
@dp.expect_or_drop("monto_total_positivo", "monto_total > 0")
def silver_ventas():
    df = spark.read.table(f"{CATALOG}.bronze.ventas")

    metodo = normalizar_texto(F.col("metodo_pago"))

    df = (
        df.withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("monto_total", F.col("monto_total").cast("double"))
        .withColumn(
            "fecha_venta",
            F.coalesce(
                F.try_to_timestamp(
                    F.col("fecha_venta"),
                    F.lit("yyyy-MM-dd")
                ).cast("date"),
                F.try_to_timestamp(
                    F.col("fecha_venta"),
                    F.lit("dd/MM/yyyy")
                ).cast("date")
            )
        )
        .withColumn(
            "metodo_pago",
            F.when(
                metodo.isin("tarjeta", "tc", "tarjeta de credito", "tarjeta_credito"),
                F.lit("tarjeta")
            )
            .when(metodo.isin("efectivo", "efv"), F.lit("efectivo"))
            .when(metodo == "yape", F.lit("yape"))
            .when(metodo == "plin", F.lit("plin"))
            .when(
                metodo.isin("transferencia", "transferencia bancaria"),
                F.lit("transferencia")
            )
            .otherwise(metodo)
        )
    )

    return ultimo_por_id(df, "venta_id", ["_ingested_at", "_source_file"])


@dp.materialized_view(name=f"{CATALOG}.silver.catalogo_productos")
@dp.expect_or_drop("precio_lista_positivo", "precio_lista > 0")
def silver_catalogo_productos():
    df = spark.read.table(f"{CATALOG}.bronze.catalogo_productos")

    categoria = normalizar_texto(F.col("categoria"))

    df = (
        df.withColumn(
            "precio_lista",
            F.regexp_replace(
                F.col("precio_lista").cast("string"),
                r"[^0-9.\-]",
                ""
            ).cast("double")
        )
        .withColumn(
            "categoria",
            F.regexp_replace(categoria, r"[\s\-]+", "_")
        )
        .withColumn("marca", F.trim("marca"))
    )

    # El snapshot no trae fecha de actualizacion por producto.
    # Se conserva una fila por producto; Bronze mantiene todos los registros originales.
    return ultimo_por_id(df, "producto_id", ["_ingested_at", "_source_file"])


@dp.materialized_view(name=f"{CATALOG}.silver.empleados_eventos")
@dp.expect_or_drop("dni_no_nulo", "dni IS NOT NULL")
def silver_empleados_eventos():
    df = spark.read.table(f"{CATALOG}.bronze.empleados")

    tipo = normalizar_texto(F.col("tipo_evento"))

    return (
        df.withColumn("dni", F.regexp_replace(F.col("dni").cast("string"), r"\.0$", ""))
        .withColumn("salario", F.col("salario").cast("double"))
        .withColumn("fecha_evento", F.to_date("fecha_evento", "yyyy-MM-dd"))
        .withColumn(
            "tipo_evento",
            F.regexp_replace(tipo, r"[\s\-]+", "_")
        )
        .dropDuplicates(
            [
                "id_empleado",
                "dni",
                "sucursal_id",
                "salario",
                "tipo_evento",
                "fecha_evento"
            ]
        )
    )


@dp.materialized_view(name=f"{CATALOG}.silver.empleados_historial")
def silver_empleados_historial():
    eventos = spark.read.table(f"{CATALOG}.silver.empleados_eventos")

    # Un DNI asociado a mas de un id_empleado es ambiguo.
    dni_ambiguo = (
        eventos.groupBy("dni")
        .agg(F.countDistinct("id_empleado").alias("cantidad_ids"))
        .filter(F.col("cantidad_ids") > 1)
        .select("dni")
    )

    eventos_validos = (
        eventos.join(dni_ambiguo, "dni", "left_anti")
        .filter(F.col("fecha_evento").isNotNull())
    )

    # La fuente solo tiene fecha, no hora/secuencia.
    # id_empleado se usa como desempate tecnico y reproducible.
    ventana = Window.partitionBy("dni").orderBy(
        F.col("fecha_evento").asc(),
        F.col("id_empleado").asc(),
        F.col("tipo_evento").asc()
    )

    siguiente_fecha = F.lead("fecha_evento").over(ventana)

    return (
        eventos_validos
        .withColumn("vigente_desde", F.col("fecha_evento"))
        .withColumn("vigente_hasta", siguiente_fecha)
        .withColumn("es_actual", siguiente_fecha.isNull())
    )


@dp.materialized_view(name=f"{CATALOG}.silver.resenas")
@dp.expect_or_drop(
    "calificacion_valida",
    "calificacion IS NOT NULL AND calificacion BETWEEN 1 AND 5"
)
def silver_resenas():
    df = spark.read.table(f"{CATALOG}.bronze.resenas")

    df = (
        df.withColumn("calificacion", F.col("calificacion").cast("int"))
        .withColumn("fecha_resena", F.to_date("fecha_resena", "yyyy-MM-dd"))
        .withColumn(
            "tags",
            F.when(F.col("tags").isNull(), F.array()).otherwise(F.col("tags"))
        )
    )

    return ultimo_por_id(df, "resena_id", ["_ingested_at", "_source_file"])


@dp.materialized_view(name=f"{CATALOG}.silver.devoluciones")
@dp.expect_or_drop("reembolso_no_negativo", "monto_reembolso >= 0")
def silver_devoluciones():
    df = spark.read.table(f"{CATALOG}.bronze.devoluciones")

    df = (
        df.withColumn("monto_reembolso", F.col("monto_reembolso").cast("double"))
        .withColumn(
            "fecha_devolucion",
            F.to_date("fecha_devolucion", "yyyy-MM-dd")
        )
    )

    return ultimo_por_id(
        df,
        "devolucion_id",
        ["_ingested_at", "_source_file"]
    )


def normalizar_estado_tracking(df):
    estado = normalizar_texto(F.col("estado_entrega"))
    courier = normalizar_texto(F.col("courier"))

    return (
        df.withColumn(
            "estado_entrega",
            F.when(estado == "entregado", F.lit("entregado"))
            .when(
                estado.isin("en camino", "en_camino", "en_transito"),
                F.lit("en_transito")
            )
            .when(estado == "pendiente", F.lit("pendiente"))
            .when(estado == "devuelto", F.lit("devuelto"))
            .otherwise(F.regexp_replace(estado, r"\s+", "_"))
        )
        .withColumn(
            "courier",
            F.initcap(courier)
        )
        .withColumn(
            "fecha_actualizacion",
            F.to_date("fecha_actualizacion")
        )
    )


@dp.materialized_view(name=f"{CATALOG}.silver.tracking_envios")
@dp.expect_or_drop(
    "estado_entrega_valido",
    "estado_entrega IN ('entregado', 'en_transito', 'pendiente', 'devuelto')"
)
def silver_tracking_envios():
    df = spark.read.table(f"{CATALOG}.bronze.tracking_envios")
    df = normalizar_estado_tracking(df)

    return ultimo_por_id(
        df,
        "tracking_id",
        ["fecha_actualizacion", "_ingested_at"]
    )


def fila_cuarentena(df, condicion_valida, fuente, motivo):
    invalida = ~F.coalesce(condicion_valida, F.lit(False))

    return (
        df.filter(invalida)
        .select(
            F.lit(fuente).alias("fuente"),
            F.lit(motivo).alias("motivo_rechazo"),
            F.coalesce(
                F.col("_source_file"),
                F.lit("origen_no_disponible")
            ).alias("origen"),
            F.to_json(
                F.struct(*[F.col(c) for c in df.columns])
            ).alias("registro_json"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )


@dp.materialized_view(
    name=f"{CATALOG}.audit.cuarentena",
    comment="Registros rechazados con fuente, motivo y fecha."
)
def audit_cuarentena():
    ventas = spark.read.table(f"{CATALOG}.bronze.ventas")
    q_ventas = fila_cuarentena(
        ventas,
        F.col("monto_total").cast("double") > 0,
        "ventas",
        "monto_total nulo, cero o negativo"
    )

    catalogo = spark.read.table(f"{CATALOG}.bronze.catalogo_productos")
    precio = F.regexp_replace(
        F.col("precio_lista").cast("string"),
        r"[^0-9.\-]",
        ""
    ).cast("double")
    q_catalogo = fila_cuarentena(
        catalogo,
        precio > 0,
        "catalogo_productos",
        "precio_lista no numerico, nulo, cero o negativo"
    )

    empleados = spark.read.table(f"{CATALOG}.bronze.empleados")
    dni_limpio = F.regexp_replace(F.col("dni").cast("string"), r"\.0$", "")
    q_empleados_dni = fila_cuarentena(
        empleados,
        dni_limpio.isNotNull() & (F.length(F.trim(dni_limpio)) > 0),
        "empleados",
        "dni nulo"
    )
    q_empleados_fecha = fila_cuarentena(
        empleados,
        F.to_date("fecha_evento", "yyyy-MM-dd").isNotNull(),
        "empleados",
        "fecha_evento nula o invalida"
    )

    empleados_con_dni = (
        empleados.withColumn("_dni_limpio", dni_limpio)
        .filter(F.col("_dni_limpio").isNotNull())
    )
    dni_ambiguo = (
        empleados_con_dni.groupBy("_dni_limpio")
        .agg(F.countDistinct("id_empleado").alias("cantidad_ids"))
        .filter(F.col("cantidad_ids") > 1)
        .select("_dni_limpio")
    )
    q_empleados_duplicado = (
        empleados_con_dni
        .join(dni_ambiguo, "_dni_limpio", "inner")
    )
    q_empleados_duplicado = fila_cuarentena(
        q_empleados_duplicado,
        F.lit(False),
        "empleados",
        "dni asociado a mas de un id_empleado"
    )

    resenas = spark.read.table(f"{CATALOG}.bronze.resenas")
    calificacion = F.col("calificacion").cast("int")
    q_resenas = fila_cuarentena(
        resenas,
        calificacion.between(1, 5),
        "resenas",
        "calificacion fuera del rango 1 a 5 o nula"
    )

    devoluciones = spark.read.table(f"{CATALOG}.bronze.devoluciones")
    q_devoluciones = fila_cuarentena(
        devoluciones,
        F.col("monto_reembolso").cast("double") >= 0,
        "devoluciones",
        "monto_reembolso negativo o nulo"
    )

    tracking = normalizar_estado_tracking(
        spark.read.table(f"{CATALOG}.bronze.tracking_envios")
    )
    q_tracking = fila_cuarentena(
        tracking,
        F.col("estado_entrega").isin(
            "entregado",
            "en_transito",
            "pendiente",
            "devuelto"
        ),
        "tracking_envios",
        "estado_entrega fuera del conjunto permitido"
    )

    return (
        q_ventas
        .unionByName(q_catalogo)
        .unionByName(q_empleados_dni)
        .unionByName(q_empleados_fecha)
        .unionByName(q_empleados_duplicado)
        .unionByName(q_resenas)
        .unionByName(q_devoluciones)
        .unionByName(q_tracking)
    )
