\
# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("catalog", "electrocasa_dev")
CATALOG = dbutils.widgets.get("catalog")

GRUPO_ING = "electrocasa_ingenieria"

print(f"Aplicando masking en {CATALOG}")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_dni(valor STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('{GRUPO_ING}') THEN valor
    WHEN valor IS NULL THEN NULL
    ELSE concat('****', right(valor, 4))
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.silver.mask_salario(valor DOUBLE)
RETURNS DOUBLE
RETURN CASE
    WHEN is_account_group_member('{GRUPO_ING}') THEN valor
    ELSE NULL
END
""")

# COMMAND ----------
# Los dos objetos son materialized views creados por Lakeflow.
spark.sql(f"""
ALTER MATERIALIZED VIEW {CATALOG}.silver.empleados_historial
ALTER COLUMN dni
SET MASK {CATALOG}.silver.mask_dni
""")

spark.sql(f"""
ALTER MATERIALIZED VIEW {CATALOG}.silver.empleados_historial
ALTER COLUMN salario
SET MASK {CATALOG}.silver.mask_salario
""")

print("Masking aplicado.")

# COMMAND ----------
# Verificacion. Si tu usuario pertenece a electrocasa_ingenieria
# debe ver DNI y salario reales; otros usuarios con acceso veran los valores protegidos.
display(
    spark.sql(f"""
        SELECT
            id_empleado,
            dni,
            salario,
            sucursal_id,
            tipo_evento,
            es_actual
        FROM {CATALOG}.silver.empleados_historial
        LIMIT 20
    """)
)
