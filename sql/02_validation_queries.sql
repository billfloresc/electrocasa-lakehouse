-- Cambia electrocasa_dev por electrocasa_prod cuando corresponda.

SELECT COUNT(*) AS filas_ventas
FROM electrocasa_dev.silver.ventas;

SELECT *
FROM electrocasa_dev.gold.ventas_sucursal_mes
ORDER BY mes DESC, ventas_total DESC
LIMIT 20;

SELECT *
FROM electrocasa_dev.gold.productos_desempeno
ORDER BY ranking_ventas, producto_id
LIMIT 20;

SELECT *
FROM electrocasa_dev.gold.dotacion_activa_sucursal
ORDER BY sucursal_id;

SELECT *
FROM electrocasa_dev.gold.resenas_negativas_categoria
ORDER BY tasa_resenas_negativas DESC;

SELECT *
FROM electrocasa_dev.audit.cuarentena
ORDER BY fecha_procesamiento DESC
LIMIT 50;

-- Monitoreo del pipeline:
SELECT
  timestamp,
  level,
  event_type,
  message
FROM event_log(TABLE(electrocasa_dev.gold.ventas_sucursal_mes))
ORDER BY timestamp DESC
LIMIT 100;
