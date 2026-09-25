-- Las credenciales se leen desde Databricks Secrets.
-- No colocar usuario ni password en texto plano.

CREATE CONNECTION IF NOT EXISTS electrocasa_sql_server
TYPE SQLSERVER
OPTIONS (
  host 'analyticsdmc.database.windows.net',
  port '1433',
  user secret('electrocasa_sql', 'username'),
  password secret('electrocasa_sql', 'password')
);

CREATE FOREIGN CATALOG IF NOT EXISTS electrocasa_sql
USING CONNECTION electrocasa_sql_server
OPTIONS (database 'electrocasadb');

-- Prueba:
SELECT
  tracking_id,
  pedido_id,
  courier,
  estado_entrega,
  sucursal_origen,
  fecha_actualizacion
FROM electrocasa_sql.dbo.TrackingEnvios
LIMIT 10;
