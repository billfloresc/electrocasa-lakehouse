# Datos entregados

Esta carpeta conserva una copia de los insumos originales del proyecto.

Archivos que sí se cargan al Unity Catalog Volume:

- `ventas_sucursales.csv`
- `catalogo_productos.json`
- `empleados_rrhh.csv`
- `resenas_clientes.json`
- `devoluciones.csv`

`tracking_envios_azure_sql.sql` se conserva solamente como referencia del dataset de
tracking. **No se carga al Volume**: el pipeline consulta la tabla ya poblada en Azure SQL.

Los archivos son datos sintéticos del ejercicio.
