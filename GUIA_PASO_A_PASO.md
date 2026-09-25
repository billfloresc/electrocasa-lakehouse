
# Guía paso a paso - ElectroCasa en Databricks Free Edition + GitHub

Esta guía está pensada para ejecutar el proyecto sin saltar pasos.

## 0. Qué no debes hacer

- No pongas la contraseña de Azure SQL en un notebook, YAML, README o commit de GitHub.
- No subas `tracking_envios_azure_sql.sql` al Volume como si fuera una fuente del pipeline.
- No uses `CREATE GROUP` por SQL para los grupos de gobierno de Unity Catalog.
- No uses DBFS temporal para checkpoints de Auto Loader.
- No ejecutes dev y prod al mismo tiempo en Free Edition.

## 1. Preparar el repositorio local

Descomprime el proyecto y entra a la carpeta:

```bash
cd electrocasa-lakehouse
```

Comprueba la estructura:

```bash
find . -maxdepth 4 -type f | sort
```

## 2. Instalar Databricks CLI en macOS

```bash
brew install databricks/tap/databricks
databricks version
```

## 3. Autenticar el CLI contra tu workspace

Copia la URL de tu workspace Databricks. Debe verse parecido a:

```text
https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
```

Luego:

```bash
databricks auth login --host TU_URL_DEL_WORKSPACE
```

Se abrirá el navegador para autenticarte.

Comprueba:

```bash
databricks current-user me
```

## 4. Crear el Secret Scope para Azure SQL

Crea el scope:

```bash
databricks secrets create-scope electrocasa_sql
```

Guarda el usuario:

```bash
databricks secrets put-secret electrocasa_sql username
```

Cuando Databricks solicite el valor, ingresa el usuario Azure SQL que te entregaron.

Guarda la contraseña:

```bash
databricks secrets put-secret electrocasa_sql password
```

Cuando solicite el valor, pega la contraseña que te entregaron. No la escribas como
argumento del comando para evitar que quede en el historial del terminal.

Verifica solo los nombres de las claves:

```bash
databricks secrets list-secrets electrocasa_sql
```

Deben aparecer `username` y `password`.

## 5. Crear y probar la conexión a Azure SQL ANTES de continuar

Abre Databricks > SQL Editor y ejecuta el archivo:

`sql/01_create_azure_sql_connection.sql`

Ese script crea:

- conexión: `electrocasa_sql_server`
- foreign catalog: `electrocasa_sql`

y luego consulta únicamente las columnas necesarias de:

`electrocasa_sql.dbo.TrackingEnvios`

La prueba debe devolver filas.

Si falla por conexión de red, firewall o timeout, detente aquí. No reemplaces la fuente por
el archivo `.sql` para "hacerla pasar": el proyecto pide consumir Azure SQL.

## 6. Crear los tres account groups

En Databricks:

1. Abre tu usuario arriba a la derecha.
2. Entra a **Settings**.
3. Entra a **Identity and access**.
4. En **Groups**, pulsa **Manage**.
5. Pulsa **Add Group**.
6. Pulsa **Add new**.
7. Crea exactamente:

```text
electrocasa_ingenieria
electrocasa_analistas
electrocasa_auditoria
```

8. Agrega tu propio usuario a `electrocasa_ingenieria`.

Si en Free Edition no aparece la opción **Add new**, no uses `CREATE GROUP`.
Ese comando generaría grupos locales del workspace que Unity Catalog no acepta.
En ese caso la parte de gobierno que exige tres grupos no puede reproducirse literalmente
en ese workspace y tendrás que usar un workspace que permita account groups.

## 7. Importar y ejecutar 00_setup.ipynb

En Databricks importa:

`notebooks/00_setup.ipynb`

Ejecuta todas las celdas.

El notebook crea:

```text
electrocasa_dev
  landing
  bronze
  silver
  gold
  audit

electrocasa_prod
  landing
  bronze
  silver
  gold
  audit
```

y el Volume:

```text
/Volumes/electrocasa_dev/bronze/landing
/Volumes/electrocasa_prod/bronze/landing
```

Además aplica los GRANT de los tres grupos.

## 8. Crear las carpetas del landing de DEV

En Catalog Explorer entra a:

`electrocasa_dev > bronze > Volumes > landing`

Crea estas carpetas:

```text
ventas
catalogo
empleados
resenas
devoluciones
```

También puedes crearlas desde un notebook:

```python
base = "/Volumes/electrocasa_dev/bronze/landing"

for carpeta in ["ventas", "catalogo", "empleados", "resenas", "devoluciones"]:
    dbutils.fs.mkdirs(f"{base}/{carpeta}")
```

## 9. Subir los cinco archivos a DEV

Carga exactamente así:

```text
ventas_sucursales.csv
→ /Volumes/electrocasa_dev/bronze/landing/ventas/

catalogo_productos.json
→ /Volumes/electrocasa_dev/bronze/landing/catalogo/

empleados_rrhh.csv
→ /Volumes/electrocasa_dev/bronze/landing/empleados/

resenas_clientes.json
→ /Volumes/electrocasa_dev/bronze/landing/resenas/

devoluciones.csv
→ /Volumes/electrocasa_dev/bronze/landing/devoluciones/
```

No subas `tracking_envios_azure_sql.sql`.

## 10. Crear el repositorio GitHub

En GitHub crea un repositorio vacío llamado:

```text
electrocasa-lakehouse
```

No marques README, `.gitignore` ni licencia porque ya existen localmente.

En terminal, desde la raíz del proyecto:

```bash
git init
git branch -M main
git add .
git status
git commit -m "Proyecto integrador ElectroCasa"
git remote add origin URL_DE_TU_REPOSITORIO
git push -u origin main
```

Revisa en GitHub que NO aparezca ninguna contraseña.

## 10.1 Confirmar que el perfil DEFAULT apunta al workspace

El bundle usa el perfil `DEFAULT` del Databricks CLI. Comprueba:

```bash
databricks auth profiles
databricks auth describe
```

Si tu autenticación quedó guardada con otro nombre, conviértela en predeterminada:

```bash
databricks auth switch
```

En `prod` se usa una ruta de despliegue no ligada a un usuario:

```text
/Workspace/Production/.bundle/electrocasa-lakehouse/prod
```

Esto evita que el target de producción dependa de la carpeta personal del usuario.

## 11. Validar el bundle DEV

Desde la raíz del repositorio:

```bash
databricks bundle validate -t dev --var="alert_email=TU_CORREO_REAL"
```

No continúes si sale error.

Guarda una captura de la validación correcta.

## 12. Desplegar DEV

```bash
databricks bundle deploy -t dev --var="alert_email=TU_CORREO_REAL"
```

Luego revisa en Databricks:

- Jobs & Pipelines
- pipeline `electrocasa_pipeline_dev`
- job `electrocasa_job_dev`

## 13. Ejecutar DEV

```bash
databricks bundle run -t dev --var="alert_email=TU_CORREO_REAL" electrocasa_job
```

El job ejecuta en orden:

```text
1. ingestar_archivos
2. ejecutar_pipeline
3. aplicar_gobierno
```

Los tipos de tarea son notebook + pipeline.

## 14. Qué hace la ingesta

### Auto Loader

Se usa para:

- ventas
- reseñas
- devoluciones

porque son fuentes incrementales.

Los checkpoints y schemas inferidos quedan en:

```text
/Volumes/electrocasa_dev/bronze/landing/_checkpoints/
/Volumes/electrocasa_dev/bronze/landing/_schemas/
```

### COPY INTO

Se usa para:

- catálogo
- empleados

porque catálogo es snapshot de baja frecuencia y RR.HH. llega por lotes.

Reejecutar el mismo archivo no debe duplicarlo.

### Lakehouse Federation

Se usa para tracking porque es una tabla Azure SQL de bajo volumen y consulta bajo demanda.

## 15. Validar Bronze

Ejecuta:

```sql
SELECT COUNT(*) FROM electrocasa_dev.bronze.ventas;
SELECT COUNT(*) FROM electrocasa_dev.bronze.catalogo_productos;
SELECT COUNT(*) FROM electrocasa_dev.bronze.empleados;
SELECT COUNT(*) FROM electrocasa_dev.bronze.resenas;
SELECT COUNT(*) FROM electrocasa_dev.bronze.devoluciones;
SELECT COUNT(*) FROM electrocasa_dev.bronze.tracking_envios;
```

Revisa además las columnas técnicas:

```sql
SELECT _ingested_at, _source_file, _batch_id
FROM electrocasa_dev.bronze.ventas
LIMIT 10;
```

## 16. Validar Silver

```sql
SELECT * FROM electrocasa_dev.silver.ventas LIMIT 20;
SELECT * FROM electrocasa_dev.silver.catalogo_productos LIMIT 20;
SELECT * FROM electrocasa_dev.silver.empleados_historial LIMIT 20;
SELECT * FROM electrocasa_dev.silver.resenas LIMIT 20;
SELECT * FROM electrocasa_dev.silver.devoluciones LIMIT 20;
SELECT * FROM electrocasa_dev.silver.tracking_envios LIMIT 20;
```

Comprueba:

- ventas con `monto_total > 0`
- fechas de ventas normalizadas
- métodos de pago normalizados
- precio de catálogo numérico y positivo
- categorías normalizadas
- DNI no nulo en eventos válidos
- historial de empleados con `vigente_desde`, `vigente_hasta`, `es_actual` (`vigente_hasta` es límite exclusivo)
- reseñas entre 1 y 5
- reembolsos no negativos
- tracking con estados estandarizados

## 17. Validar cuarentena

```sql
SELECT
  fuente,
  motivo_rechazo,
  COUNT(*) AS cantidad
FROM electrocasa_dev.audit.cuarentena
GROUP BY fuente, motivo_rechazo
ORDER BY fuente, cantidad DESC;
```

Luego:

```sql
SELECT *
FROM electrocasa_dev.audit.cuarentena
LIMIT 50;
```

La cuarentena debe permitir saber:

- qué registro se rechazó
- de qué fuente vino
- por qué se rechazó
- cuándo se procesó

## 18. Validar Gold

Ejecuta `sql/02_validation_queries.sql`.

Las tablas Gold principales son:

```text
gold.ventas_sucursal_mes
gold.productos_desempeno
gold.dotacion_activa_sucursal
gold.resenas_negativas_categoria
gold.estado_envios
```

## 19. Validar el masking

El job ejecuta `notebooks/02_apply_governance.py` al final.

Comprueba que las funciones existan:

```sql
SHOW FUNCTIONS IN electrocasa_dev.silver;
```

Y describe la vista:

```sql
DESCRIBE EXTENDED electrocasa_dev.silver.empleados_historial;
```

`dni` y `salario` tienen column masks.

La lógica permite valores reales únicamente a miembros de:

```text
electrocasa_ingenieria
```

## 20. Validar permisos

Como mínimo, conserva evidencia de los GRANT ejecutados por `00_setup.ipynb`.

El diseño es:

```text
Ingeniería:
  lectura/escritura de landing, bronze, silver, gold y audit

Analistas:
  solo lectura de gold

Auditoría:
  solo lectura de gold y audit + BROWSE del catálogo
```

## 21. Monitorear el pipeline

Puedes usar el historial visual de Jobs & Pipelines y también:

```sql
SELECT
  timestamp,
  level,
  event_type,
  message
FROM event_log(TABLE(electrocasa_dev.gold.ventas_sucursal_mes))
ORDER BY timestamp DESC
LIMIT 100;
```

Guarda capturas del grafo del pipeline, el job correcto y el event log.

## 22. Preparar PROD

Repite solamente el landing para:

```text
/Volumes/electrocasa_prod/bronze/landing/
```

Sube los mismos cinco archivos a las mismas cinco subcarpetas.

## 23. Validar y desplegar PROD

```bash
databricks bundle validate -t prod --var="alert_email=TU_CORREO_REAL"
databricks bundle deploy -t prod --var="alert_email=TU_CORREO_REAL"
```

Para la evidencia puedes ejecutar manualmente:

```bash
databricks bundle run -t prod --var="alert_email=TU_CORREO_REAL" electrocasa_job
```

En Free Edition evita ejecutar dev y prod simultáneamente.

## 24. Capturas finales

Guarda como mínimo:

```text
01_azure_sql_connection.png
02_setup_catalogos_grupos.png
03_pipeline_dev_success.png
04_job_dev_success.png
05_bundle_validate_dev.png
06_bundle_deploy_dev.png
07_bundle_deploy_prod.png
08_pipeline_graph.png
09_event_log.png
10_masking_ingenieria.png
11_permisos_analistas_auditoria.png
```

Colócalas en:

```text
docs/evidence/
```

y haz otro commit:

```bash
git add .
git commit -m "Agregar evidencias de ejecucion"
git push
```

## 25. Verificación antes de entregar

Confirma:

```text
[ ] GitHub público/privado accesible según indique el docente
[ ] No hay contraseña en GitHub
[ ] Se integraron las 6 fuentes
[ ] Se usaron al menos 2 métodos de ingesta
[ ] Auto Loader usa checkpoint/schema bajo Volume
[ ] COPY INTO es idempotente
[ ] Bronze/Silver/Gold están en Lakeflow Declarative Pipelines
[ ] Existe al menos una expectativa con política explícita
[ ] Existe cuarentena consultable
[ ] Empleados mantiene historial
[ ] Job tiene dependencias, reintentos y alertas
[ ] Bundle tiene dev y prod
[ ] Hay evidencia de validate/deploy/run
[ ] Hay grupos y GRANT diferenciados
[ ] DNI/salario están protegidos
[ ] README documenta arquitectura, despliegue, calidad, costos y evidencias
```
