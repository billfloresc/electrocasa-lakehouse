# ElectroCasa Lakehouse

Proyecto integrador para centralizar ventas, catálogo, RR.HH., reseñas, devoluciones
y tracking de envíos de ElectroCasa en Databricks.

## Arquitectura

- Landing: archivos en un Unity Catalog Volume y tablas técnicas de ingesta.
- Bronze: datos raw con columnas de auditoría.
- Silver: limpieza, estandarización, deduplicación, expectativas de calidad e
  historización de empleados.
- Gold: métricas de negocio.
- Audit: cuarentena de registros rechazados.
- Azure SQL: acceso al tracking con Lakehouse Federation.
- Orquestación: Lakeflow Job con notebook -> pipeline -> notebook.
- Despliegue: Declarative Automation Bundle con targets dev y prod.

## Métodos de ingesta

### Auto Loader

Se usa en ventas, reseñas y devoluciones porque son fuentes incrementales.
El `schemaLocation` y el `checkpointLocation` quedan bajo el Volume del proyecto,
no en almacenamiento efímero.

### COPY INTO

Se usa para catálogo y empleados. Catálogo es un snapshot de baja frecuencia y
RR.HH. llega por eventos en lote. `COPY INTO` permite reejecutar el mismo archivo
sin duplicarlo por nombre/ruta, por lo que la carga es idempotente.

### Lakehouse Federation

Tracking es una fuente Azure SQL de bajo volumen y consulta bajo demanda. Se crea
una conexión SQL Server y un catálogo federado. Solo se proyectan las seis columnas
necesarias. Usuario y contraseña se guardan en un Secret Scope.

## Calidad observada en los archivos entregados

La exploración previa de los insumos encontró, entre otros, estos casos:

- Ventas: 15,225 filas, 15,000 `venta_id` distintos; métodos de pago con distintos
  formatos, montos nulos/no positivos, fechas en dos formatos y sucursales faltantes.
- Catálogo: 3,030 filas, 3,000 productos distintos; precios con `S/`, precios no
  positivos, marca nula y categorías con casing/acentos distintos.
- Empleados: 4,078 filas para 2,000 `id_empleado`; DNI nulo, DNI asociado a más de
  un ID, eventos repetidos por empleado y fechas faltantes.
- Reseñas: 8,080 filas, 8,000 `resena_id`; calificaciones 0/6/nulas, comentarios
  vacíos, tags nulos y referencias huérfanas.
- Devoluciones: 4,040 filas, 4,000 `devolucion_id`; reembolsos negativos, motivo
  vacío y referencias huérfanas.
- Tracking: el script de origen contiene 5,050 filas para 5,000 `tracking_id` y
  estados/couriers con casing y nombres inconsistentes.

## Reglas de calidad

En Silver se aplican expectativas con política `drop`:

- ventas: `monto_total > 0`
- catálogo: `precio_lista > 0`
- empleados: `dni IS NOT NULL`
- reseñas: `calificacion BETWEEN 1 AND 5`
- devoluciones: `monto_reembolso >= 0`
- tracking: estado dentro de `entregado`, `en_transito`, `pendiente`, `devuelto`

Los registros rechazados se vuelven a identificar desde Bronze y quedan en
`audit.cuarentena` con fuente, motivo, origen, registro serializado y fecha de proceso.

Se eligió `drop` en las tablas Silver porque estas reglas invalidan el dato para el uso
analítico correspondiente. No se pierde trazabilidad porque Bronze conserva el dato raw
y `audit.cuarentena` conserva la evidencia del rechazo.

## Historización de empleados

Se implementa una historización tipo 2 derivada de los eventos de RR.HH. Cada versión
conserva `vigente_desde`, `vigente_hasta` y `es_actual`. `vigente_hasta` representa el inicio de la siguiente versión (límite exclusivo).

Se usa `dni` como clave de negocio una vez validado. Los DNI asociados a más de un
`id_empleado` se consideran ambiguos y no entran al historial; quedan auditables en
cuarentena. También se excluyen del historial los eventos sin fecha.

La fuente solo entrega fecha, no hora ni número de secuencia. Si dos eventos del mismo
empleado ocurren el mismo día, el orden real no puede reconstruirse con certeza. El código
usa un desempate técnico únicamente para que la ejecución sea reproducible; esta limitación
de origen debe mencionarse en la exposición.

## Gold

- `gold.ventas_sucursal_mes`: ventas, cantidad de ventas y ticket promedio por sucursal/mes.
- `gold.productos_desempeno`: unidades vendidas y devoluciones con rankings.
- `gold.dotacion_activa_sucursal`: dotación activa por sucursal.
- `gold.resenas_negativas_categoria`: tasa de reseñas de 1 o 2 estrellas por categoría.
- `gold.estado_envios`: cantidad de envíos por courier y estado.


## Nota importante sobre Free Edition y los grupos

Free Edition no expone account-level APIs ni SCIM. Los grupos compatibles con Unity Catalog
deben crearse desde **Settings > Identity and access > Groups > Manage > Add Group > Add new**
antes de ejecutar `00_setup.ipynb`.

No se usa `CREATE GROUP` por SQL porque ese comando crea grupos locales del workspace,
incompatibles con Unity Catalog. El notebook sí crea los catálogos, schemas, Volume y ejecuta
los `GRANT` sobre los tres account groups.

## Gobierno

Se usan tres account groups:

- `electrocasa_ingenieria`: lectura/escritura de todas las capas.
- `electrocasa_analistas`: solo lectura de Gold.
- `electrocasa_auditoria`: lectura de Gold y del schema Audit.

`dni` y `salario` se protegen con column masks. Solo el grupo de Ingeniería recibe el
valor real.

Las tablas generadas por el pipeline son managed porque su ciclo de vida pertenece al
proyecto. Los archivos raw permanecen en un Volume administrado por Unity Catalog. No se
crea una external table artificial porque el caso no entrega una ubicación cloud externa
que deba tener un ciclo de vida independiente.

## Costos

Free Edition solo usa serverless. Para este caso es coherente porque los lotes son pequeños
y el job se ejecuta una vez al día. El proyecto usa un único pipeline, tres tareas secuenciales
y no mantiene compute encendido continuamente.

## Despliegue

### 1. Prerrequisitos

- Databricks CLI autenticado contra el workspace.
- Secret Scope `electrocasa_sql` con `username` y `password`.
- Conexión y catálogo federado `electrocasa_sql`.
- Haber ejecutado `notebooks/00_setup.ipynb`.
- Haber subido los cinco archivos al Volume del target.

### 2. Validar

```bash
databricks bundle validate -t dev --var="alert_email=TU_CORREO_REAL"
```

### 3. Desplegar dev

```bash
databricks bundle deploy -t dev --var="alert_email=TU_CORREO_REAL"
```

### 4. Ejecutar dev

```bash
databricks bundle run -t dev --var="alert_email=TU_CORREO_REAL" electrocasa_job
```

### 5. Desplegar prod

```bash
databricks bundle validate -t prod --var="alert_email=TU_CORREO_REAL"
databricks bundle deploy -t prod --var="alert_email=TU_CORREO_REAL"
```

En Free Edition, dev queda con programación pausada y prod queda programado diariamente
a las 06:00, zona `America/Lima`.

## Monitoreo

Usar el historial del Job, el grafo/monitor del pipeline y la consulta:

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

El event log sirve como evidencia de progreso, errores y expectativas de calidad.

## Evidencia final

Agregar capturas reales en `docs/evidence/` y referenciarlas desde este README antes de
entregar el URL de GitHub.
