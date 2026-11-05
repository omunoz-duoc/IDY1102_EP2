# DataRetail 360 — pipeline reproducible

**IDY1102 · Oscar Muñoz**

Pipeline analítico empaquetado (`dataretail360`) que extrae **14 tablas de PostgreSQL,
un CSV y un Excel**, las procesa por capas Bronze → Silver → Gold, publica un DWH
dimensional (SCD 1/2/3 + copo de nieve) y **4 data marts dependientes** en SQL Server
validados con dbt, y deja un reporte, un manifiesto y logs JSONL de cada corrida.

Un clon limpio produce el mismo resultado en cualquier máquina x86-64 con Docker:
no requiere Python, ODBC, PostgreSQL, SQL Server ni dbt instalados en el host.

## Inicio rápido

Requisitos: Git, Docker Engine/Desktop con **contenedores Linux**, Compose v2, ~6 GB
de RAM asignados a Docker e Internet para la primera construcción.

```sh
git clone https://github.com/omunoz-duoc/IDY1102_EP2.git
cd IDY1102_EP2
docker compose up --build pipeline
```

Compose levanta PostgreSQL 17 y SQL Server 2022, ejecuta `seed` (dataset demo
sintético) y, cuando termina con éxito, corre el pipeline completo. La corrida
termina con el evento `run_completed` y `pipeline-1 exited with code 0`.

Volver a ejecutar solo el pipeline (las fuentes y la historia SCD se conservan):

```sh
docker compose run --rm pipeline run
```

## Fuentes de entrada

El pipeline necesita su **contrato de entrada** completo; para verlo:

```sh
docker compose run --rm pipeline seed contract
```

| Origen | Contenido |
|---|---|
| PostgreSQL `dr360_raw` | 14 tablas: POS, e-commerce, CRM, inventario, devoluciones, marketing, sesiones web |
| PostgreSQL `dr360_control` | `config_generacion`, `definiciones_kpi_por_area`, `problemas_calidad_inyectados` |
| `data/raw/marketing/campanias.csv` | Campañas de marketing |
| `data/raw/legacy/reportes_ventas.xlsx` | Hoja `legacy_reporte_ventas_xlsx_2026` (reportes manuales) |

El servicio `seed` prepara esas fuentes según `DATARETAIL_SEED`:

| Modo | Efecto |
|---|---|
| `demo` (defecto) | Siembra el dataset sintético determinista. Idempotente: si ya existe la misma versión demo no hace nada, y si el origen tiene datos no demo los conserva. |
| `none` | No toca las fuentes; solo crea la base destino de SQL Server si falta. |

El dataset demo contiene 3 sucursales, 6 productos, 8 identidades ficticias,
64 ventas entre enero y septiembre de 2026, devoluciones, inventario, campañas,
sesiones y envíos. Incluye a propósito problemas de calidad (compras anónimas,
reembolso posterior al corte, envíos sin consentimiento, fecha Excel ambigua)
para ejercitar las reglas. Los correos usan `example.invalid`.

### Paquetes de semilla (datos reales o congelados)

Para mover a otra máquina un conjunto de fuentes concreto, no solo la demo, se
congela en un **paquete portable**: Parquet por tabla, el DDL de columnas, copias
del CSV y el Excel, y un `manifest.json` con SHA256.

```sh
# 1. Exportar desde un stack con las fuentes cargadas (carpeta vacía y escribible por uid 1000)
mkdir -p bundle && chmod 777 bundle
docker compose run --rm -v "$PWD/bundle:/bundle" pipeline seed export /bundle

# 2. En la máquina destino: verificar, cargar y ejecutar sin sembrar la demo
export DATARETAIL_SEED=none
docker compose run --rm -v "$PWD/bundle:/bundle" seed seed verify /bundle
docker compose run --rm -v "$PWD/bundle:/bundle" seed seed load /bundle
docker compose up pipeline
```

`seed load` rechaza un origen que ya tenga datos, salvo con `--replace`. **`--replace`
(en `load` y en `demo`) elimina las tablas fuente existentes**: usar solo con bases
dedicadas. Los paquetes pueden contener datos personales; no versionarlos en Git.

## Resultados

Cada corrida escribe en el workspace (`/workspace` en Docker; `DATARETAIL_HOME` en nativo):

| Ruta | Contenido |
|---|---|
| `data/bronze/`, `data/silver/` | Instantánea de origen y datos integrados (Parquet) |
| `data/gold/` | DWH y data marts publicados (Parquet), diccionarios de datos |
| `data/control/`, `data/rechazos/` | Auditoría de calidad, rechazos, plan de consulta |
| `artifacts/reports/<run_id>/report.md` | Reporte de la corrida: tablas, gráficos, controles y conclusiones |
| `artifacts/dbt/<publicación>/` | Evidencia dbt (manifest, run_results) |
| `logs/<run_id>.jsonl` | Eventos estructurados de la corrida |

En SQL Server (`DataRetail360_Demo`) quedan el esquema `dw` (12 dimensiones y los
hechos) y los marts `dm_comercial.ventas_mensuales`, `dm_operaciones.inventario_actual`,
`dm_marketing.campanias_resumen` y `dm_clientes.clientes_mensuales`.

Por defecto el workspace es un volumen Docker. Para tenerlo como carpeta del host,
crear `.env` con `DATARETAIL_WORKSPACE=./workspace` y una carpeta `workspace/`
escribible por uid 1000 antes de iniciar. Si no, copiar desde el contenedor:

```sh
docker compose cp pipeline:/workspace/artifacts ./artifacts-out
```

## Cómo funciona

1. **Bronze**: extracción de solo lectura con orden determinista y SHA256 de archivos.
2. **Silver**: normalización (RUT, regiones, códigos, fechas Excel), resolución de
   productos e identidades, reglas documentales y auditoría de calidad con rechazos.
3. **Dimensiones**: SCD 1/2/3 con historia leída del DWH y jerarquías snowflake de
   producto y sucursal (12 tablas físicas).
4. **Hechos**: ventas, devoluciones, inventario, reposición, campañas y sesiones.
5. **Publicación Write-Audit-Publish**: carga a esquemas candidatos, dbt construye
   5 modelos y debe aprobar ≥ 33 pruebas; la promoción del DWH y los 4 marts es una
   única transacción bajo `sp_getapplock`.
6. **Gold, análisis y verificación**: exportaciones, consultas de negocio, diccionarios,
   segunda publicación, prueba adversa de rollback (debe rechazarse) y benchmark.

La corrida completa verifica 35 controles internos y 58 pruebas dbt por publicación.
`run --skip-verify` omite la segunda publicación, las sondas y el benchmark.

## Comandos

```text
dataretail360 doctor                  Verificar rutas, dbt y conexiones (sin modificar datos)
dataretail360 run [--run-id ID] [--skip-verify]
dataretail360 seed demo [--replace]   Sembrar el dataset demo
dataretail360 seed export|load|verify PATH
dataretail360 seed none | contract
dataretail360 diagram [salida.png]    Matriz de bus del modelo dimensional
```

En Docker: `docker compose run --rm pipeline <comando>`.

Pruebas unitarias (sin bases de datos, incluyen una corrida end-to-end en memoria):

```sh
docker compose run --rm --no-deps --entrypoint python pipeline -m pytest -q /app/tests/unit
```

## Ejecución nativa (sin Docker)

Requiere Python 3.14 (la versión de la imagen y de los locks), ODBC Driver 18 for SQL Server, y PostgreSQL y SQL Server accesibles.

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r requirements/pipeline.lock.txt -r requirements/dev.txt && pip install --no-deps -e .
python -m venv .dbt && .dbt/bin/pip install -r requirements/dbt.lock.txt
export DBT_EXECUTABLE=$PWD/.dbt/bin/dbt
dataretail360 doctor && dataretail360 seed demo && dataretail360 run
```

Variables (también desde un `.env` en el directorio de trabajo):

| Variable | Uso |
|---|---|
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | Origen PostgreSQL |
| `MSSQL_SERVER`, `MSSQL_PORT`, `MSSQL_DATABASE` (defecto `DataRetail360_Demo`) | Destino SQL Server |
| `MSSQL_AUTH` (`sql` o `windows`), `MSSQL_USER`, `MSSQL_PASSWORD` | Autenticación SQL Server |
| `MSSQL_DRIVER`, `MSSQL_TRUST_CERT` | Driver ODBC y confianza en el certificado local |
| `DATARETAIL_HOME` | Workspace de la corrida (defecto: directorio actual) |
| `DBT_PROJECT_DIR`, `DBT_EXECUTABLE` | Proyecto dbt y ejecutable (venv separado) |

El pipeline crea y recarga objetos en la base destino: usar una base **dedicada**,
no un DWH compartido.

## Estructura

```text
src/dataretail360/
  cli.py, pipeline.py, context.py, settings.py   entrada, orquestación, configuración
  contracts.py                                    contrato de columnas de las fuentes
  bronze.py, normalize.py, quality.py, silver.py  extracción, limpieza y calidad
  scd.py, snowflake.py, dimensions.py, facts.py   modelo dimensional
  warehouse/                                      DDL, marts, dbt y publicación atómica
  gold.py, analysis.py, dictionary.py             exportaciones, análisis, diccionarios
  verification.py, closing.py                     controles, benchmark y conclusiones
  observability.py, report.py                     logging JSONL y reporte Markdown
  seed/                                           dataset demo y paquetes portables
dbt/                 modelos y pruebas de los 4 marts
tests/unit/          pruebas unitarias
requirements/        dependencias fijadas (pipeline, dev, dbt)
Dockerfile, compose.yaml
```

## Persistencia y reinicio

Compose usa volúmenes para PostgreSQL, SQL Server y el workspace. `docker compose
stop`/`down` los conservan. Para empezar desde cero:

```sh
docker compose down --volumes   # elimina bases, historia SCD y resultados
docker compose up --build pipeline
```

Las contraseñas por defecto de `.env.example` son públicas y solo para la demo local.
Las bases no exponen puertos al host. No cambiar contraseñas con volúmenes existentes
esperando que roten solas.

## Diagnóstico

- `docker compose ps -a` y `docker compose logs seed|pipeline|sqlserver|postgres`.
- `docker compose run --rm pipeline doctor` comprueba conexiones y dbt.
- Cada corrida elimina sus propios esquemas candidatos `stage_*`/`dbt_*`, también
  cuando falla. Una interrupción abrupta (p. ej. matar el contenedor) puede dejarlos
  huérfanos; no afectan lo publicado y se pueden borrar a mano.
