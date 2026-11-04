# DataRetail 360 — notebook portable con Docker

**IDY1102 · Evaluación Parcial 3 · Oscar Muñoz**

El entregable principal es `EP3_DataRetail360.ipynb`. Un clon limpio genera una
**demo sintética determinista**, extrae 14 tablas de PostgreSQL más un CSV y un
Excel, y publica el DWH y cuatro data marts en SQL Server. No necesita las bases,
archivos privados, entornos virtuales ni credenciales del equipo original.

## Inicio rápido

Requisitos: Git, Docker Engine/Desktop con **contenedores Linux**, Compose v2,
conexión a Internet para la primera construcción y una máquina **x86-64**.
Asignar aproximadamente 6 GB de RAM a Docker como punto de partida para esta demo.
SQL Server Developer se utiliza para desarrollo y evaluación.

Desde la carpeta que contiene `compose.yaml`:

```sh
docker compose up --build -d
docker compose logs -f notebook
```

Esperar a que PostgreSQL y SQL Server estén saludables y aparezca el enlace de
Jupyter con token. Abrir `http://localhost:8888`, introducir ese token y abrir
`EP3_DataRetail360.ipynb`. Seleccionar **Run → Run All Cells** y guardar el notebook.
No ejecutar dos corridas simultáneas contra el mismo destino.

La primera construcción descarga imágenes, ODBC y dependencias. No instalar Python,
ODBC, PostgreSQL, SQL Server ni dbt en el host. Tampoco es necesario crear `.env`.
Para personalizar el puerto o las contraseñas demo, copiar `.env.example` a `.env`
antes del primer inicio. Las variables existentes del laboratorio no cambian los
hosts ni las bases dedicadas que define Compose.

Solo Jupyter expone un puerto, ligado a `127.0.0.1`; las bases quedan en la red
interna de Compose. El token de Jupyter sigue habilitado. Las contraseñas por
defecto son públicas y exclusivas de esta demo local. No usar esta configuración
como despliegue de producción. ODBC cifra la conexión y confía en el certificado
local del contenedor.

SQL Server limita la portabilidad del conjunto a x86-64. Windows/Linux con Docker
Linux son el objetivo; no se promete funcionamiento en Apple Silicon/ARM mediante
emulación. Véase la [documentación oficial de SQL Server en Docker](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver17).

## Datos y resultados

`dataretail/demo.py` genera 3 sucursales, 6 productos y 8 identidades ficticias,
64 documentos de venta entre enero y septiembre de 2026, 5 devoluciones,
36 cortes de inventario, campañas, sesiones, envíos y reposición.
Incluye compras anónimas, meses sin compras, un reembolso posterior al corte,
envíos sin consentimiento y una fecha Excel ambigua para ejercitar las reglas.
Los correos usan `example.invalid`; los identificadores se generan artificialmente.

La preparación inicial crea las 14 tablas en `dr360_raw`, metadatos en
`dr360_control` y los archivos `data/raw/marketing/campanias.csv` y
`data/raw/legacy/reportes_ventas.xlsx`. El generador comparte el contrato de
columnas con el notebook. Los reinicios conservan las fuentes y la historia SCD;
un origen existente sin marcador demo no se sobrescribe.

El notebook conserva Bronze/Silver/Gold, las reglas SCD 1/2/3, las jerarquías de
producto y sucursal, la publicación transaccional mediante candidatos y dbt,
la segunda publicación y la prueba adversa de rollback. Produce estos marts:

| Esquema | Tabla principal |
|---|---|
| `dm_comercial` | `ventas_mensuales` |
| `dm_operaciones` | `inventario_actual` |
| `dm_marketing` | `campanias_resumen` |
| `dm_clientes` | `clientes_mensuales` |

Las salidas históricas se retiraron del notebook versionado para que la evidencia
corresponda a la ejecución actual. Los conteos del dataset original de millones de
filas no son resultados de esta demo. Las conclusiones se calculan al ejecutar.

## Verificación y exportación

Pruebas de Python (incluyen transformaciones reales del notebook con la demo):

```sh
docker compose exec notebook python -m unittest discover -s tests -v
```

Ejecución completa sin interfaz, con error de salida si falla una celda:

```sh
docker compose exec notebook python -m dataretail.execute
```

El resultado, incluso si hay un error, queda en
`/app/artifacts/EP3_DataRetail360.ejecutado.ipynb`. La validación completa incluye
los modelos/pruebas dbt, publicación repetida y rollback controlado del notebook.
No basta con que los contenedores arranquen para considerar el pipeline validado.

Exportar la ejecución o los cambios guardados en Jupyter:

```sh
docker compose cp notebook:/app/artifacts/EP3_DataRetail360.ejecutado.ipynb ./EP3_DataRetail360.ejecutado.ipynb
docker compose cp notebook:/app/EP3_DataRetail360.ipynb ./EP3_DataRetail360.ipynb
```

Exportar logs y evidencia dbt antes de reiniciar desde cero:

```sh
docker compose cp notebook:/app/artifacts ./artifacts-demo
docker compose cp notebook:/app/logs ./logs-demo
```

## Persistencia y actualización

Compose usa tres volúmenes dedicados: PostgreSQL, SQL Server y `/app` (notebook,
fuentes y resultados). El workspace se copia de la imagen **solo al crear su
volumen por primera vez**. El proyecto del host no se monta: no se incluyen `.env`,
Git, bases ni archivos privados en la imagen gracias a `.dockerignore`.

```sh
docker compose stop
docker compose start
```

Estos comandos conservan datos y ediciones. `docker compose down` también conserva
los volúmenes. Para actualizar código en una demo existente, exportar primero las
ediciones y resultados. Después, si se desea reiniciar **toda la demo**, ejecutar:

```sh
docker compose down --volumes
docker compose up --build -d
```

**`down --volumes` elimina las bases, el notebook editado y resultados de esta demo.**
No ejecutarlo sin exportar lo que se quiera conservar. Reconstruir únicamente la
imagen no actualiza el workspace de un volumen existente.

## Organización y configuración

- `EP3_DataRetail360.ipynb`: explicación, orquestación, análisis y evidencia principal.
- `dataretail/config.py`: rutas desde el repositorio, conexiones y resolución de dbt.
- `dataretail/contracts.py`: contrato de columnas compartido.
- `dataretail/demo.py` y `start.py`: generación e inicialización exclusiva de la demo.
- `dataretail/execute.py`: ejecución completa y guardado del notebook.
- `snowflake.py` y `pipeline_logging.py`: jerarquías y seguimiento del pipeline.
- `dbt/`: modelos, perfil configurable y pruebas de los cuatro marts.
- `Dockerfile`, `compose.yaml`: infraestructura reproducible y comprobaciones de salud.
- `requirements.txt`: dependencias analíticas fijadas; dbt tiene entorno separado en `/opt/dbt`.
- `PLAN.md`: alcance y criterios de aceptación de la portabilidad.

Para un entorno externo administrado por el usuario, el notebook admite `PGHOST`,
`PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `MSSQL_SERVER`, `MSSQL_PORT`,
`MSSQL_DATABASE`, `MSSQL_AUTH` (`sql` o `windows`), `MSSQL_USER`, `MSSQL_PASSWORD`,
`MSSQL_DRIVER`, `MSSQL_TRUST_CERT` y `DBT_EXECUTABLE`. Se requiere el mismo contrato
SQL/CSV/Excel y una base destino creada previamente. El inicializador se niega a
sembrar bases distintas de `dataretail_demo` y `DataRetail360_Demo`.
El modo Docker usa autenticación SQL; el modo Windows integrado es opcional.
Las cargas demo usan SQLAlchemy/pyodbc; bcp solo se habilita en Windows integrado.

El notebook crea y recarga objetos analíticos del destino configurado. Usar una base
dedicada. No apuntarlo a un DWH compartido sin revisar ese alcance.

## Diagnóstico

`docker compose ps` muestra el estado. Consultar `docker compose logs postgres`,
`docker compose logs sqlserver` y `docker compose logs notebook` para fallos de
arranque. Si el motor Docker no responde, iniciar Docker Desktop con contenedores
Linux. Si el puerto 8888 está ocupado, ajustar `NOTEBOOK_PORT` antes de recrear el
servicio. No cambiar las contraseñas del archivo `.env` esperando que los volúmenes
existentes roten sus credenciales automáticamente.

Durante el procesamiento, `logs/` contiene eventos legibles y JSONL; los artefactos
dbt quedan en `artifacts/dbt/<publicación>/`. El notebook informa errores y registra
la limpieza de candidatos. Una interrupción abrupta puede dejar candidatos huérfanos.

## Validación realizada

El 2 de noviembre de 2026 (America/Santiago) se construyó la imagen y se ejecutó
el notebook completo en Docker Linux x86-64 con PostgreSQL 17 y SQL Server 2022.
Las 32 celdas de código terminaron sin errores: 37 pruebas Python, 35 controles
internos y 58 pruebas dbt aprobadas en ambas publicaciones. La prueba adversa
rechazó el candidato alterado y conservó los resultados publicados.
La evidencia local se guardó en
`artifacts/demo-validation/EP3_DataRetail360.ejecutado.ipynb` (excluida de Git).
