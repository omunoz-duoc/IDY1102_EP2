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

En Windows y macOS, abrir Docker Desktop y esperar a que el motor esté funcionando.
En Linux, comprobar que Docker Engine esté iniciado y accesible desde la terminal.
No es necesario instalar Python, ODBC, PostgreSQL, SQL Server ni dbt en el host.

Elegir **una** de las rutas siguientes según la terminal. Ambas configuran el
puerto **8889** del equipo para acceder al puerto **8888** interno del contenedor.
Los pasos que crean `.env` son para un clon nuevo; si ya existe, conservarlo y
editar únicamente `NOTEBOOK_PORT=8889`.

### Ruta A: Windows con PowerShell

1. Clonar el repositorio y entrar en la carpeta:

   ```powershell
   git clone https://github.com/omunoz-duoc/IDY1102_EP2.git
   cd IDY1102_EP2
   ```

2. Crear la configuración local y seleccionar el puerto 8889:

   ```powershell
   Copy-Item .env.example .env
   (Get-Content .env) -replace '^NOTEBOOK_PORT=.*$', 'NOTEBOOK_PORT=8889' | Set-Content .env -Encoding utf8
   ```

3. Construir e iniciar todos los servicios:

   ```powershell
   docker compose up --build -d
   docker compose ps
   docker compose logs -f notebook
   ```

   La primera construcción puede tardar varios minutos. Esperar a que PostgreSQL
   y SQL Server estén `healthy` y aparezca `Jupyter Server ... is running at`.
   Presionar **Ctrl+C** para salir del seguimiento de logs; los contenedores siguen funcionando.

4. Obtener el token y abrir el notebook:

   ```powershell
   docker compose exec notebook jupyter server list
   Start-Process "http://localhost:8889/lab/tree/EP3_DataRetail360.ipynb"
   ```

### Ruta B: macOS o Linux con Bash

Los comandos de esta ruta sirven para Bash en ambos sistemas. El conjunto requiere
x86-64 por SQL Server; no se garantiza funcionamiento en Apple Silicon/ARM mediante
emulación. Consultar la [documentación oficial de SQL Server en Docker](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver17).
La ejecución integral de este proyecto se validó en Docker Linux x86-64; no se ha
validado por separado en macOS.

1. Clonar el repositorio y entrar en la carpeta:

   ```bash
   git clone https://github.com/omunoz-duoc/IDY1102_EP2.git
   cd IDY1102_EP2
   ```

2. Crear la configuración con el puerto 8889. Este comando funciona con `sed`
   tanto en macOS como en Linux:

   ```bash
   sed 's/^NOTEBOOK_PORT=.*/NOTEBOOK_PORT=8889/' .env.example > .env
   ```

3. Construir e iniciar todos los servicios:

   ```bash
   docker compose up --build -d
   docker compose ps
   docker compose logs -f notebook
   ```

   Esperar a que PostgreSQL y SQL Server estén `healthy` y aparezca
   `Jupyter Server ... is running at`. Presionar **Ctrl+C** para salir del
   seguimiento de logs; los contenedores siguen funcionando.

4. Obtener el token:

   ```bash
   docker compose exec notebook jupyter server list
   ```

5. Abrir el notebook usando el comando correspondiente al sistema:

   **macOS:**

   ```bash
   open "http://localhost:8889/lab/tree/EP3_DataRetail360.ipynb"
   ```

   **Linux con escritorio:**

   ```bash
   xdg-open "http://localhost:8889/lab/tree/EP3_DataRetail360.ipynb"
   ```

   También se puede pegar [el enlace del notebook](http://localhost:8889/lab/tree/EP3_DataRetail360.ipynb)
   directamente en el navegador del equipo donde se ejecuta Docker.

### Acceso y ejecución de las celdas

1. En la salida de `jupyter server list`, copiar el valor que sigue a `token=`.
   Introducirlo en la pantalla de acceso de Jupyter. El comando muestra el puerto
   **interno 8888**; en el navegador mantener **8889**.
2. Abrir `EP3_DataRetail360.ipynb` y seleccionar **Python 3 (ipykernel)** si solicita un kernel.
3. Seleccionar **Run → Run All Cells**. No iniciar otra corrida mientras esté en ejecución.
4. Esperar hasta que termine y aparezcan las conclusiones. Guardar mediante
   **File → Save Notebook** o el botón de guardar.

Las bases y las fuentes sintéticas se preparan automáticamente. Las ediciones
realizadas en Jupyter se guardan en el volumen Docker; para copiarlas al repositorio
local, seguir la sección **Verificación y exportación**.

### Detener y volver a iniciar

Desde la carpeta del repositorio, en PowerShell o Bash, detener sin borrar datos:

```sh
docker compose stop
```

Para volver a iniciar todos los servicios:

```sh
docker compose up -d
docker compose logs -f notebook
```

Cuando Jupyter esté listo, salir de los logs con **Ctrl+C** y obtener el token:

```sh
docker compose exec notebook jupyter server list
```

Abrir nuevamente [el notebook](http://localhost:8889/lab/tree/EP3_DataRetail360.ipynb).
El token puede cambiar cuando se reinicia o recrea el contenedor.

### Configuración de la demo

Solo Jupyter expone un puerto, ligado a `127.0.0.1`; las bases quedan en la red
interna de Compose. El token de Jupyter sigue habilitado. Las contraseñas por
defecto son públicas y exclusivas de esta demo local. No usar esta configuración
como despliegue de producción. ODBC cifra la conexión y confía en el certificado
local del contenedor.

Sin `.env`, Compose utiliza el puerto 8888; las rutas anteriores lo cambian a 8889.
Para personalizar las contraseñas demo, editar `.env` antes del primer inicio.
Las variables existentes del laboratorio no cambian los hosts ni las bases
dedicadas que define Compose.

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
Linux. Si el puerto 8889 está ocupado, cambiar `NOTEBOOK_PORT` en `.env` por un
puerto libre y ejecutar `docker compose up -d`. Usar ese mismo puerto en el navegador. No cambiar las contraseñas del archivo `.env` esperando que los volúmenes
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
