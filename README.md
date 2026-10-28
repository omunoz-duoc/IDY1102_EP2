# DataRetail 360 — Evaluación Parcial 3

**Asignatura:** IDY1102 · Estructuras y Visualización de Datos  
**Estudiante:** Oscar Muñoz

Repositorio de desarrollo de un notebook integrador para modelado dimensional,
SCD, pipeline ETL y almacenamiento analítico del caso DataRetail 360.

## Decisiones confirmadas

- Entregable principal: `EP3_DataRetail360.ipynb`.
- Destino analítico: SQL Server, con Python, Pandas y SQLAlchemy.
- Fuentes: base poblada del estudiante, CSV de campañas y Excel de reportes
  manuales exportados por el estudiante para la simulación del negocio.
- Desarrollo por etapas con commits locales verificables.
- Texto explicativo y comentarios por sección del pipeline.

## Entorno y ejecución

Origen SQL: PostgreSQL `localhost:5432`, base `idy1102_ep2`, con 14 conjuntos.
Origen CSV: `data/raw/marketing/campanias.csv` (campañas).
Origen Excel: `data/raw/legacy/reportes_ventas.xlsx`, hoja
`legacy_reporte_ventas_xlsx_2026` (reportes manuales).
Destino autorizado: SQL Server `localhost`, base `DataRetail360_DW`, autenticación
de Windows y ODBC Driver 18. La fuente se consulta en modo de solo lectura.

1. Crear un entorno Python 3.14: `python -m venv .venv`.
2. Instalar dependencias: `.venv\Scripts\python -m pip install -r requirements.txt`.
3. Copiar `.env.example` a `.env` y completar la contraseña PostgreSQL localmente.
4. Colocar los dos archivos exportados en las rutas indicadas, con sus encabezados.
5. Abrir `EP3_DataRetail360.ipynb` en Jupyter o VS Code y seleccionar el entorno.
6. Ejecutar todas las celdas, desde la carpeta del repositorio.

Los archivos se consumen directamente con `read_csv` y `read_excel`; el notebook
no los exporta ni consulta sus tablas equivalentes como respaldo. Si falta un
archivo, su hoja o una columna obligatoria, la ingestión se detiene. Los alias
`mkt_campanias` y `legacy_reporte_ventas_xlsx` identifican conjuntos lógicos,
independientemente de sus lectores físicos. `CONTRACT` comprueba las columnas
de entrada; las conversiones de tipos y reglas de negocio se aplican en Silver.

La exportación fue una preparación del laboratorio, no una fuente empresarial
independiente acreditada. SQL se extrae con una instantánea de solo lectura y
los archivos se congelan por contenido; no hay una transacción global entre las
tres fuentes. Los originales se conservan en Bronze y su SHA256, ruta y hoja
figuran en el manifiesto. `data/raw` no se modifica durante el procesamiento.

La ejecución lee aproximadamente 3,4 millones de registros y realiza dos
publicaciones completas para comprobar idempotencia. Puede tardar varios minutos
y requiere memoria y espacio para las capas locales. Las tablas `dw` pertenecen
a esta evaluación; el pipeline renueva sus hechos de forma transaccional y
conserva las versiones de dimensiones leídas del destino.

## Archivos del proyecto

- `EP3_DataRetail360.ipynb`: informe e implementación principal.
- `modelo.drawio`: modelo editable en diagrams.net.
- `requirements.txt`: versiones del entorno utilizado.
- `.env.example`: configuración sin secretos.
- `PLAN.md`: etapas de desarrollo y criterios de evaluación.
- `data/raw/`: archivos de entrada provistos por el estudiante, excluidos de Git.
- Otras subcarpetas de `data/` y `logs/`: evidencias locales generadas, excluidas de Git.

El notebook incluye consultas sobre el destino, pruebas SCD aisladas, conciliación,
rollback, una segunda publicación, diccionario semántico, gráficos y conclusiones.
Sus resultados deben interpretarse con las limitaciones financieras y temporales
declaradas en el propio documento.

Consultar [PLAN.md](PLAN.md) para las etapas y sus criterios de verificación.
Los archivos originales del curso permanecen fuera de este repositorio.

## Datos y credenciales

No versionar credenciales, exportaciones de la base, logs ni archivos generados.
La conexión usará autenticación de Windows o secretos en variables de entorno,
según la configuración que confirme el estudiante.
Antes de cada commit del notebook se revisarán sus salidas para evitar incluir
datos personales o cadenas de conexión sensibles.

Git registra versiones locales; no se ha configurado ni publicado un remoto.
