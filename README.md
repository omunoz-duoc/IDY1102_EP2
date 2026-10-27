# DataRetail 360 — Evaluación Parcial 3

**Asignatura:** IDY1102 · Estructuras y Visualización de Datos  
**Estudiante:** Oscar Muñoz

Repositorio de desarrollo de un notebook integrador para modelado dimensional,
SCD, pipeline ETL y almacenamiento analítico del caso DataRetail 360.

## Decisiones confirmadas

- Entregable principal: `EP3_DataRetail360.ipynb`.
- Destino analítico: SQL Server, con Python, Pandas y SQLAlchemy.
- Fuente: base de datos ya poblada del estudiante. No generar datos sintéticos
  como sustituto de esa fuente.
- Desarrollo por etapas con commits locales verificables.
- Texto explicativo y comentarios por sección del pipeline.

## Entorno y ejecución

Origen confirmado: PostgreSQL `localhost:5432`, base `idy1102_ep2`.
Destino autorizado: SQL Server `localhost`, base `DataRetail360_DW`, autenticación
de Windows y ODBC Driver 18. La fuente se consulta en modo de solo lectura.

1. Crear un entorno Python 3.14: `python -m venv .venv`.
2. Instalar dependencias: `.venv\Scripts\python -m pip install -r requirements.txt`.
3. Copiar `.env.example` a `.env` y completar la contraseña PostgreSQL localmente.
4. Abrir `EP3_DataRetail360.ipynb` en Jupyter o VS Code y seleccionar el entorno.
5. Ejecutar todas las celdas, desde la carpeta del repositorio.

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
- `data/` y `logs/`: evidencias locales regenerables, excluidas de Git.

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
