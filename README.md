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

## Estado

Plan documentado; implementación pendiente de configuración de acceso a la fuente
y al destino. No se ha creado ni ejecutado el pipeline.

Consultar [PLAN.md](PLAN.md) para las etapas y sus criterios de verificación.
Los archivos originales del curso permanecen fuera de este repositorio.

## Datos y credenciales

No versionar credenciales, exportaciones de la base, logs ni archivos generados.
La conexión usará autenticación de Windows o secretos en variables de entorno,
según la configuración que confirme el estudiante.
Antes de cada commit del notebook se revisarán sus salidas para evitar incluir
datos personales o cadenas de conexión sensibles.

Git registra versiones locales; no se ha configurado ni publicado un remoto.
