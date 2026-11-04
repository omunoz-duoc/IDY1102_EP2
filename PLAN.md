# Plan de portabilidad — DataRetail 360

## Alcance confirmado

Docker con contenedores Linux, PostgreSQL como origen, SQL Server como destino,
dataset sintético reproducible y notebook como entregable principal. Objetivo
x86-64 por la plataforma soportada por SQL Server. No cambiar la semántica de
negocio ni sustituir los motores por SQLite/DuckDB en la ejecución real.

## Implementación

1. Inventariar contratos, rutas, autenticación, dependencias y pruebas existentes.
2. Centralizar configuración y contrato compartido; mantener el flujo didáctico
   y analítico en el notebook y los módulos existentes de jerarquías/logging.
3. Crear imagen Python/ODBC con entorno dbt separado y Compose con bases saludables.
4. Generar las 16 fuentes sintéticas y metadatos; inicializar solo bases demo,
   conservar historia y rechazar sobrescritura de entradas no identificadas.
5. Adaptar notebook y perfil dbt a SQL auth, rutas Linux y nombres configurables;
   retirar salidas del laboratorio e incorporar ejecución automatizada.
6. Documentar inicio, persistencia, exportación, actualización y límites de plataforma.
7. Verificar pruebas Python, estructura del notebook y Compose. Validar construcción,
   ejecución integral, segunda publicación y rollback con Docker disponible.

## Criterios de aceptación

- Un clon limpio arranca con `docker compose up --build` sin archivos privados.
- Jupyter requiere token y solo se expone en localhost; bases en red interna.
- Demo determinista con 14 tablas SQL, CSV, Excel y las tres tablas de control.
- Pipeline conserva cuatro marts, SCD, conciliaciones y reglas de publicación.
- Pruebas Python aprobadas y notebook ejecutado de principio a fin en Docker.
- README distingue comprobaciones realizadas de validación pendiente.
- No se publican cambios ni se modifican las bases originales del laboratorio.

## Resultado

Implementación y validación Docker completadas: 32 celdas de código sin errores,
37 pruebas Python, 35 controles internos y 58 pruebas dbt en ambas publicaciones.
Evidencia local: `artifacts/demo-validation/EP3_DataRetail360.ejecutado.ipynb`.
