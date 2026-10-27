# Plan de desarrollo por etapas

## Condiciones previas

Confirmado: PostgreSQL `localhost:5432/idy1102_ep2` como origen y SQL Server
`localhost/DataRetail360_DW` con autenticación Windows como destino autorizado.
El SQL entregado describe estructuras con sintaxis PostgreSQL; no demuestra
qué motor aloja actualmente los datos. Inspeccionar la fuente antes de adaptar
el diseño y preservar sus tablas originales.

## Commits previstos

1. `docs: registra alcance y plan de la EP3`
   Portada del proyecto, decisiones, exclusiones de Git y este plan.
2. `docs: define fuentes y modelo dimensional`
   Notebook inicial: portada, problema, inventario verificado, Bus Matrix,
   granularidad, diagrama y diccionario inicial. Ajustar alcance a datos reales.
3. `feat: incorpora extraccion y capa bronze`
   Configuración, conexiones, contratos de estructura y trazabilidad de lotes.
4. `feat: implementa calidad e integracion silver`
   Homologación, deduplicación por claves de negocio, rechazos y conciliación.
5. `feat: implementa dimensiones y SCD`
   Tipos 1, 2 y 3 justificados; pruebas de vigencias y conservación de historia.
6. `feat: carga modelo analitico en SQL Server`
   Dimensiones y hechos, transacciones, integridad, idempotencia y capa Gold.
7. `feat: incorpora indicadores y capa semantica`
   Definiciones comunes, consultas, gráficos y trazabilidad de resultados.
8. `test: verifica robustez y optimizacion del pipeline`
   Reejecución, errores controlados, conciliación, índices y Parquet.
9. `docs: completa evidencias y conclusiones de la EP3`
   Ejecución integral verificada, referencias, historial y cobertura de rúbrica.

Cada commit contendrá trabajo real revisado. El orden puede ajustarse a las
dependencias técnicas; no se simularán fechas, pruebas ni avances históricos.

## Cobertura del notebook

- Portada: Oscar Muñoz; IDY1102; título del proyecto.
- Introducción, objetivos, alcance y supuestos explícitos.
- Modelo dimensional con claves sustitutas, jerarquías y dimensiones conformadas.
- Ventas omnicanal, devoluciones, inventario semanal, campañas y sesiones web;
  reposición como apoyo operacional, sujeto a inspección de los datos.
- Reportes manuales usados para conciliación, evitando duplicar ventas.
- SCD 1/2/3 con demostraciones controladas que no alteren la fuente.
- ETL con validaciones, logging, manejo de errores y trazabilidad.
- Bronze/Silver/Gold persistentes y destino analítico SQL Server.
- Diccionario semántico y reglas explícitas de agregación de indicadores.
- Pruebas de integridad, reejecución, conciliación y asignación temporal SCD.
- Optimización medida y explicación de alternativas arquitectónicas.
- Conclusiones, limitaciones, referencias y matriz de evidencia de la pauta.

## Criterios de entrega

Un notebook principal, ordenado y ejecutable de principio a fin en el entorno
documentado. Cada sección explicará propósito, entradas, transformaciones y
resultados. Los archivos auxiliares de ejecución serán generados o documentados.
Las salidas versionadas se revisarán para proteger datos personales y secretos.
Las evidencias distinguirán resultados ejecutados de propuestas conceptuales.
