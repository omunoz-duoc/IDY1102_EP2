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
- Resultado final: cuatro data marts dependientes del DWH para Comercial,
  Operaciones, Marketing-Digital y Clientes.

## Data marts como producto final

El flujo culmina en cuatro esquemas de SQL Server dentro de `DataRetail360_DW`:

| Esquema | Tabla resumen persistente | Propósito |
|---|---|---|
| `dm_comercial` | `ventas_mensuales` | Ventas y devoluciones por mes, sucursal y canal; ticket y análisis de clientes/productos |
| `dm_operaciones` | `inventario_actual` | Último corte común de stock por producto/sucursal; alertas, histórico y reposición |
| `dm_marketing` | `campanias_resumen` | Campañas, consentimiento, sesiones y contraste con ventas asociadas a cupón |
| `dm_clientes` | `clientes_mensuales` | Compras, devoluciones, recurrencia, actividad y medidas RFM observadas por identidad y mes |

Son marts dependientes: cada uno contiene una tabla resumen física y vistas
de los hechos y dimensiones conformadas del DWH. Las seis tablas de hechos
siguen integradas en `dw`; las vistas no duplican físicamente esos hechos.
Marketing y navegación se agrupan para analizar adquisición y conversión,
mientras que el e-commerce forma parte de la venta omnicanal de Comercial.

`publish()` carga los frames del DWH en un esquema candidato propio y ejecuta
`dbt build`: cuatro tablas resumen, una vista intermedia y 33 pruebas. Python
promueve la misma instantánea y los resultados aprobados en una sola transacción
con el DWH. Las FK se respetan limpiando sus resúmenes antes de recargar las
dimensiones. Una excepción revierte toda la publicación; un fallo previo de dbt
no modifica los objetos finales. dbt no garantiza por sí mismo una transacción
global entre modelos.
`ctl.datamart_publicaciones` registra la corrida y conteos por mart.

Las consultas de negocio consumen los esquemas `dm_*`. Los resúmenes y su
catálogo se exportan a `data/gold/datamarts/`. El diccionario explica su grano,
las métricas sumables y las tasas que deben recalcularse desde sus componentes.

### Data mart Clientes

El grano es `cliente_nk × año × mes`, independiente de las versiones SCD.
Se incluyen meses sin compras desde la primera actividad identificable hasta
`fecha_fin`, cobertura fiable de compras declarada por el contrato del dataset.
Los atributos se enlazan
con la versión SCD válida al corte, con una bandera si no hay historia disponible.
El corte es fin de mes, salvo el último mes parcial, que termina en `fecha_fin`;
la fecha del sistema no altera la recencia al reejecutar. Los reembolsos posteriores
permanecen en los marts financieros y en `v_cobertura_identidad`, marcados con
`dentro_cobertura_compras=0`. No generan meses artificiales de compras cero ni
un supuesto abandono de clientes cuando la fuente de ventas ya no tiene cobertura.

Una compra cuenta un documento distinto. Se conservan unidades, ventas,
devoluciones, netos, primeras/últimas compras del mes, acumulados y ventanas de
hasta 12 meses calendario. La recencia usa la última compra observada hasta el
corte; activo significa compra en los seis meses calendario anteriores, inclusive.
Los meses sin actividad mantienen cero en sus flujos y conservan el último
estado observado. Las devoluciones posteriores pueden producir un neto negativo.

`v_clientes_mensuales` expone atributos y ticket; `v_estado_clientes` y `v_rfm`
presentan el último corte cubierto por compras. `v_cobertura_identidad` separa la
actividad financiera identificada/anónima y dentro/fuera de cobertura temporal.
Las compras anónimas no se transforman en una persona;
los clientes registrados sin actividad observada quedan fuera de esta población.
Los ejemplos del notebook usan la fecha de negocio del caso para comparar el
mismo indicador de cliente activo. Los cortes se muestran explícitamente.

Las ventanas RFM contienen medidas observadas, sin scores o predicciones de
abandono o valor de vida. Clientes únicos requieren `COUNT DISTINCT` entre meses;
los acumulados y ventanas se comparan dentro de un solo corte.

## Entorno y ejecución

### Entorno dbt independiente

`requirements.txt` conserva el entorno del notebook. dbt usa Python 3.14 en
`.venv-dbt`, con dbt Core 1.12.6 y dbt SQL Server 1.12.1:

```powershell
python -m venv .venv-dbt
.\.venv-dbt\Scripts\python -m pip install -r requirements-dbt-lock.txt
.\.venv-dbt\Scripts\dbt debug --project-dir dbt --profiles-dir dbt
```

El perfil versionado no contiene contraseñas: usa Windows, ODBC 18 y las mismas
variables `MSSQL_SERVER`/`MSSQL_DATABASE` que el notebook. `port: null` permite
resolver la instancia local sin forzar TCP 1433. La comprobación real de conexión
y `pip check` pasaron durante la etapa 1. El notebook explica las etapas y la
publicación mediante candidatos antes de presentar los marts.

### Modelos y pruebas dbt

`dbt/models/` contiene cuatro modelos tabla (los resúmenes de los marts) y la
vista intermedia `int_documentos_campania`, utilizada por Marketing mediante
`ref()`. Las doce tablas integradas se declaran con `source()`. El esquema fuente
se configura por corrida, sin cambiar la lógica SQL. `dbt/tests/` y las pruebas
de `schema.yml` controlan grano, nulos obligatorios, doce relaciones a dimensiones,
conciliación financiera, inventario, atribución de campañas, cobertura y ventanas
de Clientes. Son 33 pruebas de datos. La etapa 3 verificó además igualdad completa
con los cuatro marts anteriores mediante `EXCEPT` en ambas direcciones.

Para desarrollo aislado sobre el DWH ya publicado:

```powershell
.\.venv-dbt\Scripts\dbt build --project-dir dbt --profiles-dir dbt
.\.venv-dbt\Scripts\dbt docs generate --project-dir dbt --profiles-dir dbt
```

Estos comandos escriben candidatos en `dbt_dev`; la publicación final la realiza
el notebook. `manifest.json`, `run_results.json` y `catalog.json` permiten revisar
dependencias, pruebas y columnas. La documentación HTML se genera en `target/`.

El notebook orquesta el flujo completo automáticamente. Conserva por corrida
`artifacts/dbt/<run_id>/build_manifest.json`, `build_run_results.json`,
`catalog.json`, `static_index.html` y `publication.json` (versiones, hashes, corte,
estado de promoción y limpieza). Las tablas candidatas usan esquemas únicos
`stage_<token>` y `dbt_<token>`; se eliminan al finalizar, incluso ante errores
controlados. Un cierre abrupto puede dejar candidatos huérfanos para revisión
administrativa. Los artefactos y logs quedan disponibles para diagnosticar fallas.

El notebook muestra dependencias, resultados de pruebas y SQL expandible sin
necesitar abrir otros archivos. Verifica además un caso adverso real: altera un
peso en un candidato aislado, comprueba el rechazo de dbt y que los marts finales
permanezcan iguales. Las exportaciones de archivos se verifican después de la
transacción SQL; no se afirma atomicidad distribuida entre SQL y disco.

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
conserva las versiones de dimensiones leídas del destino. Ambas publicaciones
incluyen los cuatro marts; las pruebas comparan sus conteos y contenido, comprueban
la conciliación con el DWH y simulan un fallo para verificar el rollback.

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
