# 4. Diagramas de Solución y Flujo de Datos End-to-End — BBVA PRISMA

> **Ubicación de los archivos gráficos:** Los diagramas vectoriales diseñados en Lucidchart se almacenan en la carpeta [`docs/diagrams/`](diagrams/) del repositorio:
> - [`docs/diagrams/arquitectura_infraestructura.svg`](diagrams/arquitectura_infraestructura.svg) — Diagrama de Arquitectura de Infraestructura y Contenedores.
> - [`docs/diagrams/flujo_datos_end_to_end.svg`](diagrams/flujo_datos_end_to_end.svg) — Diagrama de Linaje y Flujo de Datos End-to-End.

---

## 4.1 Arquitectura de Infraestructura (`docker/docker-compose.yml` y OCI)

![Diagrama de Arquitectura de Infraestructura](diagrams/arquitectura_infraestructura.svg)

La infraestructura se divide en dos entornos desacoplados que se comunican a través de **Amazon S3**:

### A. Clúster de Procesamiento y Orquestación ETL (`docker/docker-compose.yml`)
Conectados mediante la red bridge interna `spark-network`:

| Contenedor | Imagen / Construcción | Puertos Expuestos | Volúmenes Montados | Función Técnica |
| :--- | :--- | :--- | :--- | :--- |
| **`airflow-postgres`** | `postgres:18.6-trixie` | `5432` (interno) | `airflow-postgres-data:/var/lib/postgresql` | Base de datos relacional de metadatos e historial de corridas de Apache Airflow. |
| **`airflow`** | `docker/airflow/Dockerfile` (`apache/airflow:3.3.1-python3.11` + Java 17 + PySpark 4.0.4 + XGBoost 3.2.0) | `8080:8080` | `../etl/dags:/opt/airflow/dags`, `../etl/src:/opt/project/src`, `../data:/opt/project/data` | Orquestador con `LocalExecutor`. Ejecuta diariamente la cadena `bronze_cleaning_pipeline` → `silver_cleaning_pipeline` → `gold_analysis_pipeline` a las `00:00` (`America/Mexico_City`). |
| **`spark-master`** | `docker/spark/Dockerfile` (`apache/spark:4.0.4` + Python 3.11 + `hadoop-aws-3.4.1.jar`) | `7077:7077` (RPC), `8081:8080` (Web UI) | `../etl/src:/opt/project/src`, `../data:/opt/project/data` | Nodo coordinador del clúster Spark al que Airflow envía los jobs mediante `SparkSubmitOperator` (`spark://spark-master:7077`). |
| **`spark-worker`** | `docker/spark/Dockerfile` | `8082:8081` (Web UI) | `../etl/src:/opt/project/src`, `../data:/opt/project/data` | Nodo ejecutor distribuido configurado con `4 cores` y `2 GB` de RAM para procesar las capas Medallion contra S3 (`s3a://`). |

### B. Capa de Consumo Web (OCI) y Analítica Ejecutiva (Local / Power BI)

| Servicio | Entorno de Ejecución | Puerto | Almacenamiento Local / Nube | Función Técnica |
| :--- | :--- | :--- | :--- | :--- |
| **Aplicación Web PRISMA (`web/`)** | Instancia VM en **Oracle Cloud Infrastructure (OCI)** desplegada por `.github/workflows/cd.yml` (`prisma-web.service`) | `8000` | SQLite local (`goals.sqlite3`, `monitoring.sqlite3`) + lectura de `data/gold/*.parquet` y escritura de lotes en `history/assistant_interactions/` en **Amazon S3** | Portal del cliente (`login.html`, `dashboard.html`) y API FastAPI con el agente **LangGraph** (`web/backend/assistant.py`). |
| **Asistente BI (`bi/chat/`)** | Servicio FastAPI local para análisis ejecutivo | `8001` | Lectura directa de `history/assistant_interactions/*.csv` y `data/gold/*.parquet` en **Amazon S3** | Microservicio analítico con caché en memoria, guardrails de BI y generación de mini-gráficas para el dashboard. |
| **Dashboard Ejecutivo (`bi/dashboard/`)** | **Power BI Desktop** (`financialDigitalTwin.pbip`) | Local | Conexión HTTP/XML dinámica (`ListObjectsV2`) hacia **Amazon S3** + `<iframe>` hacia `http://127.0.0.1:8001` | Visualización de **Monitoreo del Asistente**, **Comportamiento Transaccional de Clientes (Gold 3M)** y **Chat Ejecutivo Embebido**. |

---

## 4.2 Flujo de Datos End-to-End (Data Lineage)

![Diagrama de Flujo de Datos End-to-End](diagrams/flujo_datos_end_to_end.svg)

### Etapa 1: Ingesta Cruda → Capa Bronze
1. Los archivos fuente (`users_data.csv`, `cards_data.csv`, `transactions_data.csv`, `accounts_data.csv`, `obligations_payments_data.csv`) se depositan en `s3a://<S3_BUCKET>/data/incoming/`.
2. El DAG `bronze_cleaning_pipeline` (`etl/dags/bronze_pipeline.py`) invoca `etl/src/bronze_cleaning.py` en Spark (`--total-executor-cores 4`, `2g` RAM, con `retries=1`).
3. Se estandarizan textos, importes monetarios y tipos básicos, escribiendo tablas Parquet en `s3a://<S3_BUCKET>/data/bronze/` y disparando automáticamente `silver_cleaning_pipeline` mediante `TriggerDagRunOperator`.

### Etapa 2: Capa Bronze → Capa Silver y Cuarentena
1. El DAG `silver_cleaning_pipeline` (`etl/dags/silver_pipeline.py`) ejecuta `etl/src/silver_cleaning.py`.
2. Se deduplican usuarios, tarjetas, cuentas, obligaciones y transacciones (`conservar_completo`), se validan reglas financieras y se desvían los registros inválidos a `s3a://<S3_BUCKET>/data/silver/rejected/cards.parquet` y `rejected/transactions.parquet`.
3. Se imputan límites de crédito faltantes con `KNeighborsRegressor`, se persisten las tablas limpias en `s3a://<S3_BUCKET>/data/silver/` y se dispara `gold_analysis_pipeline`.

### Etapa 3: Capa Silver → Capa Gold, Modelo XGBoost y Simulación Monte Carlo
El DAG `gold_analysis_pipeline` (`etl/dags/gold_pipeline.py`) ejecuta tres tareas secuenciales en Spark:
1. **`gold_base_task` (`gold_analysis.py --stage base`):** construye las series mensuales, ventanas de los últimos 3 meses (promedios, desviación estándar, ratios de ahorro/deuda/gasto y variaciones de ingreso), gastos por categoría y estatus de salud financiera en `s3a://<S3_BUCKET>/data/gold/`, marcando la validación del modelo como `PENDIENTE`.
2. **`income_model_task` (`income_model.py` + `monte_carlo.py`):**
   - Entrena **XGBoost** y genera las predicciones del mes siguiente en `s3a://<S3_BUCKET>/data/gold/staging/income_predictions.parquet`.
   - Ejecuta las **2,000 simulaciones Monte Carlo** a 1, 2 y 3 meses guardando `s3a://<S3_BUCKET>/data/gold/monte_carlo_scenarios.parquet`.
3. **`gold_predictions_task` (`gold_analysis.py --stage predictions`):** audita las predicciones generadas, las integra en `customer_model_summary.parquet` y actualiza `model_validation.parquet` a estado `APROBADO`.

### Etapa 4: Consumo en Tiempo Real, Telemetría y Retroalimentación en BI
1. **Portal del Cliente (`web/` en OCI):** carga desde S3 `customer_model_summary.parquet`, `customer_recent_months.parquet`, `customer_recent_expenses_by_category.parquet` y `monte_carlo_scenarios.parquet` con caché TTL (`web/backend/data.py`), sirviendo el panorama financiero y el chat de **LangGraph** (`web/backend/assistant.py`).
2. **Exportación de Telemetría (`web/backend/monitoring.py`):** cada turno del asistente registra latencia, estado HTTP, uso de herramientas, tokens y costo en SQLite local y un hilo en segundo plano exporta periódicamente lotes CSV (`utf-8-sig`) hacia `s3://<S3_BUCKET>/history/assistant_interactions/YYYY/MM/DD/lote_*.csv`.
3. **Consumo Ejecutivo (`bi/dashboard/` y `bi/chat/`):** tanto Power BI Desktop como el asistente analítico del dashboard (`http://127.0.0.1:8001`) consultan directamente en S3 los lotes de `history/assistant_interactions/` y las carpetas `.parquet/` de `data/gold/`, cerrando el ciclo de observabilidad y negocio de extremo a extremo.

