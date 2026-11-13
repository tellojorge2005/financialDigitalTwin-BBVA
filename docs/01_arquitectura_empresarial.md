# 1. Documento de Arquitectura Empresarial — BBVA PRISMA

## 1.1 Justificación del Stack Tecnológico (Lakehouse sobre Data Warehouse Tradicional)

El proyecto **BBVA PRISMA** (`financialDigitalTwin-BBVA`) adopta una arquitectura **Data Lakehouse** sobre **Amazon S3**, **Apache Parquet** y **Apache Spark (PySpark)** orquestada con **Apache Airflow**, en lugar de un Data Warehouse monolítico tradicional, por cuatro razones técnicas:

| Criterio                                        | Data Warehouse Tradicional                                                                                | Arquitectura Lakehouse Implementada (`S3 + Parquet + PySpark`)                                                                                                                                                                     |
| :---------------------------------------------- | :-------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Desacoplamiento de cómputo y almacenamiento** | Almacenamiento y motor SQL acoplados; escalar capacidad implica escalar costo de cómputo permanentemente. | Almacenamiento persistente en **Amazon S3** (`s3a://<bucket>/data/...`) independiente de los contenedores de **Spark** (`docker/docker-compose.yml`) y del servidor web en **OCI**.                                                |
| **Soporte nativo para IA, ML y Simulación**     | Requiere exportar tablas fuera del almacén para entrenar modelos o ejecutar simulaciones estocásticas.    | **XGBoost** (`etl/src/income_model.py`), **Monte Carlo** (`etl/src/monte_carlo.py`), **KNN** (`etl/src/silver_cleaning.py`) y los asistentes con **LangGraph/LangChain** consumen directamente los archivos columnares `.parquet`. |
| **Tolerancia a esquemas crudos y cuarentena**   | Falla ante archivos CSV con tipos sucios (`$`, comas, nulos o registros huérfanos) antes de cargar.       | La capa **Bronze** ingesta datos crudos estandarizando tipos con `try_cast`, y la capa **Silver** aísla registros inválidos en rutas de cuarentena (`rejected/`) sin detener el pipeline.                                          |
| **Eficiencia de lectura (Formato Columnar)**    | Lecturas por fila costosas para agregaciones de series de tiempo.                                         | **Apache Parquet** con compresión `snappy` permite que `pyarrow` en la aplicación web (`web/backend/data.py`) y Power BI (`bi/dashboard/`) lean únicamente las columnas necesarias en milisegundos.                                |

---

## 1.2 Estrategia de Modelado Medallion (`Bronze` → `Silver` → `Gold`)

El procesamiento distribuido se ejecuta en **PySpark 4.0.4** mediante tres etapas encadenadas automáticamente por **Airflow** (`bronze_cleaning_pipeline` → `silver_cleaning_pipeline` → `gold_analysis_pipeline`):

### Capa Bronze (`etl/src/bronze_cleaning.py`)

- **Origen:** `s3a://<S3_BUCKET>/data/incoming/` (`users_data.csv`, `cards_data.csv`, `transactions_data.csv`, `accounts_data.csv`, `obligations_payments_data.csv`).
- **Destino:** `s3a://<S3_BUCKET>/data/bronze/*.parquet`.
- **Transformaciones:**
  - Limpieza de cadenas (`trim`, `lower` en correos/categorías y normalización de espacios en nombres y ubicaciones).
  - Conversión segura de importes monetarios retirando símbolos `$` y comas (`convertir_importes`), y conversión tolerante a fallos mediante `try_cast` (`bigint`, `double`).
  - Acotación de rangos demográficos y crediticios (`current_age` entre `18` y `120`, `credit_score` entre `300` y `850`, `credit_limit >= 0`) e imputación base con mediana (`approxQuantile`) y moda.

### Capa Silver (`etl/src/silver_cleaning.py`)

- **Origen:** `s3a://<S3_BUCKET>/data/bronze/*.parquet`.
- **Destino:** `s3a://<S3_BUCKET>/data/silver/*.parquet` y cuarentena en `s3a://<S3_BUCKET>/data/silver/rejected/*.parquet`.
- **Transformaciones y Calidad Declarativa:**
  - **Deduplicación determinista (`conservar_completo`):** ventana particionada por clave primaria que conserva el registro con mayor número de campos no nulos y desempata mediante hash `SHA-256`.
  - **Integridad referencial y reglas de negocio:** validación de formato de correo electrónico por expresión regular, unicidad de correo por cliente, consistencia de saldos (`balance_before`, `amount`, `balance_after`), signos contables (`signed_cashflow` vs. `legacy_unchanged`), transferencias internas y pagos de obligaciones.
  - **Cuarentena automatizada (`rejected/`):** las tarjetas y transacciones con identificadores nulos/negativos o asociadas a un `client_id` inexistente se desvían a `data/silver/rejected/cards.parquet` y `data/silver/rejected/transactions.parquet` etiquetadas con la columna `rejection_reason`.
  - **Imputación avanzada ML:** los límites de crédito faltantes se estiman con un pipeline `KNeighborsRegressor` (`scikit-learn`) entrenado con validación cruzada por grupos (`GroupShuffleSplit` sobre `client_id`) y marcado con la bandera `credit_limit_imputed`.

### Capa Gold (`etl/src/gold_analysis.py`, `income_model.py`, `monte_carlo.py`)

- **Origen:** `s3a://<S3_BUCKET>/data/silver/*.parquet`.
- **Destino:** `s3a://<S3_BUCKET>/data/gold/*.parquet`.
- **Estructuras analíticas y Feature Store generados:**
  1. `customer_model_summary.parquet` y `customer_financial_summary.parquet`: perfil integral por cliente con métricas históricas, indicadores de los últimos 3 meses (promedios, desviación estándar, ratios y variaciones de ingresos) y predicciones validadas.
  2. `customer_monthly_summary.parquet` y `account_monthly_summary.parquet`: series mensuales de ingresos, gastos, pagos de deuda, transferencias, flujo neto (`monthly_net_flow`), mora (`days_past_due`) y rezagos temporales (`income_lag_1`, `income_lag_3`, `income_mean_3`, `income_std_3`).
  3. `customer_recent_months.parquet`: ventana móvil de los últimos 3 meses cerrados por cliente.
  4. `customer_expenses_by_category.parquet` y `customer_recent_expenses_by_category.parquet`: agregación de monto y frecuencia de gasto por categoría.
  5. `customer_financial_health.parquet`: clasificación de salud financiera (`margen_positivo`, `sin_margen`, `requiere_atencion`, `sin_ingresos`, `historial_insuficiente`).
  6. `transactions_summary.parquet` y `cards_summary.parquet`: métricas agregadas de uso transaccional y productos de crédito por cliente.
  7. `monte_carlo_scenarios.parquet`: distribuciones probabilísticas (`p10`, `p50`, `p90`) y probabilidad de flujo negativo o déficit a 1, 2 y 3 meses.

---

## 1.3 Modelaje Predictivo y Simulación Estocástica

1. **Predicción de Ingresos (`etl/src/income_model.py`):**
   - Entrena un regresor **XGBoost** (`XGBRegressor`) sobre las series mensuales de Gold utilizando rezagos (`income_lag_1`, `income_lag_3`), medias móviles (`income_mean_3`, `expenses_mean_3`), volatilidad (`income_std_3`) y estacionalidad (`month_number`).
   - Evalúa el desempeño temporal contra un baseline de media móvil a 3 meses; la etapa `--stage predictions` de `gold_analysis.py` valida el esquema, ausencia de valores `NaN`/`Inf` y rangos antes de habilitar `has_income_prediction = True` y `prediction_validation_status = "APROBADO"`.
2. **Simulador Estocástico Monte Carlo (`etl/src/monte_carlo.py`):**
   - Ejecuta **2,000 simulaciones por cliente** proyectando horizontes de **1, 2 y 3 meses** para ingresos, gastos y saldo acumulado.
   - Calcula percentiles pesimista (`p10`), esperado (`p50`) y optimista (`p90`), además de probabilidades de estrés financiero (`prob_negative_cashflow`, `prob_balance_below_zero`, `prob_Cruzar_Umbral`).
   - En la aplicación web (`web/backend/finances.py`), el asistente permite ejecutar simulaciones interactivas al vuelo ante variaciones de ingreso propuestas por el usuario (`simular_cambio_ingresos`).
