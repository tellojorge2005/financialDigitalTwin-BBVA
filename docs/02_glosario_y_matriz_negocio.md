# 2. Glosario de Términos y Matriz de Negocio — BBVA PRISMA

## 2.1 Glosario Técnico y Financiero

| Término / Campo | Categoría | Definición en BBVA PRISMA |
| :--- | :--- | :--- |
| **Gemelo Digital Financiero (PRISMA)** | Negocio / IA | Representación analítica y predictiva del estado financiero de un cliente construida a partir de sus cuentas, tarjetas, obligaciones y movimientos históricos. |
| **Arquitectura Medallion** | Ingeniería de Datos | Patrón de diseño en tres capas (**Bronze**, **Silver** y **Gold**) que incrementa progresivamente la calidad, estructura y valor analítico del dato en Amazon S3. |
| **Cuarentena (`rejected/`)** | Gobernanza / Calidad | Aislamiento automatizado en S3 de registros que incumplen integridad referencial o reglas de negocio, conservando la causa en la columna `rejection_reason`. |
| **`monthly_net_flow` (Flujo Neto Mensual)** | Finanzas | Resultado mensual exacto: `monthly_income - monthly_expenses - monthly_debt_payments + transfers_in - transfers_out`. |
| **`average_income_3m` / `income_mean_3`** | Finanzas (Gold 3M) | Promedio mensual de ingresos del cliente durante sus últimos 3 meses cerrados con actividad. |
| **`income_std_3m` / `income_std_3`** | Finanzas (Gold 3M) | Desviación estándar muestral del ingreso mensual en los últimos 3 meses; mide la volatilidad o estabilidad de los ingresos del cliente. |
| **`savings_rate_3m` (Ratio de Ahorro 3M)** | Finanzas (Gold 3M) | Proporción del ingreso trimestral que el cliente conserva como ahorro neto (`savings_3m / income_3m`). |
| **`debt_payment_ratio_3m`** | Finanzas (Gold 3M) | Proporción del ingreso de los últimos 3 meses destinada al pago de obligaciones y deuda (`debt_payments_3m / income_3m`). |
| **`expense_to_income_ratio_3m`** | Finanzas (Gold 3M) | Proporción del ingreso trimestral consumida en gastos operativos y de estilo de vida (`expenses_3m / income_3m`). |
| **`income_change_3m`** | Finanzas (Gold 3M) | Variación relativa del ingreso del último mes respecto al promedio de los últimos 3 meses (`(monthly_income - average_income_3m) / average_income_3m`). |
| **`expense_coverage_months`** | Riesgo / Liquidez | Número de meses que el saldo al corte (`closing_balance`) puede cubrir el gasto promedio más pagos de deuda mensuales del cliente. |
| **`financial_health_status`** | Segmentación | Clasificación diagnóstica del cliente en Gold: `margen_positivo`, `sin_margen`, `requiere_atencion`, `sin_ingresos` o `historial_insuficiente`. |
| **Percentiles Monte Carlo (`p10`, `p50`, `p90`)** | Simulación Estocástica | Escenarios pesimista (`p10`), mediano (`p50`) y optimista (`p90`) obtenidos tras 2,000 trayectorias estocásticas de ingresos, gastos y saldo a 1, 2 y 3 meses. |
| **`registro_completo` / `uso_completo`** | Observabilidad IA | Banderas de integridad en los lotes CSV de monitoreo (`history/assistant_interactions/`) que indican si un turno del asistente concluyó con métricas completas de latencia, tokens y costo. |

---

## 2.2 Matriz de Alineación de Negocio y Componentes Técnicos

| Necesidad de Negocio (BBVA) | Pregunta Analítica / Operativa Clave | Indicadores y Columnas (`Gold` / `Monitoreo`) | Fuente en S3 (`.parquet` / `.csv`) | Componente que lo Consume |
| :--- | :--- | :--- | :--- | :--- |
| **1. Estabilidad y tendencia de ingresos del cliente** | ¿El ingreso del cliente es estable o volátil en el último trimestre y cómo varió en el último corte? | `average_income_3m`, `income_std_3m`, `income_variation_ratio_3m`, `income_change_3m`, `predicted_income` | `data/gold/customer_model_summary.parquet` | Web (`web/backend/finances.py`), Power BI (`Clientes.tmdl`) y BI Chat (`bi/chat/`) |
| **2. Capacidad de ahorro y presión de endeudamiento** | ¿Qué porcentaje de su ingreso ahorra el cliente y cuánto compromete en deuda y gasto corriente? | `savings_rate_3m`, `debt_payment_ratio_3m`, `expense_to_income_ratio_3m`, `expense_coverage_months` | `data/gold/customer_financial_health.parquet` y `customer_model_summary.parquet` | Panorama Web (`web/frontend/dashboard.html`), Herramienta `consultar_finanzas` y Power BI |
| **3. Comportamiento transaccional y hábitos de consumo** | ¿En qué categorías concentra el cliente su gasto y cuántas transacciones realiza por mes? | `category`, `total_expenses`, `expense_transactions`, `monthly_transactions`, `average_transaction` | `data/gold/customer_recent_expenses_by_category.parquet` y `customer_monthly_summary.parquet` | Pestaña *Resumen financiero* en Power BI (`Categorias.tmdl`, `Mensual.tmdl`) y Web |
| **4. Prevención de estrés financiero e impago** | ¿Qué clientes presentan mora (`>= 30` días), saldo negativo o déficit presupuestario trimestral? | `has_overdue_payments`, `max_days_past_due`, `installments_30_days_past_due`, `has_budget_deficit_3m`, `financial_health_status` | `data/gold/customer_monthly_summary.parquet` y `customer_model_summary.parquet` | Segmentación de Salud Financiera en Power BI y alertas del Asistente PRISMA |
| **5. Planeación de metas y simulación bajo incertidumbre** | ¿Cuánto tiempo tardará el cliente en alcanzar una meta de ahorro y qué ocurre si su ingreso cambia? | `p10`, `p50`, `p90`, `prob_negative_cashflow`, `prob_balance_below_zero`, `meses_estimados` | `data/gold/monte_carlo_scenarios.parquet` y motor en vivo (`monte_carlo.py`, `goals.py`) | Herramientas LangGraph (`crear_meta_ahorro`, `simular_cambio_ingresos`) en PRISMA Web |
| **6. Observabilidad operativa y control de costos de IA** | ¿Cuál es la latencia promedio, tasa de errores HTTP, uso de herramientas y costo por consulta del asistente? | `duracion_segundos`, `http_status`, `resultado`, `tokens_entrada`, `tokens_salida`, `costo_total_usd`, `usos_*` | `history/assistant_interactions/YYYY/MM/DD/lote_*.csv` | Pestaña *Monitoreo del asistente* en Power BI (`Interacciones.tmdl`) y BI Chat (`bi/chat/`) |
