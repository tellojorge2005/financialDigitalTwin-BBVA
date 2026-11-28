# 3. Políticas de Privacidad, Seguridad y Gobernanza — BBVA PRISMA

## 3.1 Minimización y Enmascaramiento de Datos Sensibles (PII / PCI) en el ETL

El pipeline Medallion aplica privacidad por diseño desde la transformación hacia la capa **Gold** (`etl/src/gold_analysis.py`) para garantizar que ningún artefacto analítico, modelo o asistente exponga secretos financieros:

| Entidad Origen                                    | Campos Sensibles en Origen (`Raw` / `Silver`)                                      | Política Aplicada en `Gold` y Consumo (`web/` y `bi/`)                                                                                                                                                                                                                                                                                             |
| :------------------------------------------------ | :--------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Tarjetas (`cards_data.csv`)**                   | `card_number` (PAN), `cvv`, `expires`, `year_pin_last_changed`, `card_on_dark_web` | **Eliminación total en Gold (`create_cards_summary`):** las tablas Gold solo conservan métricas agregadas por `client_id` (`total_cards`, `total_card_brands`, `total_card_types`, `average_credit_limit`, `total_credit_limit`). Jamás se expone `card_number` ni `cvv`.                                                                          |
| **Usuarios (`users_data.csv`)**                   | `address` (domicilio exacto), `latitude`, `longitude`, `birth_year`, `birth_month` | **Reducción de granularidad (`create_users_summary`):** se descartan dirección postal exacta, coordenadas GPS y fecha de nacimiento en `customer_model_summary.parquet`, reteniendo solo agregados geográficos (`city`, `state`, `country`) y `current_age`.                                                                                       |
| **Interacciones del Asistente (`monitoring.py`)** | Correo del cliente, `client_id`, texto del prompt y texto de la respuesta del LLM  | **Anonimización estricta en telemetría (`web/backend/monitoring.py`):** los lotes CSV exportados a S3 (`history/assistant_interactions/`) registran únicamente UUIDs aleatorios (`interaccion_id`, `conversacion_id`), tiempos, códigos HTTP, conteo de herramientas, tokens y costo en USD. No almacenan mensajes ni identificadores de clientes. |

---

## 3.2 Seguridad de Sesión y Aislamiento por Cliente en la Aplicación Web (`web/`)

1. **Cookies de Sesión Firmadas (`web/backend/main.py` y `config.py`):**
   - La sesión se administra mediante `SessionMiddleware` (Starlette/`itsdangerous`) firmada criptográficamente con `SESSION_SECRET` (`>= 32` caracteres validados al arranque en `config.py`), `same_site="lax"` y bandera `https_only` controlada por `COOKIE_SECURE`.
2. **Aislamiento de Contexto por `client_id` (`web/backend/data.py` y `assistant.py`):**
   - El cliente autenticado en sesión solo puede consultar filas asociadas a su propio `id` en memoria/S3.
   - Las herramientas de LangGraph (`consultar_finanzas`, `consultar_historial`, `crear_meta_ahorro`, `simular_cambio_ingresos`, `cancelar_flujo_activo`) reciben el `client_id` inyectado desde el estado del servidor, nunca como parámetro libre del prompt del usuario.
3. **Control de Concurrencia por Conversación (`web/backend/turns.py`):**
   - Evita condiciones de carrera o inyecciones simultáneas bloqueando turnos concurrentes sobre la misma sesión (`conversacion_ocupada`, HTTP `409`).

---

## 3.3 Guardrails de Entrada y Salida en los Asistentes de IA

El proyecto implementa dos capas independientes de **Guardrails** deterministas (con normalización Unicode `NFKC` y eliminación de caracteres invisibles de control `Cf` para evitar evasiones):

### A. Guardrails del Asistente de Clientes (`web/backend/guardrails.py`)

- **Filtro de Entrada (`entrada_restringida`):** bloquea consultas sobre arquitectura interna (`xgboost`, `monte carlo`, `langgraph`, `spark`, `airflow`, `s3`, `parquet`, `docker`, rutas de sistema `/opt/`, `s3a://`), intentos de _prompt injection_ (`ignora las instrucciones`, `revela el prompt`), codificaciones (`base64`, `rot13`, `hexadecimal`) y solicitudes de información de otros clientes.
- **Filtro de Salida (`proteger_respuesta`):** inspecciona el texto generado por el LLM antes de entregarlo al navegador; si detecta coincidencias con variables de entorno (`OPENAI_API_KEY`, `SESSION_SECRET`, `S3_BUCKET`), nombres internos de columnas (`snake_case`) o rutas de S3, reemplaza inmediatamente la salida por el mensaje seguro `RECHAZO`.

### B. Guardrails del Asistente Ejecutivo de BI (`bi/chat/backend/guardrails.py`)

- **Filtro de Entrada y Salida (`entrada_restringida_bi`, `proteger_respuesta_bi`):**
  - Bloquea solicitudes o filtraciones de datos PCI/PII (`CVV`, `CVC`, `NIP`, `PIN`, `CLABE`, patrones de 16 dígitos de tarjeta `\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b`, `CURP`, `RFC`, `SSN`, correos o teléfonos individuales de clientes) y secretos (`sk-...`, `AKIA...`, `OPENAI_API_KEY`).
  - El modelo opera únicamente sobre resúmenes preagregados de monitoreo y Gold mediante esquema estructurado JSON (`with_structured_output`), sin acceso a filas individuales de clientes.

---

## 3.4 Gestión de Secretos y Despliegue Seguro (CI/CD y OCI)

- **Variables de Entorno (`.env` y `.gitignore`):** todas las llaves de AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), OpenAI (`OPENAI_API_KEY`), rutas de S3 y secretos de sesión se leen exclusivamente desde `.env` sin valores sensibles hardcodeados en el código fuente.
- **Despliegue Automatizado a OCI (`.github/workflows/cd.yml`):** el pipeline inyecta los secretos desde **GitHub Encrypted Secrets**, restringe permisos del archivo en el servidor con `chmod 600 .env` y ejecuta el servicio `prisma-web.service` bajo un usuario sin privilegios de root mediante `systemd`.
