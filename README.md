<h1 align="center">BBVA PRISMA</h1>
<p align="center">
  <img src="docs/images/bbva_prisma.svg" alt="Logo PRISMA" width="100">
</p>
<p align="center"><strong>Inteligencia para tus finanzas 💵 </strong><br>Gemelo Digital Financiero · Data Experience 🏦</p>

Proyecto desarrollado en el contexto del reto Data Experience de BBVA. Integra datos financieros, procesamiento automatizado y una aplicación web para consultar el panorama de un cliente y explorar sus decisiones mediante el asistente **PRISMA**.

El repositorio conserva el nombre `financialDigitalTwin-BBVA`. **PRISMA** identifica la aplicación y su asistente dentro del prototipo.

## 💬 ¿Qué hace PRISMA?

PRISMA permite consultar ingresos, gastos, deudas e historial financiero en lenguaje natural. También ayuda a definir metas de ahorro y explorar escenarios ante cambios de ingresos, tomando como referencia los datos disponibles y la información que proporciona el usuario.

La aplicación reúne un panorama financiero y un chat con historial temporal. El asistente utiliza herramientas para consultar datos y realizar cálculos en Python; las estimaciones y simulaciones se presentan como referencias, no como resultados garantizados.

## 🚀 Componentes principales

- **ETL:** limpieza, validación, separación de rechazados y consolidación de clientes, tarjetas, transacciones, cuentas y pagos en capas Bronze, Silver y Gold, incluyendo promedios, desviaciones estándar, ratios y variaciones de los últimos tres meses.
- **Automatización:** Airflow encadena Bronze → Silver → Gold. Gold genera los datos base, ejecuta el modelo de ingresos junto con Monte Carlo e integra las predicciones validadas. Monte Carlo se invoca desde `income_model.py`; no necesita un DAG separado.
- **Modelaje y simulación:** XGBoost para estimar ingresos del siguiente mes y Monte Carlo para generar escenarios de ingresos, gastos y saldo a uno, dos y tres meses. El asistente puede consultar la estimación de ingresos cuando está disponible y aprobada por las validaciones del pipeline.
- **Aplicación web:** panorama del cliente, asistente conversacional, metas y escenarios financieros.
- **Monitoreo:** registro de tiempos, errores, uso de herramientas, tokens y costos estimados del asistente, con persistencia local y exportación de lotes CSV a S3.
- **BI y asistente del dashboard:** reporte y modelo semántico en Power BI (`bi/dashboard/`) con métricas de monitoreo del asistente y comportamiento transaccional de los clientes en Gold, acompañado de un asistente analítico local (`bi/chat/`) con guardrails y visualizaciones integradas.

## 🛠️ Tecnologías

- **Datos:** Python, PySpark, pandas, NumPy, Amazon S3 y Parquet.
- **Aplicación:** FastAPI, Pydantic, Uvicorn, HTML, CSS y JavaScript.
- **IA y modelaje:** LangGraph, LangChain, OpenAI API, scikit-learn y XGBoost.
- **Infraestructura:** Airflow, Docker, Docker Compose, PostgreSQL y Oracle Cloud Infrastructure (OCI).
- **Análisis y seguimiento:** Power BI, Jupyter, SQLite, pytest y GitHub Actions (CI/CD).

## 👩‍💻 Ejecución

Ejecutar los comandos desde la raíz del repositorio.

### 1. Configurar el entorno

```bash
conda env create -f environment.yml
conda activate financialDigitalTwin-BBVA
```

Crear `.env` en la raíz tomando [.env.example](.env.example) como referencia. Completar de forma local la configuración de los servicios, la sesión y el monitoreo con los valores del entorno de ejecución. No publicar credenciales ni detalles internos de conexión.

La ejecución local de PySpark requiere Java; las imágenes Docker incluyen su propio entorno.

### 2. Ejecutar el ETL

Con Docker abierto y `.env` configurado:

```bash
docker compose --env-file .env -f docker/docker-compose.yml up --build -d
```

Preparar los datos de entrada de clientes, tarjetas, transacciones, cuentas y pagos en el entorno configurado para el ETL.

Abrir [Airflow](http://localhost:8080), habilitar los DAGs de la cadena y ejecutar `bronze_cleaning_pipeline` para una corrida manual. Bronze dispara Silver y Silver dispara Gold; no es necesario ejecutar cada etapa por separado.

Bronze está programado diariamente a las **00:00, hora de Ciudad de México**. Para la ejecución automática, Docker y Airflow deben permanecer activos. El estado del worker puede consultarse en [Spark Master](http://localhost:8081).

### 3. Abrir PRISMA

Con los datos procesados disponibles y la configuración local completa:

```bash
uvicorn web.backend.main:app --reload
```

Abrir [PRISMA](http://localhost:8000) e ingresar el correo registrado de un cliente. Este acceso es demostrativo y no constituye autenticación bancaria.

### 4. Consultar Power BI y el asistente del dashboard

Para iniciar el asistente analítico local del dashboard en [http://127.0.0.1:8001](http://127.0.0.1:8001):

```bash
uvicorn bi.chat.backend.main:app --port 8001
```

Abrir [financialDigitalTwin.pbip](bi/dashboard/financialDigitalTwin.pbip) en Power BI Desktop. Configurar las conexiones de forma local para consultar el comportamiento transaccional de los clientes en Gold, las métricas de monitoreo de interacciones y el chat embebido del dashboard.

## 📓 Notebooks y experimentación

Los [notebooks](notebooks/) conservan la evolución del proyecto: exploración, limpieza, análisis Gold, asistente inicial y experimentos de modelaje y observabilidad. Se mantienen juntos y no son necesarios para ejecutar la cadena de Airflow. Sus rutas relativas deben resolverse desde `notebooks/`.

La estimación exploratoria de `credit_score` del notebook de modelaje es distinta del modelo de ingresos integrado en el ETL.

## 🧪 Pruebas, CI y CD

Con las dependencias y Java disponibles, ejecutar las pruebas indicando explícitamente el orden Bronze → Silver → Gold → Monte Carlo:

```bash
python -m pytest tests/test_bronze_cleaning.py tests/test_silver_cleaning.py tests/test_gold_analysis.py tests/test_monte_carlo.py -v
```

- **Integración continua (CI):** el [workflow de CI](.github/workflows/ci.yml) ejecuta esos mismos cuatro archivos, en ese orden, en pull requests dirigidos a `main` o mediante ejecución manual. Utiliza Python 3.11 y Java 17 en Ubuntu.
- **Despliegue continuo (CD):** el [workflow de CD](.github/workflows/cd.yml) empaqueta y despliega automáticamente la aplicación web (`web/`) en una instancia de Oracle Cloud Infrastructure (OCI) al integrar cambios en `main` o mediante ejecución manual, gestionando el servicio con `systemd`.

Las pruebas usan datos de prueba y rutas temporales. Cubren procesamiento e integración de predicciones en Gold, además de casos de simulación Monte Carlo; no ejecutan el entrenamiento completo de XGBoost ni validan el asistente web, Airflow o el acceso al S3 real.

## 📊 Datos y alcance

El proyecto parte de datos públicos de [Financial Transactions Dataset: Analytics](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets), adaptados para las prácticas del prototipo. Las muestras del repositorio están en [data/sample](data/sample/).

PRISMA es un prototipo, no un servicio bancario de producción. Las predicciones y los escenarios dependen de datos históricos y supuestos; los costos del asistente son estimaciones basadas en el uso registrado y las tarifas configuradas.
