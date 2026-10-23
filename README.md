# financialDigitalTwin-BBVA
**💵 Data Experience — Gemelo Digital Financiero**

Proyecto académico desarrollado en el contexto del reto Data Experience de BBVA. Integra información de clientes, tarjetas y transacciones para construir un perfil financiero que pueda consultarse mediante visualizaciones y un asistente de inteligencia artificial.

Combina ingeniería de datos, una aplicación web y pruebas de Machine Learning y simulación para explorar la situación financiera de los clientes.

## 🚀 Funciones principales

- **Procesamiento de datos:** Pipeline organizado en capas Bronze, Silver y Gold para limpiar, transformar y consolidar información financiera.
- **Calidad de datos:** Validación de registros, eliminación de duplicados, separación de rechazados e imputación de valores faltantes.
- **Perfil financiero:** Generación de indicadores y resúmenes por cliente a partir de sus datos, tarjetas y transacciones.
- **Asistente de IA:** Consulta en lenguaje natural de la información financiera del cliente mediante una interfaz web.
- **Visualización:** Reporte y modelo semántico en Power BI para explorar los datos.
- **Modelaje y simulación:** Pruebas con XGBoost para estimar el score crediticio y Monte Carlo para generar escenarios financieros.
- **Observabilidad:** Registro de métricas, resultados, errores y alertas de las pruebas de modelaje en SQLite.

Las pruebas de modelaje se ejecutan en notebooks y todavía no están integradas al asistente web. Las simulaciones representan escenarios basados en supuestos.

## 🖥️ Stack tecnológico principal

- **Procesamiento y análisis:** Python, PySpark, pandas y NumPy.
- **Almacenamiento:** Amazon S3 y archivos Parquet.
- **Orquestación e infraestructura:** Apache Airflow, Docker y Docker Compose; PostgreSQL para los metadatos de Airflow.
- **Backend:** FastAPI, Pydantic y Uvicorn.
- **Frontend:** HTML, CSS y JavaScript.
- **Inteligencia artificial:** LangGraph, LangChain y OpenAI API.
- **Machine Learning:** scikit-learn y XGBoost.
- **Visualización y experimentación:** Power BI y Jupyter Notebooks.
- **Observabilidad y pruebas:** SQLite, logging, pytest y GitHub Actions.

## 👩‍💻 Ejecución

### Configurar el entorno

```bash
conda env create -f environment.yml
conda activate financialDigitalTwin-BBVA
```

Crear un archivo `.env` en la raíz utilizando [.env.example](.env.example) como referencia. Configurar el acceso a AWS, la ubicación de los datos en S3, OpenAI y las variables de sesión.

El entorno local incluye dependencias de Windows. Para ejecutar PySpark se requiere Java.

### Procesamiento local

Ejecutar los notebooks en el siguiente orden:

1. `02_bronze_cleaning.ipynb`
2. `03_silver_cleaning.ipynb`
3. `04_gold_analysis.ipynb`

Los notebooks `05_customer_assistant.ipynb` y `06_modelaje_observabilidad.ipynb` utilizan los resultados de Gold. Revisar que las rutas relativas se ejecuten desde `notebooks/`.

### Aplicación web

Con los datos Gold disponibles en S3:

```bash
uvicorn backend.main:app --reload
```

Abrir [localhost:8000](http://localhost:8000). El acceso por ID de cliente es parte de la demostración.

### Airflow y Spark

Con Docker disponible y el archivo `.env` configurado:

```bash
docker compose --env-file .env -f docker/docker-compose.yml up --build -d
```

Abrir [localhost:8080](http://localhost:8080) y activar `bronze_cleaning_pipeline`. Los CSV de entrada deben estar en el prefijo `incoming/` del bucket configurado.

### Power BI

Abrir [financialDigitalTwin.pbip](bi/financialDigitalTwin.pbip) en Power BI Desktop y ajustar las conexiones a los archivos Parquet.

## 📊 Datos y alcance

Se utilizan datos públicos de [Financial Transactions Dataset: Analytics](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets), con modificaciones para realizar prácticas de limpieza y validación.

El proyecto es un prototipo académico en desarrollo. La estimación del score y los escenarios financieros son pruebas exploratorias.

Las credenciales, los archivos Parquet generados y la base SQLite se mantienen fuera del control de versiones.