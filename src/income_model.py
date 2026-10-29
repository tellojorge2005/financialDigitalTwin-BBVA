import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

# Cargar configuración sin depender del notebook
PROJECT_PATH = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_PATH / ".env", override=False)


# Entrenar con separación temporal y comparar con el último ingreso
# El modelo se ejecuta en el driver; Spark lee y escribe los Parquet de S3.
def entrenar_ingresos(mensual):
    mensual = mensual.copy()
    mensual["month"] = pd.to_datetime(
        mensual["month"]
    )

    mensual = mensual.sort_values(
        ["client_id", "currency", "month"]
    ).reset_index(drop=True)

    # Validar claves

    if mensual.duplicated(
            ["client_id", "currency", "month"]
    ).any():
        raise ValueError(
            "Existen resúmenes mensuales duplicados."
        )

    if mensual["currency"].nunique() != 1:
        raise ValueError(
            "Entrena un modelo por moneda."
        )

    # Definir variables predictoras

    variables_ingreso = [
        "monthly_income",
        "monthly_expenses",
        "monthly_debt_payments",
        "monthly_savings",
        "monthly_transactions",
        "transfers_in",
        "transfers_out",
        "closing_balance",
        "income_lag_1",
        "income_lag_3",
        "income_mean_3",
        "income_std_3",
        "expenses_mean_3",
        "negative_balance_months_3",
        "outstanding_due",
        "max_days_past_due",
        "month_number"
    ]

    mensual["month_number"] = mensual["month"].dt.month

    for columna in variables_ingreso:
        mensual[columna] = pd.to_numeric(
            mensual[columna],
            errors="coerce"
        )

    # Validar valores necesarios para entrenar y comparar con el último ingreso
    if mensual[["client_id", "currency", "month", "monthly_income"]].isna().any().any():
        raise ValueError("Faltan claves o ingresos mensuales.")

    if np.isinf(mensual[variables_ingreso].to_numpy(dtype=float)).any():
        raise ValueError("Hay valores infinitos en las variables predictoras.")

    if (mensual["monthly_income"] < 0).any():
        raise ValueError("El ingreso mensual no puede ser negativo.")

    # Crear objetivo del siguiente mes

    grupos = mensual.groupby(
        ["client_id", "currency"],
        sort=False
    )

    mensual["target_month"] = grupos["month"].shift(-1)
    mensual["next_month_income"] = grupos["monthly_income"].shift(-1)

    siguiente_mes = mensual["month"] + pd.offsets.MonthBegin(1)

    entrenamiento = mensual.loc[
        (mensual["history_months"] >= 4)
        & mensual["target_month"].eq(siguiente_mes)
        & mensual["next_month_income"].notna()
        ].copy()

    # Separar períodos

    periodos = sorted(
        entrenamiento["target_month"].unique()
    )

    if len(periodos) < 10:
        raise ValueError(
            "Se requieren más períodos para separar "
            "entrenamiento, validación y prueba."
        )

    inicio_validacion = periodos[-6]
    inicio_prueba = periodos[-3]

    train = entrenamiento.loc[
        entrenamiento["target_month"] < inicio_validacion
        ]

    validacion = entrenamiento.loc[
        (entrenamiento["target_month"] >= inicio_validacion)
        & (entrenamiento["target_month"] < inicio_prueba)
        ]

    prueba = entrenamiento.loc[
        entrenamiento["target_month"] >= inicio_prueba
        ]

    # Comprobar que los conjuntos permiten calcular las métricas
    for nombre, tabla in [("train", train), ("validacion", validacion), ("prueba", prueba)]:
        if len(tabla) < 2:
            raise ValueError(f"Datos insuficientes en {nombre}.")

    # Entrenar modelo

    modelo_ingreso = XGBRegressor(
        objective="reg:squarederror",
        eval_metric="mae",
        n_estimators=500,
        max_depth=3,
        learning_rate=0.03,
        min_child_weight=10,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=10,
        tree_method="hist",
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=4
    )

    modelo_ingreso.fit(
        train[variables_ingreso],
        train["next_month_income"],
        eval_set=[
            (
                validacion[variables_ingreso],
                validacion["next_month_income"]
            )
        ],
        verbose=False
    )

    # Comparar modelos

    resultados_ingreso = []

    for periodo, tabla in [
        ("Validación", validacion),
        ("Prueba", prueba)
    ]:
        real = tabla["next_month_income"]

        predicciones = {
            "Último ingreso": tabla["monthly_income"].to_numpy(),
            "XGBoost": np.maximum(
                modelo_ingreso.predict(tabla[variables_ingreso]),
                0
            )
        }

        for nombre, pred in predicciones.items():
            resultados_ingreso.append({
                "periodo": periodo,
                "modelo": nombre,
                "MAE": float(mean_absolute_error(real, pred)),
                "RMSE": float(
                    np.sqrt(mean_squared_error(real, pred))
                ),
                "R2": float(r2_score(real, pred))
            })

    metricas_ingreso = pd.DataFrame(resultados_ingreso)

    print(metricas_ingreso.round(3).to_string(index=False))

    # Seleccionar método utilizando validación

    validacion_metricas = metricas_ingreso.loc[
        metricas_ingreso["periodo"] == "Validación"
        ].set_index("modelo")

    usar_xgboost = (
            validacion_metricas.loc["XGBoost", "MAE"]
            < validacion_metricas.loc["Último ingreso", "MAE"]
    )

    # Preparar último mes disponible

    ultimos = (
        mensual
        .sort_values("month")
        .groupby(["client_id", "currency"], as_index=False)
        .tail(1)
        .copy()
    )

    # Reentrenar para predecir el próximo período

    if usar_xgboost:
        parametros_finales = modelo_ingreso.get_params()

        parametros_finales.update({
            "n_estimators": int(modelo_ingreso.best_iteration) + 1,
            "early_stopping_rounds": None
        })

        modelo_ingreso_final = XGBRegressor(
            **parametros_finales
        )

        modelo_ingreso_final.fit(
            entrenamiento[variables_ingreso],
            entrenamiento["next_month_income"]
        )

        ingreso_estimado = np.maximum(
            modelo_ingreso_final.predict(
                ultimos[variables_ingreso]
            ),
            0
        )

    else:
        ingreso_estimado = ultimos["monthly_income"].to_numpy()

    # Guardar predicciones

    predicciones_ingreso = ultimos[
        ["client_id", "currency", "month"]
    ].copy()

    predicciones_ingreso = predicciones_ingreso.rename(
        columns={"month": "feature_month"}
    )

    predicciones_ingreso["prediction_month"] = (
            predicciones_ingreso["feature_month"]
            + pd.offsets.MonthBegin(1)
    )

    predicciones_ingreso["predicted_income"] = ingreso_estimado

    predicciones_ingreso["method"] = (
        "xgboost" if usar_xgboost else "last_month"
    )

    # Devolver resultados para guardarlos mediante Spark
    return predicciones_ingreso, metricas_ingreso


def run_pipeline(spark, gold_path):
    # Leer únicamente la información necesaria para el modelaje
    columnas = [
        "client_id", "currency", "month", "history_months",
        "monthly_income", "monthly_expenses", "monthly_debt_payments",
        "monthly_savings", "monthly_transactions", "transfers_in",
        "transfers_out", "closing_balance", "income_lag_1",
        "income_lag_3", "income_mean_3", "income_std_3",
        "expenses_mean_3", "negative_balance_months_3",
        "outstanding_due", "max_days_past_due", "month_number"
    ]
    mensual = (
        spark.read.parquet(f"{gold_path}/customer_monthly_summary.parquet")
        .select(*columnas)
        .toPandas()
    )

    predicciones, metricas = entrenar_ingresos(mensual)

    # Usar fechas explícitas para evitar TIMESTAMP(NANOS) de pandas
    filas = [
        (
            int(fila.client_id),
            str(fila.currency),
            fila.feature_month.date(),
            fila.prediction_month.date(),
            float(fila.predicted_income),
            str(fila.method)
        )
        for fila in predicciones.itertuples(index=False)
    ]

    predictions_spark = spark.createDataFrame(
        filas,
        """
        client_id long, currency string, feature_month date,
        prediction_month date, predicted_income double, method string
        """
    )

    # Conservar las métricas en S3; no usar la base de metadatos de Airflow
    filas_metricas = [
        (
            str(fila.periodo), str(fila.modelo),
            float(fila.MAE), float(fila.RMSE), float(fila.R2)
        )
        for fila in metricas.itertuples(index=False)
    ]
    metrics_spark = spark.createDataFrame(
        filas_metricas,
        "periodo string, modelo string, MAE double, RMSE double, R2 double"
    )
    metrics_spark.write.mode("overwrite").parquet(
        f"{gold_path}/income_model_metrics.parquet"
    )
    predictions_spark.write.mode("overwrite").parquet(
        f"{gold_path}/income_predictions.parquet"
    )
    print("Predicciones de ingresos guardadas en GOLD")


def main():
    parser = argparse.ArgumentParser(description="Modelaje automático de ingresos")
    parser.add_argument("--gold-path", default=None)
    args = parser.parse_args()

    bucket = os.getenv("S3_BUCKET", "").strip()
    gold_path = args.gold_path

    if not gold_path:
        if not bucket:
            raise RuntimeError("Configura S3_BUCKET.")
        gold_path = f"s3a://{bucket}/gold"

    spark = (
        SparkSession.builder
        .appName("FinancialDigitalTwin_Income")
        .getOrCreate()
    )
    try:
        run_pipeline(spark, gold_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

