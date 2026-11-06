import numpy as np
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Simular ingresos, gastos y saldo de los próximos tres meses
def simular_historial(historial, meses=3, escenarios=10000, semilla=42):
    if type(meses) is not int or not 1 <= meses <= 3:
        raise ValueError("El horizonte debe estar entre uno y tres meses.")
    if type(escenarios) is not int or not 100 <= escenarios <= 100000:
        raise ValueError("Se requieren entre 100 y 100000 escenarios.")

    datos = historial.copy()
    datos["month"] = pd.to_datetime(datos["month"])
    claves = ["client_id", "currency", "month"]
    if datos[claves].isna().any().any() or datos.duplicated(claves).any():
        raise ValueError("El historial contiene claves nulas o duplicadas.")
    if datos.empty or set(datos["currency"]) != {"MXN"}:
        raise ValueError("Se esperaba historial en MXN.")

    importes = [
        "monthly_income", "monthly_expenses",
        "monthly_debt_payments", "closing_balance"
    ]
    datos[importes] = datos[importes].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(datos[importes].to_numpy(dtype=float)).all():
        raise ValueError("El historial contiene importes no finitos.")
    if (datos[importes[:3]] < 0).any().any():
        raise ValueError("Los ingresos, gastos y pagos deben ser no negativos.")

    filas = []
    for (cliente_id, moneda), grupo in datos.groupby(["client_id", "currency"], sort=True):
        grupo = grupo.sort_values("month").tail(12)
        fechas = pd.DatetimeIndex(grupo["month"])
        esperadas = pd.date_range(fechas[0], periods=len(fechas), freq="MS")
        if not fechas.equals(esperadas):
            raise ValueError("El historial mensual debe ser continuo e iniciar el día uno.")

        corte = grupo.iloc[-1]["month"]
        saldo = float(grupo.iloc[-1]["closing_balance"])
        ingreso = float(grupo["monthly_income"].mean())
        gasto = float(grupo["monthly_expenses"].mean())
        pago = float(grupo["monthly_debt_payments"].mean())
        suficiente = len(grupo) >= 6

        # Estimar la variación con un máximo de doce meses observados
        variacion_ingreso = (
            float(grupo["monthly_income"].std(ddof=1) / ingreso)
            if suficiente and ingreso > 0 else 0.0
        )
        variacion_gasto = (
            float(grupo["monthly_expenses"].std(ddof=1) / gasto)
            if suficiente and gasto > 0 else 0.0
        )

        if suficiente:
            # Mantener resultados reproducibles por cliente
            rng = np.random.default_rng(np.random.SeedSequence([semilla, int(cliente_id)]))

            def generar(media, variacion):
                sigma = np.sqrt(np.log1p(variacion ** 2))
                return media * rng.lognormal(
                    mean=-0.5 * sigma ** 2, sigma=sigma,
                    size=(escenarios, meses)
                )

            ingresos = generar(ingreso, variacion_ingreso)
            gastos = generar(gasto, variacion_gasto)
            saldos = saldo + np.cumsum(ingresos - gastos - pago, axis=1)

        for mes in range(1, meses + 1):
            fila = {
                "client_id": int(cliente_id),
                "currency": moneda,
                "feature_month": corte.date(),
                "prediction_month": (corte + pd.offsets.MonthBegin(mes)).date(),
                "horizon_months": mes,
                "history_months_used": len(grupo),
                "status": "GENERADO" if suficiente else "HISTORIAL_INSUFICIENTE",
                "method": "monte_carlo_lognormal",
                "scenarios": escenarios if suficiente else 0,
                "seed": semilla,
                "base_income": ingreso,
                "base_expenses": gasto,
                "base_debt_payment": pago,
                "initial_balance": saldo,
                "income_variation": variacion_ingreso if suficiente else None,
                "expense_variation": variacion_gasto if suficiente else None,
                "prob_negative_balance": None,
                "assumptions": (
                    "Lognormal; independencia entre meses e ingresos/gastos; "
                    "pago de deuda constante; sin transferencias futuras. "
                    "Escenarios sin validación predictiva."
                )
            }
            for nombre in ["income", "expenses", "balance"]:
                for percentil in [10, 50, 90]:
                    fila[f"{nombre}_p{percentil}"] = None

            if suficiente:
                for nombre, valores in [
                    ("income", ingresos), ("expenses", gastos), ("balance", saldos)
                ]:
                    for percentil in [10, 50, 90]:
                        fila[f"{nombre}_p{percentil}"] = float(
                            np.quantile(valores[:, mes - 1], percentil / 100)
                        )
                fila["prob_negative_balance"] = float((saldos[:, mes - 1] < 0).mean())
            filas.append(fila)

    return pd.DataFrame(filas)


def run_pipeline(spark, gold_path):
    # Leer únicamente los últimos doce meses para cada cliente
    orden = Window.partitionBy("client_id", "currency").orderBy(F.col("month").desc())
    mensual = (
        spark.read.parquet(f"{gold_path}/customer_monthly_summary.parquet")
        .withColumn("_position", F.row_number().over(orden))
        .filter(F.col("_position") <= 12)
        .select(
            "client_id", "currency", "month", "monthly_income",
            "monthly_expenses", "monthly_debt_payments", "closing_balance"
        )
        .toPandas()
    )
    resultados = simular_historial(mensual)

    esquema = """
        client_id long, currency string, feature_month date,
        prediction_month date, horizon_months int, history_months_used int,
        status string, method string, scenarios int, seed long,
        base_income double, base_expenses double, base_debt_payment double,
        initial_balance double, income_variation double, expense_variation double,
        prob_negative_balance double, assumptions string,
        income_p10 double, income_p50 double, income_p90 double,
        expenses_p10 double, expenses_p50 double, expenses_p90 double,
        balance_p10 double, balance_p50 double, balance_p90 double
    """
    # Conservar nulos explícitos cuando el historial no permite simular
    filas = [
        tuple(None if pd.isna(valor) else valor for valor in fila)
        for fila in resultados.itertuples(index=False, name=None)
    ]
    salida = spark.createDataFrame(filas, esquema).withColumn(
        "generated_at", F.current_timestamp()
    )
    salida.write.mode("overwrite").parquet(
        f"{gold_path}/customer_cashflow_scenarios.parquet"
    )
    print("Escenarios de uno, dos y tres meses guardados en GOLD")
