from datetime import date
from decimal import Decimal
import math

import numpy as np


def numero(valor):
    if isinstance(valor, bool) or valor is None:
        return None
    try:
        resultado = Decimal(str(valor))
        return resultado if resultado.is_finite() else None
    except Exception:
        return None


def cantidad(valor):
    return float(round(valor, 2)) if valor is not None else None


def periodo_continuo(filas):
    meses = [int(f["month"][:4]) * 12 + int(f["month"][5:7]) for f in filas]
    return bool(meses) and meses == list(range(meses[0], meses[0] + len(meses)))


# Revisar conjuntamente flujo mensual, saldo, deuda y vencimientos
def evaluar_finanzas(perfil, historial):
    filas = historial[-3:]
    reporte = {
        "Moneda": perfil.get("currency") or "MXN",
        "Fecha de consulta": date.today().isoformat(),
        "Fecha del perfil": perfil.get("financial_cutoff_date"),
        "Período observado": [f["month"] for f in filas],
        "Meses completos disponibles": len(filas),
        "Nombre": perfil.get("full_name"),
        "Deuda total registrada": perfil.get("total_debt"),
        "Ingreso anual registrado": perfil.get("yearly_income"),
        "Puntuación crediticia registrada": perfil.get("credit_score"),
        "Cantidad de tarjetas registradas": perfil.get("total_cards"),
        "Límite de crédito total registrado": perfil.get("total_credit_limit"),
        "Los límites de crédito incluyen estimaciones": perfil.get("has_imputed_credit_limit"),
        "Cantidad de límites de crédito estimados": perfil.get("total_credit_limits_imputed"),
        "Saldo de cuenta registrado": perfil.get("account_balance"),
        "Compromisos vencidos pendientes": perfil.get("outstanding_due"),
        "Mayor atraso en días": perfil.get("max_days_past_due"),
        "Alertas": [],
        "Información pendiente": [],
        "Nota": "El saldo no es ahorro libre. La deuda total no es una cuota mensual.",
    }
    alertas = reporte["Alertas"]
    faltantes = reporte["Información pendiente"]
    for campo, etiqueta in [
        ("total_debt", "Deuda total"), ("account_balance", "Saldo disponible"),
        ("outstanding_due", "Compromisos vencidos"), ("max_days_past_due", "Días de atraso"),
    ]:
        if numero(perfil.get(campo)) is None:
            faltantes.append(etiqueta)
    if (numero(perfil.get("outstanding_due")) or 0) > 0:
        alertas.append("Hay compromisos vencidos que no se descuentan de nuevo automáticamente; revisar su regularización.")
    if (numero(perfil.get("max_days_past_due")) or 0) > 0:
        alertas.append("Hay atraso registrado en pagos.")
    if (numero(perfil.get("account_balance")) or 0) < 0:
        alertas.append("El saldo registrado es negativo.")
    if len(filas) < 3 or not periodo_continuo(filas):
        faltantes.append("Tres meses completos y consecutivos para evaluar una referencia estable")
    if not filas:
        reporte["Estado orientativo"] = "Información insuficiente"
        return reporte

    campos = ["monthly_income", "monthly_expenses", "monthly_debt_payments"]
    if any(numero(f.get(c)) is None for f in filas for c in campos):
        faltantes.append("Ingresos, gastos y pagos mensuales completos")
        reporte["Estado orientativo"] = "Información insuficiente"
        return reporte
    ingresos, gastos, pagos = [sum(numero(f[c]) for f in filas) for c in campos]
    margen = ingresos - gastos - pagos
    promedio_ingreso = ingresos / len(filas)
    promedio_gasto = gastos / len(filas)
    ultimo_ingreso = numero(filas[-1]["monthly_income"])
    ultimo_gasto = numero(filas[-1]["monthly_expenses"])
    reporte.update({
        "Ingreso total del período": cantidad(ingresos),
        "Gasto total del período sin deuda": cantidad(gastos),
        "Pagos de deuda del período": cantidad(pagos),
        "Margen del período después de pagos de deuda": cantidad(margen),
        "Ingreso mensual promedio": cantidad(promedio_ingreso),
        "Gasto mensual promedio sin deuda": cantidad(promedio_gasto),
        "Pago mensual promedio de deuda": cantidad(pagos / len(filas)),
        "Pago de deuda del último mes": filas[-1]["monthly_debt_payments"],
        "Margen mensual promedio después de deuda": cantidad(margen / len(filas)),
        "Desviación estándar de ingresos en el período": perfil.get("income_std_3m"),
        "Desviación estándar de gastos en el período": perfil.get("expenses_std_3m"),
        "Proporción de ingreso destinada a gastos sin deuda": float(gastos / ingresos) if ingresos > 0 else None,
        "Proporción de ingreso destinada a deuda": float(pagos / ingresos) if ingresos > 0 else None,
        "Proporción del ingreso que queda después de gastos y deuda": float(margen / ingresos) if ingresos > 0 else None,
        "Ratio de ingreso del último mes frente al promedio": (
            perfil.get("income_to_average_ratio_3m")
            if perfil.get("income_to_average_ratio_3m") is not None
            else float(round(ultimo_ingreso / promedio_ingreso, 4)) if promedio_ingreso > 0 else None
        ),
        "Cambio de ingreso frente al promedio del período": (
            perfil.get("income_change_3m")
            if perfil.get("income_change_3m") is not None
            else float(round((ultimo_ingreso - promedio_ingreso) / promedio_ingreso, 4)) if promedio_ingreso > 0 else None
        ),
        "Cambio de gasto frente al promedio del período": (
            perfil.get("expenses_change_3m")
            if perfil.get("expenses_change_3m") is not None
            else float(round((ultimo_gasto - promedio_gasto) / promedio_gasto, 4)) if promedio_gasto > 0 else None
        ),
        "Cambio en proporción de ahorro frente al período": perfil.get("savings_rate_change_3m"),
        "Cambio en proporción de gasto frente al período": perfil.get("expense_ratio_change_3m"),
        "Cambio en proporción de pago de deuda frente al período": perfil.get("debt_payment_ratio_change_3m"),
    })
    saldo = numero(filas[-1].get("closing_balance"))
    reporte["Meses de gastos y pagos cubiertos por el saldo"] = (
        cantidad(max(saldo, Decimal(0)) / ((gastos + pagos) / len(filas)))
        if saldo is not None and gastos + pagos > 0 else None
    )
    if margen < 0:
        alertas.append("Los gastos y pagos de deuda superan los ingresos del período.")
    if ingresos <= 0:
        alertas.append("No se registran ingresos positivos en el período.")
    if ingresos > 0:
        valores = [float(numero(f["monthly_income"])) for f in filas]
        reporte["Variación relativa observada de ingresos"] = float(np.std(valores) / np.mean(valores))
    if (numero(perfil.get("total_debt")) or 0) > 0 and pagos == 0:
        faltantes.append("Existe deuda pero no pagos registrados: confirmar la próxima cuota antes de asumir que es cero")
    reporte["Estado orientativo"] = (
        "Requiere revisar compromisos" if alertas else
        "Información insuficiente" if faltantes else
        "Margen positivo de referencia" if margen > 0 else "Sin margen de referencia"
    )
    reporte["Nota temporal"] = (
        "Usar las fechas indicadas; estos registros no garantizan que el presupuesto siga igual hoy."
    )
    return reporte


# Reutilizar la simulación del proyecto sin levantar Spark en una petición web
def simular_proyecciones(cliente_id, historial):
    filas = historial[-12:]
    if len(filas) < 6 or not periodo_continuo(filas):
        return []
    campos = ["monthly_income", "monthly_expenses", "monthly_debt_payments", "closing_balance"]
    if any(numero(f.get(c)) is None for f in filas for c in campos):
        return []
    datos = np.array([[float(numero(f[c])) for c in campos] for f in filas])
    if not np.isfinite(datos).all() or (datos[:, :3] < 0).any():
        return []
    ingreso, gasto, pago = datos[:, :3].mean(axis=0)
    saldo = datos[-1, 3]
    rng = np.random.default_rng(np.random.SeedSequence([42, cliente_id]))

    def generar(media, observaciones):
        variacion = np.std(observaciones, ddof=1) / media if media > 0 else 0.0
        sigma = np.sqrt(np.log1p(variacion ** 2))
        return media * rng.lognormal(-0.5 * sigma ** 2, sigma, size=(10000, 3))

    ingresos = generar(ingreso, datos[:, 0])
    gastos = generar(gasto, datos[:, 1])
    saldos = saldo + np.cumsum(ingresos - gastos - pago, axis=1)
    corte = date.fromisoformat(filas[-1]["month"])
    resultado = []
    for horizonte in range(1, 4):
        indice = corte.year * 12 + corte.month - 1 + horizonte
        fila = {
            "feature_month": corte.isoformat(),
            "prediction_month": date(indice // 12, indice % 12 + 1, 1).isoformat(),
            "horizon_months": horizonte, "currency": "MXN", "status": "GENERADO",
            "base_debt_payment": float(pago), "initial_balance": float(saldo),
            "prob_negative_balance": float((saldos[:, horizonte - 1] < 0).mean()),
        }
        for nombre, valores in [("income", ingresos), ("expenses", gastos), ("balance", saldos)]:
            for percentil in [10, 50, 90]:
                valor = float(np.quantile(valores[:, horizonte - 1], percentil / 100))
                if not math.isfinite(valor):
                    return []
                fila[f"{nombre}_p{percentil}"] = valor
        resultado.append(fila)
    return resultado


def explicar_proyecciones(filas):
    return [{
        "Mes observado de referencia": f["feature_month"],
        "Mes estimado": f["prediction_month"],
        "Moneda": f["currency"],
        "Ingreso típico estimado": f.get("income_p50"),
        "Gasto típico estimado sin pagos de deuda": f.get("expenses_p50"),
        "Pago de deuda mensual supuesto": f.get("base_debt_payment"),
        "Saldo típico estimado después de deuda": f.get("balance_p50"),
        "Proporción de escenarios con saldo negativo": f.get("prob_negative_balance"),
        "Límites": "Estimación orientativa desde el último mes registrado; pagos de deuda constantes, sin transferencias futuras ni garantía de precisión.",
    } for f in filas if f.get("status") == "GENERADO"]
