from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
import logging
from threading import Lock
from time import monotonic
import urllib3

import boto3
from botocore.config import Config
from botocore.exceptions import SSLError
import pyarrow.parquet as pq

from bi.chat.backend.config import (
    AWS_REGION,
    S3_BUCKET,
    S3_CATEGORIES_PATH,
    S3_CUSTOMER_PATH,
    S3_INTERACTIONS_PREFIX,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

TTL_CACHE = 300
ZONA_MX = timezone(timedelta(hours=-6))
_candado = Lock()
_cache = {"expira": 0.0, "datos": None}

# Solo se leen columnas numéricas y de segmentación; jamás PII ni identificadores individuales
COLUMNAS_CLIENTES_GOLD = [
    "currency",
    "current_age",
    "retirement_age",
    "yearly_income",
    "monthly_income",
    "monthly_expenses",
    "monthly_debt_payments",
    "monthly_savings",
    "account_balance",
    "total_debt",
    "credit_score",
    "total_cards",
    "total_credit_limit",
    "total_transactions",
    "total_spent",
    "average_transaction",
    "spending_to_income_ratio",
    "debt_to_income_ratio",
    "income_3m",
    "expenses_3m",
    "debt_payments_3m",
    "savings_3m",
    "transactions_3m",
    "average_income_3m",
    "average_expenses_3m",
    "average_debt_payments_3m",
    "average_savings_3m",
    "income_std_3m",
    "income_std_3",
    "expenses_std_3m",
    "savings_rate_3m",
    "debt_payment_ratio_3m",
    "expense_to_income_ratio_3m",
    "income_change_3m",
    "expense_coverage_months",
    "has_overdue_payments",
    "has_negative_balance",
    "has_budget_deficit_3m",
    "financial_health_status",
    "predicted_income",
    "has_income_prediction",
    "prediction_validation_status",
]


# Crear cliente S3 con soporte para certificados locales en Windows
def cliente_s3(verificar_ssl=True):
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        verify=verificar_ssl,
        config=Config(
            connect_timeout=5,
            read_timeout=15,
            max_pool_connections=16,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


# Ejecutar operacion S3 reintentando sin verificacion SSL si el antivirus local interviene
def ejecutar_s3(operacion):
    try:
        return operacion(cliente_s3(verificar_ssl=True))
    except SSLError:
        logger.warning("Certificado local detectado en S3; reintentando conexion.")
        return operacion(cliente_s3(verificar_ssl=False))


# Convertir texto a entero de forma segura
def a_entero(valor, defecto=0):
    try:
        if valor is None or str(valor).strip() == "":
            return defecto
        return int(float(str(valor).strip()))
    except (ValueError, TypeError):
        return defecto


# Convertir texto a flotante de forma segura
def a_flotante(valor, defecto=None):
    try:
        if valor is None or str(valor).strip() == "":
            return defecto
        return float(str(valor).strip())
    except (ValueError, TypeError):
        return defecto


# Convertir timestamp UTC a fecha y hora local de Ciudad de Mexico (UTC-6)
def extraer_fecha_local(texto_utc):
    if not texto_utc:
        return "Sin fecha", "Sin fecha", 0
    limpio = str(texto_utc).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(limpio)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(ZONA_MX)
        return (
            local.strftime("%Y-%m-%d"),
            local.strftime("%Y-%m-%d %H:%M:%S"),
            local.hour,
        )
    except ValueError:
        return str(texto_utc)[:10], str(texto_utc)[:19], 0


# Descargar lotes CSV exportados por monitoring.py en paralelo
def leer_interacciones_s3():
    prefijo = S3_INTERACTIONS_PREFIX.strip("/")
    if not prefijo:
        return []

    def _descargar(s3):
        claves_csv = []
        paginador = s3.get_paginator("list_objects_v2")
        for pagina in paginador.paginate(Bucket=S3_BUCKET, Prefix=f"{prefijo}/"):
            for obj in pagina.get("Contents", []):
                clave = obj.get("Key", "")
                if clave.lower().endswith(".csv") and clave not in claves_csv:
                    claves_csv.append(clave)

        if not claves_csv:
            return []

        def _leer_csv(clave):
            cuerpo = (
                s3.get_object(Bucket=S3_BUCKET, Key=clave)["Body"]
                .read()
                .decode("utf-8-sig", errors="replace")
            )
            filas_archivo = []
            for fila in csv.DictReader(StringIO(cuerpo)):
                limpia = {
                    (k.lstrip("\ufeff").strip() if k else ""): v
                    for k, v in fila.items()
                }
                iid = (limpia.get("interaccion_id") or "").strip()
                if iid:
                    filas_archivo.append((iid, limpia))
            return filas_archivo

        por_id = {}
        max_hilos = min(8, len(claves_csv))
        with ThreadPoolExecutor(max_workers=max_hilos) as pool:
            for lista in pool.map(_leer_csv, claves_csv):
                for iid, limpia in lista:
                    if iid not in por_id:
                        por_id[iid] = limpia
        return list(por_id.values())

    try:
        return ejecutar_s3(_descargar)
    except Exception as exc:
        logger.error("Error al leer lotes CSV de monitoreo en S3: %s", exc)
        return []


# Calcular exactamente las mismas medidas DAX del modelo Power BI (Interacciones.tmdl)
def calcular_metricas_monitoreo(filas):
    total_interacciones = len(filas)
    conversaciones = len(
        {
            (f.get("conversacion_id") or "").strip()
            for f in filas
            if (f.get("conversacion_id") or "").strip()
        }
    )

    tiempos = []
    statuses = []
    errores_http = 0
    tokens_in = []
    tokens_out = []
    costos = []
    total_llamadas_modelo = 0
    total_errores_modelo = 0
    total_llamadas_herramientas = 0
    total_errores_herramientas = 0

    por_resultado = {}
    por_status = {}
    por_fecha = {}
    por_hora = {}

    herramientas = {
        "Metas de ahorro (usos_meta)": 0,
        "Escenarios (usos_escenario)": 0,
        "Consulta de historial (usos_historial)": 0,
        "Cancelaciones (usos_cancelacion)": 0,
        "Consulta de finanzas (usos_finanzas)": 0,
    }

    for f in filas:
        reg_completo = a_entero(f.get("registro_completo"))
        uso_completo = a_entero(f.get("uso_completo"))
        costo_completo = a_entero(f.get("costo_completo"))
        llamadas_mod = a_entero(f.get("llamadas_modelo"))
        err_mod = a_entero(f.get("errores_modelo"))
        llamadas_herr = a_entero(f.get("llamadas_herramientas"))
        err_herr = a_entero(f.get("errores_herramientas"))
        duracion = a_flotante(f.get("duracion_segundos"))
        status = a_flotante(f.get("http_status"))
        resultado = (f.get("resultado") or "sin_resultado").strip()
        fecha_local, _, hora_local = extraer_fecha_local(f.get("fecha_hora_utc"))

        total_llamadas_modelo += llamadas_mod
        total_errores_modelo += err_mod
        total_llamadas_herramientas += llamadas_herr
        total_errores_herramientas += err_herr

        if reg_completo == 1 and duracion is not None:
            tiempos.append(duracion)

        if status is not None:
            codigo = int(status)
            statuses.append(codigo)
            por_status[str(codigo)] = por_status.get(str(codigo), 0) + 1
            if codigo >= 400:
                errores_http += 1

        if reg_completo == 1 and uso_completo == 1 and llamadas_mod > 0:
            t_in = a_flotante(f.get("tokens_entrada"))
            t_out = a_flotante(f.get("tokens_salida"))
            if t_in is not None:
                tokens_in.append(t_in)
            if t_out is not None:
                tokens_out.append(t_out)

        if reg_completo == 1 and costo_completo == 1 and llamadas_mod > 0:
            costo = a_flotante(f.get("costo_total_usd"))
            if costo is not None:
                costos.append(costo)

        por_resultado[resultado] = por_resultado.get(resultado, 0) + 1
        por_hora[str(hora_local)] = por_hora.get(str(hora_local), 0) + 1

        u_meta = a_entero(f.get("usos_meta"))
        u_esc = a_entero(f.get("usos_escenario"))
        u_hist = a_entero(f.get("usos_historial"))
        u_canc = a_entero(f.get("usos_cancelacion"))
        u_fin = a_entero(f.get("usos_finanzas"))

        herramientas["Metas de ahorro (usos_meta)"] += u_meta
        herramientas["Escenarios (usos_escenario)"] += u_esc
        herramientas["Consulta de historial (usos_historial)"] += u_hist
        herramientas["Cancelaciones (usos_cancelacion)"] += u_canc
        herramientas["Consulta de finanzas (usos_finanzas)"] += u_fin

        if fecha_local not in por_fecha:
            por_fecha[fecha_local] = {
                "interacciones": 0,
                "tiempos": [],
                "errores": 0,
                "usos_meta": 0,
                "usos_escenario": 0,
                "usos_historial": 0,
                "usos_cancelacion": 0,
                "usos_finanzas": 0,
            }
        por_fecha[fecha_local]["interacciones"] += 1
        if reg_completo == 1 and duracion is not None:
            por_fecha[fecha_local]["tiempos"].append(duracion)
        if status is not None and int(status) >= 400:
            por_fecha[fecha_local]["errores"] += 1
        por_fecha[fecha_local]["usos_meta"] += u_meta
        por_fecha[fecha_local]["usos_escenario"] += u_esc
        por_fecha[fecha_local]["usos_historial"] += u_hist
        por_fecha[fecha_local]["usos_cancelacion"] += u_canc
        por_fecha[fecha_local]["usos_finanzas"] += u_fin

    serie_fechas = []
    for fecha in sorted(por_fecha.keys()):
        item = por_fecha[fecha]
        t_lista = item["tiempos"]
        serie_fechas.append(
            {
                "fecha_local": fecha,
                "interacciones": item["interacciones"],
                "tiempo_promedio_s": (
                    round(sum(t_lista) / len(t_lista), 2) if t_lista else 0.0
                ),
                "errores_http": item["errores"],
                "usos_meta": item["usos_meta"],
                "usos_escenario": item["usos_escenario"],
                "usos_historial": item["usos_historial"],
                "usos_cancelacion": item["usos_cancelacion"],
                "usos_finanzas": item["usos_finanzas"],
            }
        )

    return {
        "total_interacciones": total_interacciones,
        "conversaciones": conversaciones,
        "tiempo_promedio_s": round(sum(tiempos) / len(tiempos), 2) if tiempos else 0.0,
        "tiempo_minimo_s": round(min(tiempos), 2) if tiempos else 0.0,
        "tiempo_maximo_s": round(max(tiempos), 2) if tiempos else 0.0,
        "errores_http": errores_http,
        "porcentaje_errores": (
            round((errores_http / len(statuses)) * 100, 2) if statuses else 0.0
        ),
        "tokens_entrada_promedio": (
            round(sum(tokens_in) / len(tokens_in), 2) if tokens_in else 0.0
        ),
        "tokens_salida_promedio": (
            round(sum(tokens_out) / len(tokens_out), 2) if tokens_out else 0.0
        ),
        "tokens_entrada_total": int(sum(tokens_in)) if tokens_in else 0,
        "tokens_salida_total": int(sum(tokens_out)) if tokens_out else 0,
        "costo_promedio_consulta_usd": (
            round(sum(costos) / len(costos), 6) if costos else 0.0
        ),
        "costo_acumulado_usd": round(sum(costos), 6) if costos else 0.0,
        "total_llamadas_modelo": total_llamadas_modelo,
        "total_errores_modelo": total_errores_modelo,
        "total_llamadas_herramientas": total_llamadas_herramientas,
        "total_errores_herramientas": total_errores_herramientas,
        "interacciones_por_resultado": dict(
            sorted(por_resultado.items(), key=lambda x: x[1], reverse=True)
        ),
        "interacciones_por_http_status": por_status,
        "interacciones_por_hora_local": dict(
            sorted(por_hora.items(), key=lambda x: int(x[0]))
        ),
        "uso_herramientas_total": herramientas,
        "serie_por_fecha_local": serie_fechas,
    }


# Leer archivo o carpeta Parquet en S3 proyectando solo las columnas necesarias
def leer_tabla_parquet_s3(ruta, columnas=None):
    if not ruta:
        return []

    def _descargar_parquet(s3):
        paginador = s3.get_paginator("list_objects_v2")
        filas = []
        prefijo = ruta.strip("/")
        for pagina in paginador.paginate(Bucket=S3_BUCKET, Prefix=prefijo):
            for obj in pagina.get("Contents", []):
                k = obj.get("Key", "")
                nombre = k.rsplit("/", 1)[-1]
                if k.endswith(".parquet") and not nombre.startswith((".", "_")):
                    cuerpo = s3.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read()
                    archivo = pq.ParquetFile(BytesIO(cuerpo))
                    cols_validas = (
                        [c for c in columnas if c in archivo.schema.names]
                        if columnas
                        else None
                    )
                    filas.extend(archivo.read(columns=cols_validas).to_pylist())
        return filas

    try:
        return ejecutar_s3(_descargar_parquet)
    except Exception as exc:
        logger.error("Error al leer Parquet %s en S3: %s", ruta, exc)
        return []


# Calcular promedio de la primera columna disponible en la lista de nombres
def promedio_columnas(filas, campos, decimales=2):
    if isinstance(campos, str):
        campos = [campos]
    for campo in campos:
        valores = [
            float(f[campo])
            for f in filas
            if f.get(campo) is not None and a_flotante(f.get(campo)) is not None
        ]
        if valores:
            return round(sum(valores) / len(valores), decimales)
    return None


# Clasificar score crediticio en niveles bancarios agregados
def rango_score(score):
    if score is None:
        return "Sin score"
    if score < 580:
        return "Bajo (<580)"
    if score < 670:
        return "Regular (580-669)"
    if score < 740:
        return "Bueno (670-739)"
    return "Excelente (740+)"


# Clasificar edad en cohortes demográficas agregadas
def rango_edad(edad):
    if edad is None:
        return "Sin dato"
    if edad < 30:
        return "18-29 años"
    if edad < 45:
        return "30-44 años"
    if edad < 60:
        return "45-59 años"
    return "60+ años"


# Calcular medidas agregadas del Dashboard Resumen Financiero (sin filas individuales)
def calcular_resumen_gold():
    clientes = leer_tabla_parquet_s3(S3_CUSTOMER_PATH, COLUMNAS_CLIENTES_GOLD)
    categorias = leer_tabla_parquet_s3(
        S3_CATEGORIES_PATH,
        ["currency", "category", "expense_transactions", "total_expenses"],
    )

    if not clientes:
        return {"disponible": False}

    por_salud_detalle = {}
    por_score = {}
    por_edad = {}
    con_atrasos = 0
    con_saldo_negativo = 0
    con_deficit_3m = 0
    predicciones_aprobadas = []

    suma_expenses_3m = 0.0
    suma_income_3m = 0.0

    for c in clientes:
        estado = str(c.get("financial_health_status") or "SIN_CLASIFICAR")
        seg = por_salud_detalle.setdefault(
            estado,
            {
                "clientes": 0,
                "ingresos_3m": [],
                "gastos_3m": [],
                "ratios_ahorro_3m": [],
                "deudas": [],
                "transacciones_3m": [],
            },
        )
        seg["clientes"] += 1

        ing3 = a_flotante(c.get("average_income_3m"))
        gas3 = a_flotante(c.get("average_expenses_3m"))
        aho3 = a_flotante(c.get("savings_rate_3m"))
        deu = a_flotante(c.get("total_debt"))
        tx3 = a_flotante(c.get("transactions_3m"))

        if ing3 is not None:
            seg["ingresos_3m"].append(ing3)
        if gas3 is not None:
            seg["gastos_3m"].append(gas3)
        if aho3 is not None:
            seg["ratios_ahorro_3m"].append(aho3)
        if deu is not None:
            seg["deudas"].append(deu)
        if tx3 is not None:
            seg["transacciones_3m"].append(tx3)

        nivel_sc = rango_score(a_entero(c.get("credit_score"), None))
        por_score[nivel_sc] = por_score.get(nivel_sc, 0) + 1

        cohorte_ed = rango_edad(a_flotante(c.get("current_age"), None))
        por_edad[cohorte_ed] = por_edad.get(cohorte_ed, 0) + 1

        suma_expenses_3m += a_flotante(c.get("expenses_3m"), 0.0) or 0.0
        suma_income_3m += a_flotante(c.get("income_3m"), 0.0) or 0.0

        if c.get("has_overdue_payments"):
            con_atrasos += 1
        if c.get("has_negative_balance"):
            con_saldo_negativo += 1
        if c.get("has_budget_deficit_3m"):
            con_deficit_3m += 1
        if (
            c.get("has_income_prediction")
            and c.get("prediction_validation_status") == "APROBADO"
            and a_flotante(c.get("predicted_income")) is not None
        ):
            predicciones_aprobadas.append(a_flotante(c.get("predicted_income")))

    total_clientes = len(clientes)
    resumen_segmentos_salud = {}
    for estado, info in sorted(
        por_salud_detalle.items(), key=lambda x: x[1]["clientes"], reverse=True
    ):
        resumen_segmentos_salud[estado] = {
            "clientes": info["clientes"],
            "porcentaje_cartera": round((info["clientes"] / total_clientes) * 100, 2),
            "ingreso_promedio_3m_mxn": (
                round(sum(info["ingresos_3m"]) / len(info["ingresos_3m"]), 2)
                if info["ingresos_3m"]
                else 0.0
            ),
            "gasto_promedio_3m_mxn": (
                round(sum(info["gastos_3m"]) / len(info["gastos_3m"]), 2)
                if info["gastos_3m"]
                else 0.0
            ),
            "ratio_ahorro_3m_promedio": (
                round(sum(info["ratios_ahorro_3m"]) / len(info["ratios_ahorro_3m"]), 4)
                if info["ratios_ahorro_3m"]
                else 0.0
            ),
            "deuda_total_promedio_mxn": (
                round(sum(info["deudas"]) / len(info["deudas"]), 2)
                if info["deudas"]
                else 0.0
            ),
            "transacciones_3m_promedio": (
                round(sum(info["transacciones_3m"]) / len(info["transacciones_3m"]), 1)
                if info["transacciones_3m"]
                else 0.0
            ),
        }

    gasto_cat = {}
    tx_cat = {}
    for fila in categorias:
        cat = str(fila.get("category") or "otros")
        monto = a_flotante(fila.get("total_expenses"), 0.0) or 0.0
        txs = a_entero(fila.get("expense_transactions"), 0)
        if monto > 0:
            gasto_cat[cat] = gasto_cat.get(cat, 0.0) + monto
            tx_cat[cat] = tx_cat.get(cat, 0) + txs

    total_gasto_categorias = sum(gasto_cat.values()) or 1.0
    top_categorias = {
        k: {
            "gasto_total_mxn": round(v, 2),
            "porcentaje_del_gasto": round((v / total_gasto_categorias) * 100, 2),
            "transacciones": tx_cat.get(k, 0),
        }
        for k, v in sorted(gasto_cat.items(), key=lambda x: x[1], reverse=True)[:12]
    }

    # Medidas DAX exactas de Clientes.tmdl
    avg_monthly_inc = promedio_columnas(clientes, ["monthly_income"]) or 0.0
    avg_inc_3m = promedio_columnas(clientes, ["average_income_3m"]) or 0.0
    variacion_dax_3m = (
        round(((avg_monthly_inc - avg_inc_3m) / avg_inc_3m) * 100, 2)
        if avg_inc_3m > 0
        else 0.0
    )
    ratio_gasto_ingreso_dax_3m = (
        round(suma_expenses_3m / suma_income_3m, 4) if suma_income_3m > 0 else 0.0
    )

    resumen_3m = {
        "ingreso_promedio_3m": avg_inc_3m,
        "desviacion_estandar_ingreso_3m": promedio_columnas(
            clientes, ["income_std_3", "income_std_3m"]
        ),
        "gasto_promedio_3m": promedio_columnas(clientes, ["average_expenses_3m"]),
        "pago_deuda_promedio_3m": promedio_columnas(
            clientes, ["average_debt_payments_3m"]
        ),
        "ahorro_acumulado_3m": promedio_columnas(clientes, ["savings_3m"]),
        "ratio_ahorro_3m": promedio_columnas(clientes, ["savings_rate_3m"], 4),
        "ratio_gasto_ingreso_3m_dax": ratio_gasto_ingreso_dax_3m,
        "ratio_gasto_ingreso_3m_promedio": promedio_columnas(
            clientes, ["expense_to_income_ratio_3m"], 4
        ),
        "ratio_deuda_ingreso_3m": promedio_columnas(
            clientes, ["debt_payment_ratio_3m", "debt_to_income_ratio"], 4
        ),
        "variacion_ingreso_3m_pct_dax": variacion_dax_3m,
        "ticket_promedio_transaccion": promedio_columnas(
            clientes, ["average_transaction"]
        ),
    }

    return {
        "disponible": True,
        "total_clientes": total_clientes,
        "edad_promedio": promedio_columnas(clientes, ["current_age"], 1),
        "ingreso_mensual_promedio_general": avg_monthly_inc,
        "gasto_mensual_promedio_general": promedio_columnas(
            clientes, ["monthly_expenses"]
        ),
        "gasto_total_historico_promedio": promedio_columnas(clientes, ["total_spent"]),
        "transacciones_promedio_por_cliente": promedio_columnas(
            clientes, ["total_transactions"]
        ),
        "deuda_total_promedio": promedio_columnas(clientes, ["total_debt"]),
        "score_crediticio_promedio": promedio_columnas(clientes, ["credit_score"]),
        "limite_credito_total_promedio": promedio_columnas(
            clientes, ["total_credit_limit"]
        ),
        "ratio_gasto_ingreso_general": promedio_columnas(
            clientes, ["spending_to_income_ratio"], 4
        ),
        "ratio_deuda_ingreso_general": promedio_columnas(
            clientes, ["debt_to_income_ratio"], 4
        ),
        "metricas_ultimos_3_meses_dax": resumen_3m,
        "alertas_cartera": {
            "clientes_con_pagos_vencidos": con_atrasos,
            "clientes_con_saldo_negativo": con_saldo_negativo,
            "clientes_con_deficit_3m": con_deficit_3m,
            "clientes_con_prediccion_ingreso_aprobada": len(predicciones_aprobadas),
            "ingreso_predicho_promedio_mxn": (
                round(
                    sum(predicciones_aprobadas) / len(predicciones_aprobadas),
                    2,
                )
                if predicciones_aprobadas
                else None
            ),
        },
        "clientes_por_salud_financiera": {
            k: v["clientes"] for k, v in resumen_segmentos_salud.items()
        },
        "detalle_segmentos_salud_financiera": resumen_segmentos_salud,
        "clientes_por_rango_score_crediticio": por_score,
        "clientes_por_rango_edad": por_edad,
        "top_gastos_por_categoria": top_categorias,
    }


# Obtener contexto completo con cache en memoria
def obtener_contexto_bi(forzar=False):
    ahora = monotonic()
    with _candado:
        if not forzar and _cache["datos"] is not None and ahora < _cache["expira"]:
            return _cache["datos"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_interacciones = pool.submit(leer_interacciones_s3)
        fut_gold = pool.submit(calcular_resumen_gold)
        filas_interacciones = fut_interacciones.result()
        gold = fut_gold.result()

    monitoreo = calcular_metricas_monitoreo(filas_interacciones)

    datos = {
        "monitoreo": monitoreo,
        "gold": gold,
    }
    with _candado:
        _cache["datos"] = datos
        _cache["expira"] = monotonic() + TTL_CACHE
    return datos
