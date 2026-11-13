import csv
from datetime import datetime, timezone, timedelta
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
    S3_MONTHLY_PATH,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

TTL_CACHE = 60
ZONA_MX = timezone(timedelta(hours=-6))
_candado = Lock()
_cache = {"expira": 0.0, "datos": None}


# Crear cliente S3 con soporte para certificados locales en Windows
def cliente_s3(verificar_ssl=True):
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        verify=verificar_ssl,
        config=Config(
            connect_timeout=5,
            read_timeout=15,
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


# Convertir timestamp UTC a fecha local de Ciudad de Mexico (UTC-6)
def extraer_fecha_local(texto_utc):
    if not texto_utc:
        return "Sin fecha", 0
    limpio = str(texto_utc).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(limpio)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(ZONA_MX)
        return local.strftime("%Y-%m-%d"), local.hour
    except ValueError:
        return str(texto_utc)[:10], 0


# Descargar lotes CSV exportados por monitoring.py (codificados en utf-8-sig con subcarpetas YYYY/MM/DD)
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

        por_id = {}
        for clave in claves_csv:
            # monitoring.py exporta los lotes con encode("utf-8-sig")
            cuerpo = (
                s3.get_object(Bucket=S3_BUCKET, Key=clave)["Body"]
                .read()
                .decode("utf-8-sig", errors="replace")
            )
            lector = csv.DictReader(StringIO(cuerpo))
            for fila in lector:
                limpia = {
                    (k.lstrip("\ufeff").strip() if k else ""): v
                    for k, v in fila.items()
                }
                iid = (limpia.get("interaccion_id") or "").strip()
                if iid and iid not in por_id:
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

    por_resultado = {}
    por_fecha = {}
    herramientas = {
        "Metas de ahorro": 0,
        "Escenarios": 0,
        "Consulta de historial": 0,
        "Cancelaciones": 0,
        "Consulta de finanzas": 0,
    }

    for f in filas:
        reg_completo = a_entero(f.get("registro_completo"))
        uso_completo = a_entero(f.get("uso_completo"))
        costo_completo = a_entero(f.get("costo_completo"))
        llamadas_mod = a_entero(f.get("llamadas_modelo"))
        duracion = a_flotante(f.get("duracion_segundos"))
        status = a_flotante(f.get("http_status"))
        resultado = (f.get("resultado") or "sin_resultado").strip()
        fecha_local, _ = extraer_fecha_local(f.get("fecha_hora_utc"))

        if reg_completo == 1 and duracion is not None:
            tiempos.append(duracion)

        if status is not None:
            statuses.append(int(status))
            if int(status) >= 400:
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

        u_meta = a_entero(f.get("usos_meta"))
        u_esc = a_entero(f.get("usos_escenario"))
        u_hist = a_entero(f.get("usos_historial"))
        u_canc = a_entero(f.get("usos_cancelacion"))
        u_fin = a_entero(f.get("usos_finanzas"))

        herramientas["Metas de ahorro"] += u_meta
        herramientas["Escenarios"] += u_esc
        herramientas["Consulta de historial"] += u_hist
        herramientas["Cancelaciones"] += u_canc
        herramientas["Consulta de finanzas"] += u_fin

        if fecha_local not in por_fecha:
            por_fecha[fecha_local] = {
                "interacciones": 0,
                "tiempos": [],
                "errores": 0,
                "usos_meta": 0,
                "usos_escenario": 0,
                "usos_historial": 0,
                "usos_cancelacion": 0,
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
                "errores": item["errores"],
                "usos_meta": item["usos_meta"],
                "usos_escenario": item["usos_escenario"],
                "usos_historial": item["usos_historial"],
                "usos_cancelacion": item["usos_cancelacion"],
            }
        )

    resultados_ordenados = dict(
        sorted(por_resultado.items(), key=lambda x: x[1], reverse=True)
    )

    return {
        "total_interacciones": total_interacciones,
        "conversaciones": conversaciones,
        "tiempo_promedio_s": round(sum(tiempos) / len(tiempos), 2) if tiempos else 0.0,
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
        "costo_promedio_consulta_usd": (
            round(sum(costos) / len(costos), 6) if costos else 0.0
        ),
        "costo_acumulado_usd": round(sum(costos), 6) if costos else 0.0,
        "interacciones_por_resultado": resultados_ordenados,
        "uso_herramientas_total": herramientas,
        "serie_por_fecha_local": serie_fechas,
    }


# Leer archivo o carpeta Parquet en S3 soportando SSL local
def leer_tabla_parquet_s3(ruta):
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
                    filas.extend(pq.read_table(BytesIO(cuerpo)).to_pylist())
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


# Calcular resumen agregado de las tablas Gold respetando las columnas de customer_model_summary
def calcular_resumen_gold():
    clientes = leer_tabla_parquet_s3(S3_CUSTOMER_PATH)
    categorias = leer_tabla_parquet_s3(S3_CATEGORIES_PATH)

    if not clientes:
        return {"disponible": False}

    por_salud = {}
    for c in clientes:
        estado = str(
            c.get("financial_health_status")
            or c.get("debt_risk_segment")
            or "SIN_CLASIFICAR"
        )
        por_salud[estado] = por_salud.get(estado, 0) + 1

    gasto_cat = {}
    for fila in categorias:
        cat = str(fila.get("category") or "otros")
        monto = a_flotante(fila.get("total_expenses"), 0.0) or 0.0
        if monto > 0:
            gasto_cat[cat] = gasto_cat.get(cat, 0.0) + monto

    top_categorias = {
        k: round(v, 2)
        for k, v in sorted(gasto_cat.items(), key=lambda x: x[1], reverse=True)[:8]
    }

    resumen_3m = {
        "ingreso_promedio_3m": promedio_columnas(
            clientes, ["avg_monthly_income_3m", "average_income_3m"]
        ),
        "desviacion_estandar_ingreso_3m": promedio_columnas(
            clientes, ["std_monthly_income_3m"]
        ),
        "gasto_promedio_3m": promedio_columnas(
            clientes, ["avg_monthly_expenses_3m", "average_expenses_3m"]
        ),
        "ahorro_acumulado_3m": promedio_columnas(clientes, ["savings_3m"]),
        "ratio_ahorro_3m": promedio_columnas(clientes, ["savings_rate_3m"], 4),
        "ratio_gasto_ingreso_3m": promedio_columnas(
            clientes, ["expense_to_income_ratio_3m", "spending_to_income_ratio"], 4
        ),
        "ratio_deuda_ingreso_3m": promedio_columnas(
            clientes,
            [
                "debt_to_income_ratio_3m",
                "debt_payment_ratio_3m",
                "debt_to_income_ratio",
            ],
            4,
        ),
        "variacion_ingreso_3m_pct": promedio_columnas(
            clientes, ["income_change_pct_3m"]
        ),
    }

    # Calcular desviacion estandar y variacion de 3 meses desde customer_monthly_summary si aun no estan en el Parquet
    if resumen_3m["desviacion_estandar_ingreso_3m"] is None:
        mensual = leer_tabla_parquet_s3(S3_MONTHLY_PATH)
        if mensual:
            por_cliente = {}
            for m in mensual:
                cid = (
                    m.get("client_id")
                    if m.get("client_id") is not None
                    else m.get("customer_id")
                )
                if cid is not None:
                    por_cliente.setdefault(cid, []).append(m)
            ing_3m, std_3m, gas_ratio_3m, var_3m = [], [], [], []
            for lista in por_cliente.values():
                ultimos = sorted(lista, key=lambda x: str(x.get("month") or ""))[-3:]
                ingresos = [
                    a_flotante(x.get("monthly_income"), 0.0) or 0.0 for x in ultimos
                ]
                gastos = [
                    a_flotante(x.get("monthly_expenses"), 0.0) or 0.0 for x in ultimos
                ]
                if ingresos:
                    prom_i = sum(ingresos) / len(ingresos)
                    prom_g = sum(gastos) / len(gastos)
                    ing_3m.append(prom_i)
                    if len(ingresos) > 1:
                        varianza = sum((x - prom_i) ** 2 for x in ingresos) / (
                            len(ingresos) - 1
                        )
                        std_3m.append(varianza**0.5)
                        if ingresos[0] > 0:
                            var_3m.append(
                                ((ingresos[-1] - ingresos[0]) / ingresos[0]) * 100.0
                            )
                    if prom_i > 0:
                        gas_ratio_3m.append(prom_g / prom_i)
            if resumen_3m["ingreso_promedio_3m"] is None and ing_3m:
                resumen_3m["ingreso_promedio_3m"] = round(sum(ing_3m) / len(ing_3m), 2)
            if std_3m:
                resumen_3m["desviacion_estandar_ingreso_3m"] = round(
                    sum(std_3m) / len(std_3m), 2
                )
            if resumen_3m["ratio_gasto_ingreso_3m"] is None and gas_ratio_3m:
                resumen_3m["ratio_gasto_ingreso_3m"] = round(
                    sum(gas_ratio_3m) / len(gas_ratio_3m), 4
                )
            if resumen_3m["variacion_ingreso_3m_pct"] is None and var_3m:
                resumen_3m["variacion_ingreso_3m_pct"] = round(
                    sum(var_3m) / len(var_3m), 2
                )

    return {
        "disponible": True,
        "total_clientes": len(clientes),
        "ingreso_mensual_promedio_general": promedio_columnas(
            clientes, ["monthly_income", "avg_monthly_income"]
        ),
        "gasto_mensual_promedio_general": promedio_columnas(
            clientes, ["monthly_expenses", "avg_monthly_expenses"]
        ),
        "gasto_total_historico_promedio": promedio_columnas(clientes, ["total_spent"]),
        "transacciones_promedio_por_cliente": promedio_columnas(
            clientes, ["total_transactions"]
        ),
        "deuda_total_promedio": promedio_columnas(clientes, ["total_debt"]),
        "score_crediticio_promedio": promedio_columnas(clientes, ["credit_score"]),
        "ratio_gasto_ingreso_general": promedio_columnas(
            clientes, ["spending_to_income_ratio"], 4
        ),
        "ratio_deuda_ingreso_general": promedio_columnas(
            clientes, ["debt_to_income_ratio"], 4
        ),
        "metricas_ultimos_3_meses": resumen_3m,
        "clientes_por_salud_financiera": por_salud,
        "top_gastos_por_categoria": top_categorias,
    }


# Construir componentes visuales listos para renderizar en el chat
def construir_visuales(monitoreo, gold):
    fechas = monitoreo.get("serie_por_fecha_local", [])
    m3 = gold.get("metricas_ultimos_3_meses") or {}
    return {
        "resultados": {
            "titulo": "Total interacciones por resultado",
            "tipo": "barras_horizontales",
            "datos": [
                {"etiqueta": k, "valor": v}
                for k, v in monitoreo.get("interacciones_por_resultado", {}).items()
            ],
        },
        "fechas": {
            "titulo": "Total interacciones por fecha_local",
            "tipo": "barras_verticales",
            "datos": [
                {"etiqueta": f["fecha_local"][5:], "valor": f["interacciones"]}
                for f in fechas
            ],
        },
        "tiempos": {
            "titulo": "Tiempo promedio (s) por fecha_local",
            "tipo": "barras_horizontales",
            "datos": [
                {"etiqueta": f["fecha_local"], "valor": f["tiempo_promedio_s"]}
                for f in fechas
            ],
        },
        "herramientas": {
            "titulo": "Uso de herramientas del asistente",
            "tipo": "barras_horizontales",
            "datos": [
                {"etiqueta": k, "valor": v}
                for k, v in monitoreo.get("uso_herramientas_total", {}).items()
            ],
        },
        "gold_3m": {
            "titulo": "Comportamiento transaccional y métricas 3M (Gold)",
            "tipo": "tarjetas_gold",
            "datos": [
                {
                    "etiqueta": "Clientes analizados",
                    "valor": gold.get("total_clientes", 0),
                },
                {
                    "etiqueta": "Ingreso prom. 3M (MXN)",
                    "valor": m3.get("ingreso_promedio_3m")
                    or gold.get("ingreso_mensual_promedio_general")
                    or 0,
                },
                {
                    "etiqueta": "Desv. estándar 3M (MXN)",
                    "valor": m3.get("desviacion_estandar_ingreso_3m") or 0,
                },
                {
                    "etiqueta": "Ratio ahorro 3M",
                    "valor": m3.get("ratio_ahorro_3m") or 0,
                },
                {
                    "etiqueta": "Ratio deuda/ingreso 3M",
                    "valor": m3.get("ratio_deuda_ingreso_3m")
                    or gold.get("ratio_deuda_ingreso_general")
                    or 0,
                },
                {
                    "etiqueta": "Variación ingreso 3M (%)",
                    "valor": m3.get("variacion_ingreso_3m_pct") or 0,
                },
            ],
        },
        "categorias": {
            "titulo": "Top gastos por categoría (Gold)",
            "tipo": "barras_horizontales",
            "datos": [
                {"etiqueta": k, "valor": v}
                for k, v in (gold.get("top_gastos_por_categoria") or {}).items()
            ],
        },
    }


# Obtener contexto completo con cache en memoria
def obtener_contexto_bi(forzar=False):
    ahora = monotonic()
    with _candado:
        if not forzar and _cache["datos"] is not None and ahora < _cache["expira"]:
            return _cache["datos"]

    filas_interacciones = leer_interacciones_s3()
    monitoreo = calcular_metricas_monitoreo(filas_interacciones)
    gold = calcular_resumen_gold()
    visuales = construir_visuales(monitoreo, gold)

    datos = {
        "monitoreo": monitoreo,
        "gold": gold,
        "visuales": visuales,
    }
    with _candado:
        _cache["datos"] = datos
        _cache["expira"] = monotonic() + TTL_CACHE
    return datos
