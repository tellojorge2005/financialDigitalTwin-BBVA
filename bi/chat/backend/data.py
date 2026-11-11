import csv
from datetime import datetime, timezone, timedelta
from io import BytesIO, StringIO
from threading import Lock
from time import monotonic

import boto3
from botocore.config import Config
import pyarrow.parquet as pq

from bi.chat.backend.config import (
    AWS_REGION,
    S3_BUCKET,
    S3_CATEGORIES_PATH,
    S3_CUSTOMER_PATH,
    S3_INTERACTIONS_PREFIX,
    S3_MONTHLY_PATH,
)


TTL_CACHE = 60
ZONA_MX = timezone(timedelta(hours=-6))
_candado = Lock()
_cache = {"expira": 0.0, "datos": None}


# Crear cliente S3 con timeouts cortos
def cliente_s3():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(
            connect_timeout=5,
            read_timeout=15,
            retries={"max_attempts": 2, "mode": "standard"},
        ),
    )


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


# Descargar y combinar todos los CSV de interacciones desde S3 usando S3_INTERACTIONS_PREFIX
def leer_interacciones_s3(s3):
    prefijo = S3_INTERACTIONS_PREFIX.strip("/")
    if not prefijo:
        return []

    claves_csv = []
    paginador = s3.get_paginator("list_objects_v2")
    try:
        for pagina in paginador.paginate(Bucket=S3_BUCKET, Prefix=f"{prefijo}/"):
            for obj in pagina.get("Contents", []):
                clave = obj.get("Key", "")
                if clave.lower().endswith(".csv") and clave not in claves_csv:
                    claves_csv.append(clave)
    except Exception:
        return []

    por_id = {}
    for clave in claves_csv:
        try:
            cuerpo = s3.get_object(Bucket=S3_BUCKET, Key=clave)["Body"].read().decode("utf-8", errors="replace")
            lector = csv.DictReader(StringIO(cuerpo))
            for fila in lector:
                iid = (fila.get("interaccion_id") or "").strip()
                if iid and iid not in por_id:
                    por_id[iid] = fila
        except Exception:
            continue

    return list(por_id.values())


# Calcular exactamente las mismas medidas DAX del modelo Power BI (Interacciones.tmdl)
def calcular_metricas_monitoreo(filas):
    total_interacciones = len(filas)
    conversaciones = len({
        (f.get("conversacion_id") or "").strip()
        for f in filas
        if (f.get("conversacion_id") or "").strip()
    })

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

        herramientas["Metas de ahorro"] += u_meta
        herramientas["Escenarios"] += u_esc
        herramientas["Consulta de historial"] += u_hist
        herramientas["Cancelaciones"] += u_canc

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
        serie_fechas.append({
            "fecha_local": fecha,
            "interacciones": item["interacciones"],
            "tiempo_promedio_s": round(sum(t_lista) / len(t_lista), 2) if t_lista else 0.0,
            "errores": item["errores"],
            "usos_meta": item["usos_meta"],
            "usos_escenario": item["usos_escenario"],
            "usos_historial": item["usos_historial"],
            "usos_cancelacion": item["usos_cancelacion"],
        })

    resultados_ordenados = dict(
        sorted(por_resultado.items(), key=lambda x: x[1], reverse=True)
    )

    return {
        "total_interacciones": total_interacciones,
        "conversaciones": conversaciones,
        "tiempo_promedio_s": round(sum(tiempos) / len(tiempos), 2) if tiempos else 0.0,
        "errores_http": errores_http,
        "porcentaje_errores": round((errores_http / len(statuses)) * 100, 2) if statuses else 0.0,
        "tokens_entrada_promedio": round(sum(tokens_in) / len(tokens_in), 2) if tokens_in else 0.0,
        "tokens_salida_promedio": round(sum(tokens_out) / len(tokens_out), 2) if tokens_out else 0.0,
        "costo_promedio_consulta_usd": round(sum(costos) / len(costos), 6) if costos else 0.0,
        "costo_acumulado_usd": round(sum(costos), 6) if costos else 0.0,
        "interacciones_por_resultado": resultados_ordenados,
        "uso_herramientas_total": herramientas,
        "serie_por_fecha_local": serie_fechas,
    }


# Leer archivo Parquet en S3 soportando archivo directo o carpeta particionada de Spark
def leer_tabla_parquet_s3(s3, ruta):
    if not ruta:
        return []
    try:
        cuerpo = s3.get_object(Bucket=S3_BUCKET, Key=ruta)["Body"].read()
        return pq.read_table(BytesIO(cuerpo)).to_pylist()
    except Exception:
        paginador = s3.get_paginator("list_objects_v2")
        filas = []
        try:
            for pagina in paginador.paginate(Bucket=S3_BUCKET, Prefix=ruta.rstrip("/") + "/"):
                for obj in pagina.get("Contents", []):
                    k = obj.get("Key", "")
                    if k.endswith(".parquet"):
                        cuerpo = s3.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read()
                        filas.extend(pq.read_table(BytesIO(cuerpo)).to_pylist())
        except Exception:
            return []
        return filas


# Calcular promedio de una columna numerica ignorando nulos
def promedio_columna(filas, campo, decimales=2):
    valores = [
        float(f[campo]) for f in filas
        if f.get(campo) is not None and a_flotante(f.get(campo)) is not None
    ]
    return round(sum(valores) / len(valores), decimales) if valores else None


# Calcular resumen agregado de las tablas Gold sin exponer PII ni tarjetas individuales
def calcular_resumen_gold(s3):
    clientes = leer_tabla_parquet_s3(s3, S3_CUSTOMER_PATH)
    categorias = leer_tabla_parquet_s3(s3, S3_CATEGORIES_PATH)

    if not clientes:
        return {"disponible": False}

    por_segmento = {}
    for c in clientes:
        seg = str(c.get("debt_risk_segment") or c.get("financial_segment") or "SIN_SEGMENTO")
        por_segmento[seg] = por_segmento.get(seg, 0) + 1

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
        "ingreso_promedio_3m": promedio_columna(clientes, "avg_monthly_income_3m"),
        "desviacion_estandar_ingreso_3m": promedio_columna(clientes, "std_monthly_income_3m"),
        "gasto_promedio_3m": promedio_columna(clientes, "avg_monthly_expenses_3m"),
        "ratio_ahorro_3m": promedio_columna(clientes, "savings_rate_3m", 4),
        "ratio_gasto_ingreso_3m": promedio_columna(clientes, "expense_to_income_ratio_3m", 4),
        "ratio_deuda_ingreso_3m": promedio_columna(clientes, "debt_to_income_ratio_3m", 4),
        "variacion_ingreso_3m_pct": promedio_columna(clientes, "income_change_pct_3m"),
    }

    if resumen_3m["ingreso_promedio_3m"] is None:
        mensual = leer_tabla_parquet_s3(s3, S3_MONTHLY_PATH)
        if mensual:
            por_cliente = {}
            for m in mensual:
                cid = m.get("customer_id")
                if cid is not None:
                    por_cliente.setdefault(cid, []).append(m)
            ing_3m, std_3m, aho_3m, gas_ratio_3m = [], [], [], []
            for lista in por_cliente.values():
                ultimos = sorted(lista, key=lambda x: str(x.get("month") or ""))[-3:]
                ingresos = [a_flotante(x.get("monthly_income"), 0.0) or 0.0 for x in ultimos]
                gastos = [a_flotante(x.get("monthly_expenses"), 0.0) or 0.0 for x in ultimos]
                if ingresos:
                    prom_i = sum(ingresos) / len(ingresos)
                    prom_g = sum(gastos) / len(gastos)
                    ing_3m.append(prom_i)
                    if len(ingresos) > 1:
                        varianza = sum((x - prom_i) ** 2 for x in ingresos) / (len(ingresos) - 1)
                        std_3m.append(varianza ** 0.5)
                    if prom_i > 0:
                        aho_3m.append((prom_i - prom_g) / prom_i)
                        gas_ratio_3m.append(prom_g / prom_i)
            resumen_3m["ingreso_promedio_3m"] = round(sum(ing_3m) / len(ing_3m), 2) if ing_3m else None
            resumen_3m["desviacion_estandar_ingreso_3m"] = round(sum(std_3m) / len(std_3m), 2) if std_3m else None
            resumen_3m["ratio_ahorro_3m"] = round(sum(aho_3m) / len(aho_3m), 4) if aho_3m else None
            resumen_3m["ratio_gasto_ingreso_3m"] = round(sum(gas_ratio_3m) / len(gas_ratio_3m), 4) if gas_ratio_3m else None

    return {
        "disponible": True,
        "total_clientes": len(clientes),
        "ingreso_mensual_promedio_general": promedio_columna(clientes, "avg_monthly_income"),
        "gasto_mensual_promedio_general": promedio_columna(clientes, "avg_monthly_expenses"),
        "deuda_total_promedio": promedio_columna(clientes, "total_debt"),
        "ratio_ahorro_general": promedio_columna(clientes, "savings_rate", 4),
        "ratio_deuda_ingreso_general": promedio_columna(clientes, "debt_to_income_ratio", 4),
        "metricas_ultimos_3_meses": resumen_3m,
        "clientes_por_segmento": por_segmento,
        "top_gastos_por_categoria": top_categorias,
    }


# Construir componentes visuales listos para renderizar en el chat
def construir_visuales(monitoreo, gold):
    fechas = monitoreo.get("serie_por_fecha_local", [])
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
                {"etiqueta": "Clientes analizados", "valor": gold.get("total_clientes", 0)},
                {"etiqueta": "Ingreso prom. 3M (MXN)", "valor": (gold.get("metricas_ultimos_3_meses") or {}).get("ingreso_promedio_3m") or gold.get("ingreso_mensual_promedio_general") or 0},
                {"etiqueta": "Desv. estándar 3M (MXN)", "valor": (gold.get("metricas_ultimos_3_meses") or {}).get("desviacion_estandar_ingreso_3m") or 0},
                {"etiqueta": "Ratio ahorro 3M", "valor": (gold.get("metricas_ultimos_3_meses") or {}).get("ratio_ahorro_3m") or gold.get("ratio_ahorro_general") or 0},
                {"etiqueta": "Ratio gasto/ingreso 3M", "valor": (gold.get("metricas_ultimos_3_meses") or {}).get("ratio_gasto_ingreso_3m") or 0},
                {"etiqueta": "Deuda prom. (MXN)", "valor": gold.get("deuda_total_promedio") or 0},
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

    s3 = cliente_s3()
    filas_interacciones = leer_interacciones_s3(s3)
    monitoreo = calcular_metricas_monitoreo(filas_interacciones)
    gold = calcular_resumen_gold(s3)
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
