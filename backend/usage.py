import os
import re
from decimal import Decimal, InvalidOperation
from time import perf_counter

from backend.config import OPENAI_MODEL


# Tarifas estándar de texto en USD por millón de tokens, consultadas 2026-11-05
# https://developers.openai.com/api/docs/models/gpt-4o-mini
TARIFAS = {
    "gpt-4o-mini": (Decimal("0.15"), Decimal("0.075"), Decimal("0.60")),
    "gpt-4o-mini-2024-07-18": (Decimal("0.15"), Decimal("0.075"), Decimal("0.60")),
}

CAMPOS_USO = (
    "version_metricas", "modelos_usados", "llamadas_con_uso", "llamadas_con_costo",
    "uso_completo", "costo_completo", "tokens_entrada", "tokens_salida",
    "tokens_cache", "tokens_totales", "tokens_entrada_observados",
    "tokens_salida_observados", "tokens_cache_observados",
    "promedio_tokens_entrada_llamada", "promedio_tokens_salida_llamada",
    "costo_observado_usd", "costo_total_usd", "promedio_costo_llamada_usd",
    "duracion_modelo_segundos", "promedio_duracion_llamada_segundos",
    "tarifa_fuente", "tarifa_fecha", "moneda_costo",
)


def tarifas(modelo):
    # Una tarifa personalizada solo se aplica al modelo indicado, no a cualquiera
    configurado = os.getenv("OPENAI_PRICING_MODEL", "").strip()
    valores = [os.getenv(c, "").strip() for c in (
        "OPENAI_INPUT_USD_PER_1M", "OPENAI_CACHED_INPUT_USD_PER_1M", "OPENAI_OUTPUT_USD_PER_1M"
    )]
    coincide = configurado and (
        modelo == configurado or re.fullmatch(re.escape(configurado) + r"-\d{4}-\d{2}-\d{2}", modelo)
    )
    if coincide and all(valores):
        try:
            precios = tuple(Decimal(v) for v in valores)
            if all(p.is_finite() and p >= 0 for p in precios):
                return precios, "configuracion", os.getenv("OPENAI_PRICING_DATE", "")
        except InvalidOperation:
            pass
        return None
    if modelo in TARIFAS:
        return TARIFAS[modelo], "openai_gpt4o_mini_standard", "2026-11-05"
    return None


def entero(valor):
    return valor if type(valor) is int and valor >= 0 else None


# Contar cada respuesta una sola vez aunque aparezca en dos metadatos
def extraer_uso(respuesta):
    grupos = getattr(respuesta, "generations", []) or []
    mensaje = getattr(grupos[0][0], "message", None) if grupos and grupos[0] else None
    metadata = getattr(mensaje, "response_metadata", {}) or {}
    llm_output = getattr(respuesta, "llm_output", {}) or {}
    modelo = metadata.get("model_name") or metadata.get("model") or llm_output.get("model_name")
    tier = metadata.get("service_tier") or llm_output.get("service_tier") or "default"
    uso = getattr(mensaje, "usage_metadata", None)
    if uso:
        entrada, salida = entero(uso.get("input_tokens")), entero(uso.get("output_tokens"))
        cache = entero((uso.get("input_token_details") or {}).get("cache_read", 0))
    else:
        uso = metadata.get("token_usage") or llm_output.get("token_usage") or {}
        entrada, salida = entero(uso.get("prompt_tokens")), entero(uso.get("completion_tokens"))
        cache = entero((uso.get("prompt_tokens_details") or {}).get("cached_tokens", 0))
    if entrada is None or salida is None or cache is None or cache > entrada:
        return modelo, tier, None
    return modelo, tier, (entrada, salida, cache)


class UsoModelo:
    def __init__(self):
        self.inicios = {}
        self.finalizados = set()
        self.modelos = set()
        self.fuentes = set()
        self.fechas = set()
        self.con_uso = 0
        self.con_costo = 0
        self.entrada = self.salida = self.cache = 0
        self.costo = Decimal(0)
        self.duracion = 0.0

    def iniciar(self, run_id, parametros):
        self.inicios.setdefault(run_id, (perf_counter(), parametros.get("model") or OPENAI_MODEL))

    def cerrar(self, run_id):
        if run_id in self.finalizados:
            return None
        self.finalizados.add(run_id)
        inicio, modelo = self.inicios.get(run_id, (perf_counter(), OPENAI_MODEL))
        self.duracion += max(perf_counter() - inicio, 0)
        return modelo

    def fallar(self, run_id):
        self.cerrar(run_id)

    def terminar(self, run_id, respuesta):
        previsto = self.cerrar(run_id)
        if previsto is None:
            return
        modelo, tier, uso = extraer_uso(respuesta)
        modelo = str(modelo or previsto)
        self.modelos.add(modelo)
        if uso is None:
            return
        entrada, salida, cache = uso
        self.con_uso += 1
        self.entrada += entrada
        self.salida += salida
        self.cache += cache
        precio = tarifas(modelo)
        # No aplicar tarifas estándar a niveles especiales sin conocer el precio
        if precio is not None and tier in ("default", "standard"):
            (p_entrada, p_cache, p_salida), fuente, fecha = precio
            self.costo += ((entrada - cache) * p_entrada + cache * p_cache + salida * p_salida) / Decimal(1000000)
            self.con_costo += 1
            self.fuentes.add(fuente)
            self.fechas.add(fecha)

    def resumen(self):
        llamadas = len(self.inicios)
        uso_completo = self.con_uso == llamadas == len(self.finalizados)
        costo_completo = self.con_costo == llamadas == len(self.finalizados)
        return {
            "version_metricas": 2, "modelos_usados": ";".join(sorted(self.modelos)),
            "llamadas_con_uso": self.con_uso, "llamadas_con_costo": self.con_costo,
            "uso_completo": int(uso_completo), "costo_completo": int(costo_completo),
            "tokens_entrada": self.entrada if uso_completo else None,
            "tokens_salida": self.salida if uso_completo else None,
            "tokens_cache": self.cache if uso_completo else None,
            "tokens_totales": self.entrada + self.salida if uso_completo else None,
            "tokens_entrada_observados": self.entrada,
            "tokens_salida_observados": self.salida,
            "tokens_cache_observados": self.cache,
            "promedio_tokens_entrada_llamada": self.entrada / self.con_uso if self.con_uso else None,
            "promedio_tokens_salida_llamada": self.salida / self.con_uso if self.con_uso else None,
            "costo_observado_usd": float(self.costo),
            "costo_total_usd": float(self.costo) if costo_completo else None,
            "promedio_costo_llamada_usd": float(self.costo / self.con_costo) if self.con_costo else None,
            "duracion_modelo_segundos": round(self.duracion, 3),
            "promedio_duracion_llamada_segundos": round(self.duracion / len(self.finalizados), 3) if self.finalizados else None,
            "tarifa_fuente": ";".join(sorted(self.fuentes)),
            "tarifa_fecha": ";".join(sorted(self.fechas)), "moneda_costo": "USD",
        }
