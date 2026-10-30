from datetime import date
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Literal

import re
from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.guardrails import entrada_restringida


class CambioMeta(BaseModel):
    model_config = ConfigDict(extra="forbid")
    nombre: str | None = Field(default=None, min_length=1, max_length=150)
    monto: Decimal | None = Field(default=None, gt=0, le=1000000000, decimal_places=2)
    plazo: int | None = Field(default=None, strict=True, ge=1, le=120)
    ahorro: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    margen: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    moneda: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    usar_historial: bool | None = Field(default=None, strict=True)
    ingreso: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    gastos: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    pagos: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    evaluar_presupuesto: bool | None = Field(default=None, strict=True)

    @field_validator("monto", "ahorro", "margen", "ingreso", "gastos", "pagos", mode="before")
    @classmethod
    def normalizar_importe(cls, valor):
        return normalizar_monto(valor)

    @field_validator("moneda", mode="before")
    @classmethod
    def normalizar_moneda(cls, valor):
        return normalizar_divisa(valor)


class Decision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accion: Literal[
        "consulta", "meta", "nueva_meta", "cancelar_meta", "educacion", "rechazar",
        "escenario", "nuevo_escenario"
    ]
    cambios: CambioMeta = Field(default_factory=CambioMeta)
    respuesta: str = Field(default="", max_length=2400)
    escenario: "CambioEscenario" = Field(default_factory=lambda: CambioEscenario())


def importe(valor):
    return Decimal(str(valor))


def mostrar(valor):
    return f"{importe(valor):,.2f}"


def actualizar_meta(meta, cambios):
    nueva = dict(meta)
    moneda_nueva = cambios.moneda
    if moneda_nueva and nueva.get("moneda") not in (None, moneda_nueva):
        for campo in ("monto", "ahorro", "margen", "ingreso", "gastos", "pagos", "usar_historial"):
            nueva.pop(campo, None)
    for campo, valor in cambios.model_dump(mode="json", exclude_unset=True).items():
        if valor is not None:
            if campo == "nombre" and entrada_restringida(valor):
                raise ValueError("Nombre no permitido.")
            nueva[campo] = valor
            nueva["confirmada"] = False
    return nueva


def referencia_historial(historial, moneda):
    filas = sorted(
        [fila for fila in historial if fila["currency"] == moneda],
        key=lambda fila: fila["month"]
    )[-3:]
    if len(filas) != 3:
        return None
    fechas = [date.fromisoformat(fila["month"][:10]) for fila in filas]
    periodos = [f.year * 12 + f.month for f in fechas]
    if periodos != list(range(periodos[0], periodos[0] + 3)):
        return None
    campos = [
        "monthly_income", "monthly_expenses", "monthly_debt_payments",
        "closing_balance", "outstanding_due"
    ]
    if any(f.get(c) is None for f in filas for c in campos):
        return None
    promedios = {
        campo: sum(importe(f[campo]) for f in filas) / 3
        for campo in campos[:3]
    }
    return {
        "ingreso": promedios["monthly_income"],
        "gastos": promedios["monthly_expenses"],
        "pagos": promedios["monthly_debt_payments"],
        "saldo": importe(filas[-1]["closing_balance"]),
        "vencido": importe(filas[-1]["outstanding_due"]),
        "desde": filas[0]["month"][:7],
        "hasta": filas[-1]["month"][:7]
    }


def siguiente_pregunta(meta, perfil, historial):
    preguntas = [
        ("nombre", "¿Para qué te gustaría ahorrar?"),
        ("monto", "¿Cuánto dinero necesitas reunir en total?"),
        ("moneda", "¿En qué moneda quieres definir la meta?"),
        ("plazo", "¿En cuántos meses te gustaría reunirlo?"),
        ("ahorro", "¿Cuánto tienes ya apartado exclusivamente para esta meta? Puede ser cero."),
        ("margen", "Además de tus gastos habituales, ¿cuánto deseas reservar cada mes para imprevistos? Puede ser cero.")
    ]
    for campo, pregunta in preguntas:
        if meta.get(campo) is None:
            if campo == "moneda" and perfil.get("currency"):
                return f"¿La meta también está en {perfil['currency']}?"
            return pregunta

    ref = referencia_historial(historial, meta["moneda"])
    if meta.get("usar_historial") is None:
        if ref:
            return (
                f"Tengo información de {ref['desde']} a {ref['hasta']}. "
                "¿Quieres usarla como referencia para un escenario de ahorro, "
                "o prefieres indicar tus ingresos y gastos mensuales actuales?"
            )
        return (
            "No tengo tres meses consecutivos completos en esa moneda. "
            "¿Quieres hacer el cálculo con los ingresos, gastos y pagos "
            "mensuales que tú me indiques?"
        )

    if meta["usar_historial"] and ref is None:
        return (
            "No dispongo del historial necesario para esa moneda. "
            "¿Prefieres utilizar un presupuesto mensual que tú me indiques?"
        )

    if not meta["usar_historial"]:
        for campo, pregunta in [
            ("ingreso", "¿Cuánto recibes al mes, después de descuentos?"),
            ("gastos", "¿Cuánto gastas al mes, sin contar los pagos de deuda?"),
            ("pagos", "¿Cuánto pagas al mes por tus deudas? Puede ser cero.")
        ]:
            if meta.get(campo) is None:
                return pregunta
    return None


# Mantener los cálculos fuera del modelo de lenguaje
def calcular_plan(meta, perfil, historial):
    ref = referencia_historial(historial, meta["moneda"])
    if meta["usar_historial"]:
        if ref is None:
            raise ValueError("Información insuficiente.")
        ingreso, gastos, pagos = ref["ingreso"], ref["gastos"], ref["pagos"]
        advertencia = (
            f"Referencia histórica: {ref['desde']} a {ref['hasta']}; "
            "no describe necesariamente tu situación actual."
        )
        revisar = ref["saldo"] < 0 or ref["vencido"] > 0
    else:
        ingreso, gastos, pagos = (importe(meta[c]) for c in ("ingreso", "gastos", "pagos"))
        advertencia = "Presupuesto mensual declarado por ti; no está verificado."
        revisar = False

    pendiente = max(importe(meta["monto"]) - importe(meta["ahorro"]), Decimal(0))
    capacidad = max(
        ingreso - gastos - pagos - importe(meta["margen"]), Decimal(0)
    ).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    aporte = (pendiente / meta["plazo"]).quantize(
        Decimal("0.01"), rounding=ROUND_CEILING
    )
    meses = (
        int((pendiente / capacidad).to_integral_value(rounding=ROUND_CEILING))
        if capacidad > 0 and not revisar else None
    )
    moneda = meta["moneda"]
    texto = (
        f"Para «{meta['nombre']}», tu objetivo es {mostrar(meta['monto'])} {moneda}. "
        f"Con {mostrar(meta['ahorro'])} ya apartados, faltan "
        f"{mostrar(pendiente)} {moneda}.\n\n"
        f"En {meta['plazo']} meses necesitarías apartar "
        f"{mostrar(aporte)} {moneda} al mes. Después de gastos, pagos de deuda "
        f"y la reserva para imprevistos, la capacidad de referencia es "
        f"{mostrar(capacidad)} {moneda} mensuales.\n\n"
    )
    if pendiente == 0:
        texto += "El ahorro que declaraste ya cubre el objetivo."
    elif revisar:
        texto += (
            "El historial presenta saldo negativo o cuotas vencidas. "
            "Necesitamos revisar esos compromisos antes de considerar viable "
            "la aportación; este cálculo no incluye regularizar atrasos."
        )
    elif aporte <= capacidad:
        texto += "La aportación cabe en este presupuesto de referencia."
    elif capacidad > 0:
        texto += (
            f"Faltarían {mostrar(aporte - capacidad)} {moneda} mensuales. "
            f"Manteniendo esa capacidad, el plazo calculado sería de {meses} meses. "
            "Podemos revisar gastos ajustables, ampliar el plazo o reducir el objetivo."
        )
    else:
        texto += (
            "Este presupuesto no deja capacidad positiva de ahorro. "
            "Primero conviene revisar gastos y compromisos; ampliar el plazo "
            "por sí solo no resuelve esa falta de capacidad."
        )
    texto += (
        f"\n\n{advertencia} El escenario no incluye rendimientos ni inflación. "
        "Conviene revisar el avance y actualizar el presupuesto cuando cambie tu situación."
    )
    return texto

# Interpretar magnitudes no equivale a convertir entre monedas
def normalizar_monto(valor):
    if not isinstance(valor, str):
        return valor
    texto = valor.strip().lower()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(mil|miles|k)", texto)
    if match:
        return Decimal(match[1]) * 1000
    # Los separadores ambiguos no se reinterpretan silenciosamente.
    return texto


def normalizar_divisa(valor):
    if valor is None:
        return None
    texto = str(valor).strip().lower()
    return {
        "dólares": "USD", "dolares": "USD", "dólar": "USD",
        "dolar": "USD", "usd": "USD", "us$": "USD",
        "pesos mexicanos": "MXN", "mxn": "MXN", "euros": "EUR", "eur": "EUR"
    }.get(texto, texto.upper())


class CambioEscenario(BaseModel):
    model_config = ConfigDict(extra="forbid")
    moneda: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    ahorro: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    gastos: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    pagos: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)
    ingreso_restante: Decimal | None = Field(default=None, ge=0, le=1000000000, decimal_places=2)

    @field_validator("ahorro", "gastos", "pagos", "ingreso_restante", mode="before")
    @classmethod
    def normalizar_importe(cls, valor):
        return normalizar_monto(valor)

    @field_validator("moneda", mode="before")
    @classmethod
    def normalizar_moneda(cls, valor):
        return normalizar_divisa(valor)


Decision.model_rebuild()


def preparar_escenario(estado, cambios):
    nuevo = dict(estado)
    valores = cambios.model_dump(mode="json", exclude_unset=True)
    moneda_nueva = valores.get("moneda")
    if moneda_nueva and nuevo.get("moneda") not in (None, moneda_nueva):
        # Un cambio de moneda no convierte automáticamente los importes previos.
        nuevo = {}
    nuevo.update({k: v for k, v in valores.items() if v is not None})
    return nuevo


def explicar_salida_trabajo(escenario):
    introduccion = (
        "Dejar el trabajo puede reducir lo que te queda cada mes y obligarte "
        "a usar tus ahorros. Lo importante es comparar los ingresos que "
        "conservarías con tus gastos y pagos de deuda."
    )
    if escenario.get("ingreso_restante") is None:
        return introduccion + (
            " ¿Te quedarías sin ingresos o conservarías alguna cantidad mensual?"
        )
    if escenario.get("gastos") is None or escenario.get("pagos") is None:
        return (
            "Para ponerle números, ¿cuánto gastas al mes y cuánto pagas "
            "por deudas, por separado? Si no tienes pagos de deuda, dime cero."
        )
    if escenario.get("moneda") is None:
        return "¿En qué moneda están esas cantidades? Si son pesos, dime de qué país."
    if escenario.get("ahorro") is None:
        return "¿Cuánto ahorro podrías usar para mantenerte mientras consigues otro ingreso?"

    gastos = importe(escenario["gastos"]) + importe(escenario["pagos"])
    restante = importe(escenario["ingreso_restante"])
    ahorro = importe(escenario["ahorro"])
    deficit = gastos - restante
    moneda = escenario["moneda"]
    if deficit > 0:
        meses = (ahorro / deficit).quantize(Decimal("0.1"), rounding=ROUND_DOWN)
        resultado = (
            f"Con lo que me indicas, saldrían {mostrar(gastos)} {moneda} al mes "
            f"y conservarías {mostrar(restante)} {moneda} de ingresos. "
            f"La diferencia sería de {mostrar(deficit)} {moneda} mensuales. "
            f"Tus {mostrar(ahorro)} {moneda} de ahorro cubrirían aproximadamente "
            f"{meses} meses a ese ritmo."
        )
    else:
        resultado = (
            f"Los ingresos que conservarías cubrirían los "
            f"{mostrar(gastos)} {moneda} de gastos y pagos mensuales. "
            f"El margen sería de {mostrar(-deficit)} {moneda} al mes."
        )
    return resultado + (
        " Es un escenario con las cifras que me diste, no una garantía. "
        "No incluye liquidación, prestaciones que perderías, inflación ni gastos "
        "extraordinarios. Antes de decidir, conviene considerar esos cambios "
        "y cuánto tiempo podrías tardar en obtener otro ingreso."
    )


def orientar_meta(meta, perfil, historial):
    # Ofrecer primero un cálculo útil; el presupuesto detallado es opcional.
    for campo, pregunta in [
        ("nombre", "Cuéntame, ¿para qué te gustaría ahorrar?"),
        ("monto", "¿Cuánto costaría aproximadamente lo que quieres conseguir?"),
        ("moneda", "¿En qué moneda? Si son pesos, dime de qué país."),
        ("plazo", "¿Para cuándo te gustaría tenerlo? Puedes decirme en cuántos meses.")
    ]:
        if meta.get(campo) is None:
            return pregunta, False

    objetivo = importe(meta["monto"])
    ahorro = importe(meta.get("ahorro", 0))
    pendiente = max(objetivo - ahorro, Decimal(0))
    mensual = (pendiente / meta["plazo"]).quantize(
        Decimal("0.01"), rounding=ROUND_CEILING
    )
    texto = (
        f"Para «{meta['nombre']}», reunir {mostrar(objetivo)} {meta['moneda']} "
        f"en {meta['plazo']} meses supone apartar "
        f"{mostrar(mensual)} {meta['moneda']} al mes"
    )
    if meta.get("ahorro") is None:
        return texto + (
            " si empiezas desde cero. ¿Ya tienes algo apartado? "
            "Con eso puedo ajustar la cantidad."
        ), False

    texto += f", contando los {mostrar(ahorro)} que ya apartaste."
    if not meta.get("evaluar_presupuesto"):
        return texto + (
            " Este cálculo todavía no comprueba si cabe en tu presupuesto. "
            "¿Quieres que también revisemos eso con tus ingresos y gastos actuales?"
        ), True

    # Solo usar cifras históricas si el usuario las solicitó expresamente.
    if meta.get("usar_historial") is True:
        ref = referencia_historial(historial, meta["moneda"])
        if ref is None:
            return (
                "No tengo historial suficiente en esa moneda. "
                "Podemos usar un presupuesto actual que tú me indiques."
            ), False
    else:
        if meta.get("ingreso") is None or meta.get("gastos") is None:
            return (
                "¿Cuánto recibes al mes y cuánto gastas normalmente, "
                "sin contar los pagos de deuda?"
            ), False
        if meta.get("pagos") is None:
            return "¿Cuánto pagas al mes por deudas? Si no tienes, dime cero.", False

    calculo = dict(meta)
    calculo.setdefault("usar_historial", False)
    calculo.setdefault("margen", "0")
    texto = calcular_plan(calculo, perfil, historial)
    if "margen" not in meta:
        texto += (
            " No he descontado una reserva adicional para imprevistos; "
            "si quieres apartar una cantidad, puedo incorporarla."
        )
    return texto, True



