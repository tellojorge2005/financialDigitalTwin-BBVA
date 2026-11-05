import { buscar, elemento, texto } from "./ui.js";

const moneda = new Intl.NumberFormat("es-MX", {
    style: "currency", currency: "MXN", minimumFractionDigits: 2, maximumFractionDigits: 2
});
const fechaCorta = new Intl.DateTimeFormat("es-MX", { month: "short", year: "numeric", timeZone: "UTC" });
const fechaLarga = new Intl.DateTimeFormat("es-MX", { month: "long", year: "numeric", timeZone: "UTC" });
const importe = valor => typeof valor === "number" && Number.isFinite(valor) ? moneda.format(valor) : "—";
const firmado = valor => Number.isFinite(valor) ? `${valor > 0 ? "+" : ""}${importe(valor)}` : "—";

// No convertir nulos ni textos vacíos en importes de cero
function numero(valor) {
    if (typeof valor !== "number" && typeof valor !== "string") return null;
    if (typeof valor === "string" && !valor.trim()) return null;
    const resultado = Number(valor);
    return Number.isFinite(resultado) ? resultado : null;
}

function fecha(valor) {
    if (typeof valor !== "string" || !/^\d{4}-\d{2}-01$/.test(valor)) return null;
    const resultado = new Date(`${valor}T00:00:00Z`);
    return Number.isFinite(resultado.getTime()) && resultado.toISOString().slice(0, 10) === valor ? resultado : null;
}

// El backend entrega solo los meses completos del cliente de la sesión
function mesesValidos(filas, proyeccion = false) {
    if (!Array.isArray(filas)) return [];
    const meses = new Set();
    const cortes = new Set();
    const resultado = [];
    for (const fila of filas) {
        if (!fila || fila.currency !== "MXN") return [];
        if (proyeccion && fila.status !== "GENERADO") continue;
        const mes = fecha(proyeccion ? fila.prediction_month : fila.month);
        const ingreso = numero(proyeccion ? fila.income_p50 : fila.monthly_income);
        const gasto = numero(proyeccion ? fila.expenses_p50 : fila.monthly_expenses);
        if (!mes || ingreso === null || gasto === null || ingreso < 0 || gasto < 0) return [];
        if (meses.has(mes.getTime())) return [];
        if (proyeccion) {
            const corte = fecha(fila.feature_month);
            const horizonte = numero(fila.horizon_months);
            if (!corte || ![1, 2, 3].includes(horizonte)) return [];
            cortes.add(corte.getTime());
            if (cortes.size > 1) return [];
            const esperado = new Date(corte);
            esperado.setUTCMonth(esperado.getUTCMonth() + horizonte);
            if (esperado.getTime() !== mes.getTime()) return [];
        }
        meses.add(mes.getTime());
        resultado.push({ mes, ingreso, gasto });
    }
    return resultado.sort((a, b) => a.mes - b.mes);
}

function crearBarra(clase, valor, maximo) {
    const barra = elemento("div", clase);
    barra.style.height = `${maximo > 0 ? valor / maximo * 100 : 0}%`;
    return barra;
}

export function mostrarPanorama(datos = {}) {
    const historial = mesesValidos(datos.monthly_history).slice(-3);
    mostrarResumen(historial);
    const escenarios = Array.isArray(datos.cashflow_scenarios) ? datos.cashflow_scenarios : [];
    const mismoCorte = !historial.length || escenarios.every(fila =>
        fila && fila.feature_month === historial.at(-1).mes.toISOString().slice(0, 10)
    );
    mostrarProyeccion(mismoCorte ? mesesValidos(escenarios, true).slice(0, 3) : []);
}

function mostrarResumen(meses) {
    const chart = buscar("[data-chart]");
    const tabla = buscar("[data-history-table]");
    chart.replaceChildren();
    tabla.replaceChildren();
    chart.hidden = meses.length === 0;
    buscar("[data-history-empty]").hidden = meses.length > 0;
    texto("[data-period]", meses.length
        ? `${fechaCorta.format(meses[0].mes)} – ${fechaCorta.format(meses.at(-1).mes)} · MXN`
        : "Últimos 3 meses · MXN");
    const ingresos = meses.length ? meses.reduce((s, m) => s + m.ingreso, 0) : null;
    const gastos = meses.length ? meses.reduce((s, m) => s + m.gasto, 0) : null;
    texto("[data-income]", importe(ingresos));
    texto("[data-expenses]", importe(gastos));
    texto("[data-net]", meses.length ? firmado(ingresos - gastos) : "—");
    buscar("[data-pane=resumen] .kicker").textContent = meses.length && meses.length < 3
        ? `Balance neto · ${meses.length} ${meses.length === 1 ? "mes disponible" : "meses disponibles"}`
        : "Balance neto · últimos 3 meses";
    buscar("[data-pane=resumen] .quiet").textContent =
        `${meses.length && meses.length < 3 ? "Aún no hay tres meses completos disponibles. " : ""}` +
        "El balance corresponde a ingresos menos gastos del período disponible. No es el saldo de tu cuenta.";
    document.querySelectorAll("[data-pane=resumen] .metric small").forEach(nodo => {
        nodo.textContent = meses.length && meses.length < 3 ? "Período disponible" : "Últimos 3 meses";
    });
    if (!meses.length) return;
    const maximo = Math.max(...meses.flatMap(m => [m.ingreso, m.gasto]));
    chart.style.gridTemplateColumns = `repeat(${meses.length},1fr)`;
    const table = elemento("table", "historytable");
    table.setAttribute("aria-label", "Ingresos y gastos mensuales en pesos mexicanos");
    const head = elemento("thead");
    const encabezados = elemento("tr");
    for (const titulo of ["Mes", "Ingresos", "Gastos"]) {
        const th = elemento("th", "", titulo);
        th.scope = "col";
        encabezados.append(th);
    }
    head.append(encabezados);
    const body = elemento("tbody");
    for (const mes of meses) {
        const grupo = elemento("div", "barsection");
        const par = elemento("div", "barpair");
        par.append(crearBarra("bar inc", mes.ingreso, maximo), crearBarra("bar exp", mes.gasto, maximo));
        grupo.append(par, elemento("span", "monthname", fechaCorta.format(mes.mes).toUpperCase()));
        chart.append(grupo);
        const fila = elemento("tr");
        for (const valor of [fechaCorta.format(mes.mes), importe(mes.ingreso), importe(mes.gasto)]) fila.append(elemento("td", "", valor));
        body.append(fila);
    }
    table.append(head, body);
    tabla.append(table);
}

function mostrarProyeccion(meses) {
    const lista = buscar("[data-forecast-list]");
    lista.replaceChildren();
    buscar("[data-forecast-empty]").hidden = meses.length > 0;
    texto("[data-forecast-net]", meses.length ? firmado(meses[0].ingreso - meses[0].gasto) : "—");
    buscar("[data-pane=proyeccion] .kicker").textContent = meses.length
        ? `Balance previsto · ${fechaLarga.format(meses[0].mes)}` : "Balance previsto · próximo mes";
    texto("[data-forecast-period]", meses.length
        ? `Panorama de ${meses.length} ${meses.length === 1 ? "mes estimado" : "meses estimados"}` : "Panorama de los próximos 3 meses");
    texto("[data-forecast-income]", meses.length ? importe(meses.reduce((s, m) => s + m.ingreso, 0)) : "—");
    texto("[data-forecast-expenses]", meses.length ? importe(meses.reduce((s, m) => s + m.gasto, 0)) : "—");
    const maximo = Math.max(0, ...meses.flatMap(m => [m.ingreso, m.gasto]));
    for (const mes of meses) {
        const card = elemento("div", "forecastcard");
        const header = elemento("div", "forecastheader");
        header.append(elemento("strong", "", fechaLarga.format(mes.mes)), elemento("span", "", firmado(mes.ingreso - mes.gasto)));
        card.append(header);
        for (const [valor, clase] of [[mes.ingreso, "fill"], [mes.gasto, "fill exp"]]) {
            const track = elemento("div", "track");
            track.setAttribute("aria-hidden", "true");
            const fill = elemento("div", clase);
            fill.style.width = `${maximo > 0 ? valor / maximo * 100 : 0}%`;
            track.append(fill);
            card.append(track);
        }
        const footer = elemento("div", "forecastfooter");
        footer.append(elemento("span", "", `Ingreso ${importe(mes.ingreso)}`), elemento("span", "", `Gasto ${importe(mes.gasto)}`));
        card.append(footer);
        lista.append(card);
    }
}
