// Definir perfiles de ejemplo
const perfiles = {
    "demo-001": {
        nombre: "Perfil 001",
        datos: {
            total_credit_limit: 45000,
            average_credit_limit: 15000,
            total_cards: 3,
            total_transactions: 18,
            total_spent: 3250.50,
            total_debt: 12500,
            yearly_income: 180000,
            total_credit_limits_imputed: 1,
            has_imputed_credit_limit: true,
            has_card_records: true,
            has_transaction_records: true
        }
    },

    "demo-002": {
        nombre: "Perfil 002",
        datos: {
            total_credit_limit: 72000,
            average_credit_limit: 36000,
            total_cards: 2,
            total_transactions: 32,
            total_spent: 8460,
            total_debt: 21000,
            yearly_income: 264000,
            total_credit_limits_imputed: 0,
            has_imputed_credit_limit: false,
            has_card_records: true,
            has_transaction_records: true
        }
    },

    "demo-003": {
        nombre: "Perfil 003",
        datos: {
            total_credit_limit: null,
            average_credit_limit: null,
            total_cards: null,
            total_transactions: null,
            total_spent: null,
            total_debt: 0,
            yearly_income: 120000,
            total_credit_limits_imputed: null,
            has_imputed_credit_limit: null,
            has_card_records: false,
            has_transaction_records: false
        }
    }
};


// Seleccionar elementos
const selectorPerfil = document.getElementById("profile");
const perfilActivo = document.getElementById("active-profile");
const mensajes = document.getElementById("messages");
const formulario = document.getElementById("chat-form");
const pregunta = document.getElementById("question");
const botonEnviar = document.getElementById("send-button");
const botonLimpiar = document.getElementById("clear-chat");
const contador = document.getElementById("character-count");
const errorEntrada = document.getElementById("input-error");
const sugerencias = document.querySelectorAll("[data-question]");


// Formatear importes
const formatoImporte = new Intl.NumberFormat("es-MX", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
});

function formatearImporte(valor) {
    return valor === null || valor === undefined
        ? "No disponible"
        : formatoImporte.format(valor);
}


// Obtener perfil
function obtenerPerfil() {
    return perfiles[selectorPerfil.value];
}


// Mostrar resumen
function mostrarResumen() {
    const perfil = obtenerPerfil();
    const datos = perfil.datos;

    perfilActivo.textContent =
        `${perfil.nombre} · Datos de ejemplo`;

    document.getElementById("credit-limit").textContent =
        formatearImporte(datos.total_credit_limit);

    document.getElementById("transaction-count").textContent =
        datos.total_transactions ?? "—";

    document.getElementById("card-count").textContent =
        datos.total_cards ?? "—";

    document.getElementById("transaction-amount").textContent =
        formatearImporte(datos.total_spent);

    document.getElementById("total-debt").textContent =
        formatearImporte(datos.total_debt);

    document.getElementById("yearly-income").textContent =
        formatearImporte(datos.yearly_income);

    const nota = document.getElementById("credit-note");

    if (!datos.has_card_records) {
        nota.textContent = "Sin registros de tarjetas";
    } else if (datos.has_imputed_credit_limit === true) {
        nota.textContent =
            `Incluye ${datos.total_credit_limits_imputed} límite estimado`;
    } else if (datos.has_imputed_credit_limit === false) {
        nota.textContent = "Sin límites marcados como estimados";
    } else {
        nota.textContent = "Procedencia de los límites no disponible";
    }
}


// Agregar mensaje
function agregarMensaje(autor, texto) {
    const mensaje = document.createElement("article");
    const etiqueta = document.createElement("div");
    const contenido = document.createElement("div");
    const parrafo = document.createElement("p");

    mensaje.classList.add(
        "message",
        autor === "user"
            ? "message-user"
            : "message-assistant"
    );

    etiqueta.className = "message-author";
    etiqueta.textContent = autor === "user"
        ? "Tú"
        : "Asistente · Demostración";

    contenido.className = "message-bubble";
    parrafo.textContent = texto;

    contenido.appendChild(parrafo);
    mensaje.append(etiqueta, contenido);
    mensajes.appendChild(mensaje);

    mensajes.scrollTop = mensajes.scrollHeight;
}


// Normalizar pregunta
function normalizarTexto(texto) {
    return texto
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
}


// Explicar estimaciones
function explicarEstimaciones(datos) {
    if (!datos.has_card_records) {
        return (
            "No hay registros de tarjetas para este perfil. " +
            "No se puede determinar un límite total ni sus estimaciones."
        );
    }

    if (datos.has_imputed_credit_limit === true) {
        return (
            `Hay ${datos.total_credit_limits_imputed} límite de tarjeta ` +
            "completado con una estimación. El límite total y el promedio " +
            "incluyen ese valor. Una estimación no es una aprobación de crédito."
        );
    }

    if (datos.has_imputed_credit_limit === false) {
        return (
            "No hay límites marcados como estimados en este resumen. " +
            "Esto no significa que todos los demás campos hayan sido verificados."
        );
    }

    return "No hay información sobre la procedencia de los límites.";
}


// Consultar información de ejemplo
function consultarInformacion(texto, datos) {
    const consulta = normalizarTexto(texto);

    if (/^(hola|buenas|buenos dias|buenas tardes|buenas noches)[!. ]*$/.test(consulta)) {
        return (
            "Hola. Puedes preguntarme por los límites de crédito, " +
            "las transacciones, la deuda o los ingresos del perfil seleccionado."
        );
    }

    if (
        /\b(aprobar|aprobacion|prestamo|solvencia|recomiendas|recomendacion)\b/.test(consulta) ||
        /puedo (obtener|pedir)|me (darian|prestarias|aprobarian)/.test(consulta)
    ) {
        return (
            "Este prototipo explica datos del resumen. " +
            "No evalúa solicitudes, recomienda créditos ni predice aprobaciones."
        );
    }

    if (
        /\b(nombre|direccion|telefono|correo|cvv|nip)\b/.test(consulta) ||
        /numero de (mi |la )?tarjeta/.test(consulta)
    ) {
        return (
            "Esta demostración no contiene nombres, datos de contacto, " +
            "números de tarjeta ni credenciales."
        );
    }

    if (
        /\b(mensual|mensuales|mes|ayer|hoy|semana|semanal|ultima|ultimas|reciente)\b/.test(consulta) ||
        /gasto anual|gaste en|transacciones de/.test(consulta)
    ) {
        return (
            "El resumen no contiene el periodo ni el detalle individual de " +
            "las transacciones. No puedo identificar gastos por fecha, " +
            "comercio ni movimientos específicos."
        );
    }

    if (
        /\b(saldo|disponible|disponibles|moneda|pesos|dolares|mxn|usd)\b/.test(consulta)
    ) {
        return (
            "La moneda y el saldo disponible no están especificados. " +
            "El límite de crédito agregado no equivale al dinero disponible."
        );
    }

    const respuestas = [];

    const consultaEstimaciones =
        /\b(estimado|estimados|estimada|estimadas|estimacion|estimaciones|imputado|imputados)\b/.test(consulta);

    const consultaLimites =
        /\b(limite|limites)\b/.test(consulta);

    const consultaTransacciones =
        /\b(transaccion|transacciones|movimientos|operaciones|gasto|gastos|gastado|gaste)\b/.test(consulta);

    const consultaTarjetas =
        /\b(tarjeta|tarjetas)\b/.test(consulta);

    const consultaDeuda =
        /\b(deuda|deudas|debo|adeudo)\b/.test(consulta);

    const consultaIngreso =
        /\b(ingreso|ingresos|gano|salario)\b/.test(consulta);

    if (consultaLimites) {
        if (!datos.has_card_records || datos.total_credit_limit === null) {
            respuestas.push(
                "No hay un límite de crédito disponible para este perfil."
            );
        } else {
            const esPromedio = /\b(promedio|medio)\b/.test(consulta);
            const valor = esPromedio
                ? datos.average_credit_limit
                : datos.total_credit_limit;

            respuestas.push(
                esPromedio
                    ? `El límite promedio por tarjeta es ${formatearImporte(valor)}.`
                    : `El límite agregado de las tarjetas es ${formatearImporte(valor)}.`
            );

            respuestas.push(explicarEstimaciones(datos));

            respuestas.push(
                "La moneda no está especificada y este importe no representa saldo disponible."
            );
        }
    }

    if (consultaEstimaciones && !consultaLimites) {
        respuestas.push(explicarEstimaciones(datos));
    }

    if (consultaTransacciones) {
        if (!datos.has_transaction_records) {
            respuestas.push(
                "No hay registros de transacciones disponibles. " +
                "Esto no significa que el perfil nunca haya realizado operaciones."
            );
        } else {
            respuestas.push(
                `Hay ${datos.total_transactions} transacciones registradas, ` +
                `con una suma de ${formatearImporte(datos.total_spent)}. ` +
                "La moneda y el periodo no están especificados."
            );
        }
    }

    if (consultaTarjetas && !consultaLimites && !consultaEstimaciones) {
        respuestas.push(
            datos.has_card_records
                ? `Hay ${datos.total_cards} registros de tarjetas asociados al perfil.`
                : "No hay registros de tarjetas asociados al perfil."
        );
    }

    if (consultaDeuda) {
        respuestas.push(
            datos.total_debt === null
                ? "La deuda registrada no está disponible."
                : `La deuda registrada es ${formatearImporte(datos.total_debt)}. ` +
                  "La moneda no está especificada."
        );
    }

    if (consultaIngreso) {
        respuestas.push(
            datos.yearly_income === null
                ? "El ingreso anual no está disponible."
                : `El ingreso anual registrado es ${formatearImporte(datos.yearly_income)}. ` +
                  "La moneda no está especificada."
        );
    }

    if (respuestas.length > 0) {
        return respuestas.join("\n\n");
    }

    return (
        "Esta demostración reconoce preguntas sencillas sobre límites, " +
        "tarjetas, transacciones, deuda, ingresos y estimaciones. " +
        "Todavía no interpreta cualquier pregunta ni recuerda mensajes anteriores. " +
        "Prueba con una de las preguntas sugeridas."
    );
}


// Actualizar entrada
function actualizarEntrada() {
    contador.textContent = `${pregunta.value.length} / 2000`;
    botonEnviar.disabled = pregunta.value.trim().length === 0;
    errorEntrada.textContent = "";
}


// Iniciar conversación
function iniciarConversacion() {
    mensajes.replaceChildren();
    pregunta.value = "";
    actualizarEntrada();

    agregarMensaje(
        "assistant",
        `Estás consultando ${obtenerPerfil().nombre}.\n\n` +
        "Puedo mostrarte ejemplos de respuestas sobre su información financiera. " +
        "Escribe una pregunta o utiliza las sugerencias de abajo."
    );
}


// Procesar consulta
formulario.addEventListener("submit", (event) => {
    event.preventDefault();

    const texto = pregunta.value.trim();

    if (!texto || texto.length > 2000) {
        errorEntrada.textContent =
            "Escribe una pregunta de entre 1 y 2000 caracteres.";
        return;
    }

    agregarMensaje("user", texto);

    const respuesta = consultarInformacion(
        texto,
        obtenerPerfil().datos
    );

    agregarMensaje("assistant", respuesta);

    pregunta.value = "";
    actualizarEntrada();
    pregunta.focus();
});


// Actualizar pregunta
pregunta.addEventListener("input", actualizarEntrada);


// Seleccionar pregunta sugerida
sugerencias.forEach((boton) => {
    boton.addEventListener("click", () => {
        pregunta.value = boton.dataset.question;
        actualizarEntrada();
        pregunta.focus();
    });
});


// Cambiar perfil
selectorPerfil.addEventListener("change", () => {
    mostrarResumen();
    iniciarConversacion();
});


// Limpiar conversación
botonLimpiar.addEventListener("click", () => {
    iniciarConversacion();
    pregunta.focus();
});


// Cargar información
mostrarResumen();
iniciarConversacion();