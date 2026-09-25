// Seleccionar elementos
const loginView = document.getElementById("login-view");
const appView = document.getElementById("app-view");
const loginForm = document.getElementById("login-form");
const clientIdInput = document.getElementById("client-id");
const loginButton = document.getElementById("login-button");
const loginError = document.getElementById("login-error");
const logoutButton = document.getElementById("logout-button");
const sessionError = document.getElementById("session-error");

const mensajes = document.getElementById("messages");
const formulario = document.getElementById("chat-form");
const pregunta = document.getElementById("question");
const botonEnviar = document.getElementById("send-button");
const botonLimpiar = document.getElementById("clear-chat");
const contador = document.getElementById("character-count");
const errorEntrada = document.getElementById("input-error");
const estadoChat = document.getElementById("chat-status");
const sugerencias = document.querySelectorAll("[data-question]");


// Definir estado
let clienteId = null;
let ocupado = true;


// Formatear importes
const formatoImporte = new Intl.NumberFormat("es-MX", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
});

function formatearImporte(valor) {
    return typeof valor === "number" && Number.isFinite(valor)
        ? formatoImporte.format(valor)
        : "No disponible";
}


// Consultar backend
async function consultarApi(ruta, opciones = {}) {
    const controlador = new AbortController();
    const timeout = window.setTimeout(() => {
        controlador.abort();
    }, 90000);

    try {
        const response = await fetch(ruta, {
            ...opciones,
            credentials: "same-origin",
            signal: controlador.signal,
            headers: {
                "Accept": "application/json",
                ...(opciones.body
                    ? { "Content-Type": "application/json" }
                    : {})
            }
        });

        const datos = response.status === 204
            ? null
            : await response.json().catch(() => null);

        if (!response.ok) {
            const mensaje = typeof datos?.detail === "string"
                ? datos.detail
                : "No se pudo procesar la solicitud.";

            const error = new Error(mensaje);
            error.status = response.status;
            throw error;
        }

        return datos;

    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error(
                "La solicitud tardó demasiado. Intenta nuevamente."
            );
        }

        if (error instanceof TypeError) {
            throw new Error(
                "No se pudo conectar con el servidor."
            );
        }

        throw error;

    } finally {
        window.clearTimeout(timeout);
    }
}


// Actualizar controles
function actualizarControles() {
    clientIdInput.disabled = ocupado;

    loginButton.disabled =
        ocupado || !clientIdInput.value.trim();

    logoutButton.disabled = ocupado;
    botonLimpiar.disabled = ocupado || clienteId === null;
    pregunta.disabled = ocupado || clienteId === null;

    botonEnviar.disabled =
        ocupado ||
        clienteId === null ||
        !pregunta.value.trim();

    sugerencias.forEach((boton) => {
        boton.disabled = ocupado || clienteId === null;
    });

    contador.textContent = `${pregunta.value.length} / 2000`;
    formulario.setAttribute("aria-busy", String(ocupado));
}


function cambiarEstado(valor) {
    ocupado = valor;
    actualizarControles();
}


// Mostrar resumen
function mostrarResumen(datos) {
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

    if (datos.has_card_records === false) {
        nota.textContent = "Sin registros de tarjetas";
    } else if (datos.has_imputed_credit_limit === true) {
        const cantidad = datos.total_credit_limits_imputed;

        nota.textContent = cantidad == null
            ? "Incluye límites estimados"
            : `Incluye ${cantidad} límite(s) estimado(s)`;
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
    etiqueta.textContent = autor === "user" ? "Tú" : "Asistente";

    contenido.className = "message-bubble";
    parrafo.textContent = texto;

    contenido.appendChild(parrafo);
    mensaje.append(etiqueta, contenido);
    mensajes.appendChild(mensaje);

    mensajes.scrollTop = mensajes.scrollHeight;

    return mensaje;
}


// Iniciar conversación
function iniciarConversacion() {
    mensajes.replaceChildren();
    pregunta.value = "";
    errorEntrada.textContent = "";
    estadoChat.textContent = "";

    agregarMensaje(
        "assistant",
        `Estás consultando el cliente ${clienteId}.\n\n` +
        "Puedes preguntar por los límites de crédito, las transacciones, " +
        "la deuda, los ingresos y los valores estimados de su resumen."
    );

    actualizarControles();
}


// Mostrar sesión
function mostrarSesion(datos) {
    clienteId = datos.cliente_id;

    document.getElementById("session-client").textContent =
        `Cliente ${clienteId}`;

    document.getElementById("active-profile").textContent =
        `Cliente ${clienteId}`;

    mostrarResumen(datos.datos_cliente);

    loginView.hidden = true;
    appView.hidden = false;
    loginError.textContent = "";
    sessionError.textContent = "";
    clientIdInput.value = "";

    iniciarConversacion();
}


// Mostrar inicio de sesión
function mostrarLogin(mensaje = "") {
    clienteId = null;
    mensajes.replaceChildren();
    pregunta.value = "";
    errorEntrada.textContent = "";
    estadoChat.textContent = "";
    sessionError.textContent = "";

    mostrarResumen({});

    document.getElementById("session-client").textContent = "—";
    document.getElementById("active-profile").textContent = "—";

    appView.hidden = true;
    loginView.hidden = false;
    loginError.textContent = mensaje;

    actualizarControles();
}


// Iniciar sesión
loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (ocupado) {
        return;
    }

    const texto = clientIdInput.value.trim();
    const id = Number(texto);

    if (!/^\d+$/.test(texto) || !Number.isSafeInteger(id) || id < 0) {
        loginError.textContent =
            "Escribe un ID entero no negativo válido.";
        return;
    }

    loginError.textContent = "";
    loginButton.textContent = "Consultando...";
    cambiarEstado(true);

    try {
        const datos = await consultarApi("/api/login", {
            method: "POST",
            body: JSON.stringify({ cliente_id: id })
        });

        mostrarSesion(datos);

    } catch (error) {
        loginError.textContent = error.message;

    } finally {
        loginButton.textContent = "Iniciar sesión";
        cambiarEstado(false);

        if (clienteId !== null) {
            pregunta.focus();
        } else {
            clientIdInput.focus();
        }
    }
});


// Cerrar sesión
logoutButton.addEventListener("click", async () => {
    if (ocupado) {
        return;
    }

    sessionError.textContent = "";
    cambiarEstado(true);

    try {
        await consultarApi("/api/logout", { method: "POST" });
        mostrarLogin();

    } catch (error) {
        sessionError.textContent = error.message;

    } finally {
        cambiarEstado(false);

        if (clienteId === null) {
            clientIdInput.focus();
        }
    }
});


// Procesar consulta
formulario.addEventListener("submit", async (event) => {
    event.preventDefault();

    if (ocupado || clienteId === null) {
        return;
    }

    const texto = pregunta.value.trim();

    if (!texto || texto.length > 2000) {
        errorEntrada.textContent =
            "Escribe una pregunta de entre 1 y 2000 caracteres.";
        return;
    }

    errorEntrada.textContent = "";
    const mensajeUsuario = agregarMensaje("user", texto);

    pregunta.value = "";
    estadoChat.textContent = "Consultando tu información...";
    cambiarEstado(true);

    try {
        const datos = await consultarApi("/api/chat", {
            method: "POST",
            body: JSON.stringify({ pregunta: texto })
        });

        mostrarResumen(datos.datos_cliente);
        agregarMensaje("assistant", datos.respuesta);

    } catch (error) {
        if (error.status === 401) {
            mostrarLogin(error.message);
        } else {
            mensajeUsuario.remove();
            pregunta.value = texto;
            errorEntrada.textContent = error.message;
        }

    } finally {
        estadoChat.textContent = "";
        cambiarEstado(false);

        if (clienteId !== null) {
            pregunta.focus();
        } else {
            clientIdInput.focus();
        }
    }
});


// Actualizar entrada
pregunta.addEventListener("input", () => {
    errorEntrada.textContent = "";
    actualizarControles();
});

clientIdInput.addEventListener("input", () => {
    loginError.textContent = "";
    actualizarControles();
});


// Enviar con Enter
pregunta.addEventListener("keydown", (event) => {
    if (
        event.key === "Enter" &&
        !event.shiftKey &&
        !event.isComposing
    ) {
        event.preventDefault();

        if (!botonEnviar.disabled) {
            formulario.requestSubmit();
        }
    }
});


// Seleccionar pregunta sugerida
sugerencias.forEach((boton) => {
    boton.addEventListener("click", () => {
        pregunta.value = boton.dataset.question;
        errorEntrada.textContent = "";
        actualizarControles();
        pregunta.focus();
    });
});


// Limpiar conversación
botonLimpiar.addEventListener("click", () => {
    iniciarConversacion();
    pregunta.focus();
});


// Recuperar sesión
async function cargarSesion() {
    cambiarEstado(true);

    try {
        const datos = await consultarApi("/api/me");
        mostrarSesion(datos);

    } catch (error) {
        mostrarLogin(
            error.status === 401 ? "" : error.message
        );

    } finally {
        cambiarEstado(false);
    }
}

cargarSesion();