import { api } from "./api.js";
import { buscar, texto, cerrarMenu, seleccionarTab, prepararInterfaz } from "./ui.js";
import { mostrarPanorama } from "./panorama.js";
import { agregarMensaje, iniciarConversacion, prepararChat } from "./chat.js";

// Mantener la sesión en el servidor; no guardar perfiles ni chats en el navegador
let sesion = null;
let ocupado = false;
let panorama = {};

function actualizarControles() {
    const bloqueado = ocupado || sesion === null;
    ["[data-logout]", "[data-clear]", "[data-input]", "[data-health]"].forEach(selector => {
        buscar(selector).disabled = bloqueado;
    });
    document.querySelectorAll("[data-question]").forEach(boton => { boton.disabled = bloqueado; });
    buscar("[data-send]").disabled = bloqueado || !buscar("[data-input]").value.trim();
    buscar("[data-form]").setAttribute("aria-busy", String(ocupado));
    texto("[data-count]", `${buscar("[data-input]").value.length} / 2000`);
}

function cambiarEstado(valor) {
    ocupado = valor;
    actualizarControles();
}

function actualizarPanorama(datos) {
    // Una respuesta de chat sin series no borra el panorama ya recibido
    for (const campo of ["monthly_history", "cashflow_scenarios"]) {
        if (Object.hasOwn(datos, campo)) panorama[campo] = datos[campo];
    }
    mostrarPanorama(panorama);
}

function mostrarSesion(datos) {
    if (!Number.isSafeInteger(datos?.cliente_id) || datos.cliente_id < 0 || !datos.datos_cliente) {
        throw new Error("No se pudo recuperar tu perfil. Intenta iniciar sesión nuevamente.");
    }
    buscar("[data-loading]").hidden = true;
    sesion = datos;
    panorama = {};
    const perfil = datos.datos_cliente;
    const nombre = typeof perfil.full_name === "string" ? perfil.full_name.trim() : "";
    const correo = typeof perfil.email === "string" ? perfil.email : "";
    const partes = nombre.split(/\s+/).filter(Boolean);
    texto("[data-fullname]", nombre || "Mi perfil");
    texto("[data-email]", correo);
    texto("[data-initials]", partes.length ? partes.slice(0, 2).map(p => Array.from(p)[0]).join("").toUpperCase() : "●");
    texto("[data-greeting]", partes.length ? `Hola, ${partes[0]}.` : "Hola.");
    buscar("[data-app]").hidden = false;
    buscar("[data-profile-area]").hidden = false;
    texto("[data-session-error]", "");
    cerrarMenu();
    seleccionarTab("resumen");
    actualizarPanorama(datos);
    iniciarConversacion(perfil, datos.historial);
}

// Vaciar la información antes de volver al acceso
function mostrarLogin() {
    sesion = null;
    panorama = {};
    cerrarMenu();
    buscar("[data-app]").hidden = true;
    buscar("[data-profile-area]").hidden = true;
    buscar("[data-messages]").replaceChildren();
    buscar("[data-input]").value = "";
    for (const selector of ["[data-fullname]", "[data-email]", "[data-initials]"]) texto(selector, "");
    mostrarPanorama();
    window.location.replace("login.html");
}

// Recuperar la respuesta sin borrar la conversación ni reenviar el mensaje
async function recuperarChat(pregunta = "", requestId = null) {
    const limite = Date.now() + 80000;
    while (sesion && Date.now() < limite) {
        texto("[data-chat-status]", "Comprobando la respuesta anterior…");
        const estado = await api.estadoChat();
        if (estado.cliente_id !== sesion.cliente_id) {
            mostrarLogin();
            return;
        }
        if (!estado.en_curso) {
            iniciarConversacion(sesion.datos_cliente, estado.historial);
            if (pregunta && estado.ultimo_turno !== requestId) {
                buscar("[data-input]").value = pregunta;
                texto("[data-chat-error]", "No se guardó una respuesta para este mensaje. Lo dejamos escrito para que puedas volver a enviarlo; tu conversación se conserva.");
            }
            return;
        }
        texto("[data-chat-status]", "Prisma sigue trabajando. Recuperaremos la respuesta automáticamente…");
        await new Promise(resolve => window.setTimeout(resolve, 2000));
    }
    throw new Error("No se pudo confirmar el estado de la respuesta. Recarga la página para recuperar el historial; no necesitas limpiar el chat.");
}

async function enviar() {
    if (ocupado || !sesion) return;
    const pregunta = buscar("[data-input]").value.trim();
    if (!pregunta || pregunta.length > 2000) return;
    const requestId = Array.from(crypto.getRandomValues(new Uint8Array(16)), n => n.toString(16).padStart(2, "0")).join("");
    texto("[data-chat-error]", "");
    const mensaje = agregarMensaje("user", pregunta);
    buscar("[data-input]").value = "";
    texto("[data-chat-status]", "Consultando tu información…");
    cambiarEstado(true);
    try {
        const datos = await api.preguntar(pregunta, requestId);
        if (datos.cliente_id !== sesion.cliente_id) {
            mostrarLogin();
            return;
        }
        if (typeof datos.respuesta !== "string" || !datos.respuesta.trim()) {
            const error = new Error("No se recibió una respuesta válida.");
            error.incierto = true;
            throw error;
        }
        agregarMensaje("assistant", datos.respuesta);
        actualizarPanorama(datos);
    } catch (error) {
        if (error.status === 401) mostrarLogin();
        else if (error.incierto || error.status === 409 || error.status >= 500) {
            try {
                await recuperarChat(pregunta, requestId);
            } catch (recuperacion) {
                if (recuperacion.status === 401) mostrarLogin();
                else {
                    mensaje.remove();
                    buscar("[data-input]").value = pregunta;
                    texto("[data-chat-error]", "No pudimos recuperar el estado. Recarga para consultar el historial antes de reenviar; no necesitas borrarlo.");
                }
            }
        } else {
            mensaje.remove();
            buscar("[data-input]").value = pregunta;
            texto("[data-chat-error]", error.message);
        }
    } finally {
        texto("[data-chat-status]", "");
        cambiarEstado(false);
        if (sesion) buscar("[data-input]").focus();
    }
}

async function limpiar() {
    if (ocupado || !sesion) return;
    if (!window.confirm("¿Borrar la conversación y la meta que contiene?")) return;
    cambiarEstado(true);
    texto("[data-chat-error]", "");
    try {
        await api.limpiar();
        iniciarConversacion(sesion.datos_cliente);
    } catch (error) {
        if (error.status === 401) mostrarLogin(error.message);
        else texto("[data-chat-error]", error.message);
    } finally {
        cambiarEstado(false);
    }
}

prepararInterfaz();
prepararChat(enviar, limpiar, actualizarControles, () => !ocupado && sesion !== null);

buscar("[data-logout]").addEventListener("click", async () => {
    if (ocupado || !sesion) return;
    cambiarEstado(true);
    texto("[data-session-error]", "");
    try {
        await api.salir();
        mostrarLogin();
    } catch (error) {
        if (error.status === 401) mostrarLogin();
        else texto("[data-session-error]", "No se pudo cerrar la sesión. Intenta nuevamente.");
    } finally {
        cambiarEstado(false);
    }
});

// Recuperar la conversación de la cookie existente sin simular un acceso
async function recuperarSesion() {
    cambiarEstado(true);
    try {
        mostrarSesion(await api.sesion());
        await recuperarChat();
    } catch (error) {
        if (error.status === 401) mostrarLogin();
        else texto(sesion ? "[data-chat-error]" : "[data-loading]", `${error.message} Recarga la página para intentarlo nuevamente.`);
    } finally {
        cambiarEstado(false);
    }
}

recuperarSesion();

// Al volver con el navegador, comprobar otra vez la cookie antes de mostrar datos
window.addEventListener("pagehide", () => {
    buscar("[data-app]").hidden = true;
    buscar("[data-profile-area]").hidden = true;
});
window.addEventListener("pageshow", evento => {
    if (evento.persisted) window.location.reload();
});
