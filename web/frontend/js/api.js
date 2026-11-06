// Conservar las rutas y las cookies de sesión del proyecto
const rutas = {
    login: "/api/login",
    sesion: "/api/me",
    logout: "/api/logout",
    chat: "/api/chat",
    limpiar: "/api/chat/clear",
    estadoChat: "/api/chat/status"
};

async function consultar(ruta, { method = "GET", body, timeoutMs = 80000 } = {}) {
    const controlador = new AbortController();
    const timeout = window.setTimeout(() => controlador.abort(), timeoutMs);
    try {
        const respuesta = await fetch(ruta, {
            method,
            credentials: "same-origin",
            cache: "no-store",
            signal: controlador.signal,
            headers: {
                Accept: "application/json",
                ...(body ? { "Content-Type": "application/json" } : {})
            },
            ...(body ? { body: JSON.stringify(body) } : {})
        });
        const datos = respuesta.status === 204
            ? null : await respuesta.json().catch(() => null);
        if (!respuesta.ok) {
            // Mostrar mensajes públicos sin volcar detalles internos del servidor
            const mensajes = {
                401: "Tu sesión terminó. Inicia sesión nuevamente.",
                404: "No se encontró la información solicitada.",
                409: "Espera a que termine la respuesta anterior.",
                422: "No pudimos iniciar la solicitud con esos datos.",
                429: "El servicio está ocupado. Intenta más tarde.",
                503: "Tu información no está disponible temporalmente.",
                504: "La respuesta no se completó a tiempo. Tu conversación se conserva."
            };
            const error = new Error(mensajes[respuesta.status] || "No se pudo completar la solicitud. Intenta más tarde.");
            error.status = respuesta.status;
            throw error;
        }
        if (respuesta.status !== 204 && (!datos || typeof datos !== "object")) {
            throw new Error("No se recibió una respuesta válida. Intenta más tarde.");
        }
        return datos;
    } catch (error) {
        if (error.name === "AbortError") {
            const fallo = new Error("La solicitud tardó demasiado. Vamos a comprobar el estado de tu conversación.");
            fallo.incierto = true;
            throw fallo;
        }
        if (error instanceof TypeError) {
            const fallo = new Error("No se pudo confirmar la respuesta por un problema de conexión.");
            fallo.incierto = true;
            throw fallo;
        }
        throw error;
    } finally {
        window.clearTimeout(timeout);
    }
}

// Mantener el contrato del backend y recuperar el chat sin repetir la pregunta
export const api = {
    entrar: email => consultar(rutas.login, { method: "POST", body: { email } }),
    sesion: () => consultar(rutas.sesion),
    salir: () => consultar(rutas.logout, { method: "POST" }),
    preguntar: (pregunta, request_id) => consultar(rutas.chat, { method: "POST", body: { pregunta, request_id } }),
    estadoChat: () => consultar(rutas.estadoChat, { timeoutMs: 10000 }),
    limpiar: () => consultar(rutas.limpiar, { method: "POST" })
};
