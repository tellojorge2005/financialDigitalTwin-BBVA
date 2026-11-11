import { renderizarMarkdown } from "./markdown.js";
import { buscar, elemento, texto } from "./ui.js";

// Aplicar formato solo al asistente, sin interpretar HTML recibido
export function agregarMensaje(autor, contenido) {
  const mensaje = elemento(
    "article",
    `message ${autor === "user" ? "user" : "assistant"}`,
  );
  const burbuja = elemento("div", "bubble");
  if (autor === "assistant") {
    burbuja.classList.add("markdown");
    burbuja.append(renderizarMarkdown(contenido));
  } else burbuja.textContent = contenido;
  mensaje.append(
    elemento("div", "author", autor === "user" ? "Tú" : "Prisma"),
    burbuja,
  );
  const mensajes = buscar("[data-messages]");
  mensajes.append(mensaje);
  mensajes.scrollTop = mensajes.scrollHeight;
  return mensaje;
}

export function iniciarConversacion(perfil, historial = []) {
  buscar("[data-messages]").replaceChildren();
  buscar("[data-input]").value = "";
  texto("[data-chat-error]", "");
  texto("[data-chat-status]", "");
  const validos = Array.isArray(historial)
    ? historial.filter(
        (item) =>
          item &&
          ["user", "assistant"].includes(item.autor) &&
          typeof item.texto === "string",
      )
    : [];
  if (validos.length) {
    validos.forEach((item) => agregarMensaje(item.autor, item.texto));
  } else {
    const nombre =
      typeof perfil.full_name === "string"
        ? perfil.full_name.trim().split(/\s+/)[0]
        : "";
    agregarMensaje(
      "assistant",
      `${nombre ? `¡Hola, ${nombre}!` : "¡Hola!"} 💙\n\n` +
        "Soy Prisma, tu asistente financiero. Puedo ayudarte a comprender tus ingresos y gastos, analizar tu salud financiera y explorar cómo podrían comportarse tus finanzas.\n\n¿Qué te gustaría saber?",
    );
  }
}

export function prepararChat(enviar, limpiar, actualizar, puedeEscribir) {
  const input = buscar("[data-input]");
  buscar("[data-form]").addEventListener("submit", (evento) => {
    evento.preventDefault();
    enviar();
  });
  input.addEventListener("input", () => {
    texto("[data-chat-error]", "");
    actualizar();
  });
  input.addEventListener("keydown", (evento) => {
    if (evento.key === "Enter" && !evento.shiftKey && !evento.isComposing) {
      evento.preventDefault();
      if (!buscar("[data-send]").disabled)
        buscar("[data-form]").requestSubmit();
    }
  });
  const sugerir = (pregunta) => {
    if (!puedeEscribir()) return;
    input.value = pregunta;
    texto("[data-chat-error]", "");
    actualizar();
    input.focus();
  };
  document.querySelectorAll("[data-question]").forEach((boton) => {
    boton.addEventListener("click", () => sugerir(boton.dataset.question));
  });
  buscar("[data-health]").addEventListener("click", () =>
    sugerir("¿Cómo está mi salud financiera?"),
  );
  buscar("[data-clear]").addEventListener("click", limpiar);
}
