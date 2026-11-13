import { api } from "./api.js";
import { buscar, texto, prepararInterfaz } from "./ui.js";

let ocupado = false;

function actualizarControles() {
  buscar("[data-logininput]").disabled = ocupado;
  buscar("[data-login-button]").disabled =
    ocupado || !buscar("[data-logininput]").value.trim();
  buscar("[data-loginform]").setAttribute("aria-busy", String(ocupado));
}

function abrirDashboard(datos) {
  if (
    !Number.isSafeInteger(datos?.cliente_id) ||
    datos.cliente_id < 0 ||
    !datos.datos_cliente
  ) {
    throw new Error(
      "No se pudo recuperar tu perfil. Intenta iniciar sesión nuevamente.",
    );
  }
  window.location.replace("dashboard.html");
}

prepararInterfaz();
buscar("[data-logininput]").addEventListener("input", () => {
  texto("[data-login-error]", "");
  actualizarControles();
});

buscar("[data-loginform]").addEventListener("submit", async (evento) => {
  evento.preventDefault();
  if (ocupado || !evento.currentTarget.reportValidity()) return;
  const email = buscar("[data-logininput]").value.trim().toLowerCase();
  ocupado = true;
  actualizarControles();
  texto("[data-login-error]", "");
  texto("[data-login-button]", "Iniciando sesión…");
  try {
    abrirDashboard(await api.entrar(email));
  } catch (error) {
    texto(
      "[data-login-error]",
      error.status === 422
        ? "No pudimos iniciar sesión con ese correo. Revisa la dirección o intenta más tarde."
        : error.message,
    );
  } finally {
    ocupado = false;
    texto("[data-login-button]", "Iniciar sesión");
    actualizarControles();
  }
});

// Reutilizar la cookie existente sin trasladar datos privados entre páginas
async function recuperarSesion() {
  ocupado = true;
  actualizarControles();
  try {
    abrirDashboard(await api.sesion());
  } catch (error) {
    if (error.status !== 401) texto("[data-login-error]", error.message);
  } finally {
    ocupado = false;
    actualizarControles();
  }
}

recuperarSesion();
window.addEventListener("pageshow", (evento) => {
  if (evento.persisted) window.location.reload();
});
