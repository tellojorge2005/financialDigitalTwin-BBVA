export const buscar = (selector) => document.querySelector(selector);

// Crear contenido con texto, sin interpretar HTML recibido
export function elemento(etiqueta, clase = "", texto = "") {
  const nodo = document.createElement(etiqueta);
  nodo.className = clase;
  nodo.textContent = texto;
  return nodo;
}

export function texto(selector, valor) {
  buscar(selector).textContent = valor;
}

export function cerrarMenu() {
  buscar("[data-menu]").hidden = true;
  buscar("[data-profile]").setAttribute("aria-expanded", "false");
}

export function seleccionarTab(nombre, enfocar = false) {
  document.querySelectorAll("[data-tab]").forEach((boton) => {
    const activo = boton.dataset.tab === nombre;
    boton.setAttribute("aria-selected", String(activo));
    boton.tabIndex = activo ? 0 : -1;
    if (activo && enfocar) boton.focus();
  });
  document.querySelectorAll("[data-pane]").forEach((panel) => {
    panel.hidden = panel.dataset.pane !== nombre;
  });
}

export function prepararInterfaz() {
  const perfil = buscar("[data-profile]");
  if (perfil) {
    perfil.addEventListener("click", () => {
      const abierto = perfil.getAttribute("aria-expanded") === "true";
      buscar("[data-menu]").hidden = abierto;
      perfil.setAttribute("aria-expanded", String(!abierto));
    });
    document.addEventListener("click", (evento) => {
      if (!buscar("[data-profile-area]").contains(evento.target)) cerrarMenu();
    });
    document.addEventListener("keydown", (evento) => {
      if (evento.key === "Escape" && !buscar("[data-menu]").hidden) {
        cerrarMenu();
        perfil.focus();
      }
    });
    buscar("[data-profile-area]").addEventListener("focusout", (evento) => {
      if (!evento.currentTarget.contains(evento.relatedTarget)) cerrarMenu();
    });
  }
  const tabs = [...document.querySelectorAll("[data-tab]")];
  tabs.forEach((boton, indice) => {
    boton.addEventListener("click", () => seleccionarTab(boton.dataset.tab));
    boton.addEventListener("keydown", (evento) => {
      let destino;
      if (evento.key === "ArrowRight") destino = (indice + 1) % tabs.length;
      if (evento.key === "ArrowLeft")
        destino = (indice + tabs.length - 1) % tabs.length;
      if (evento.key === "Home") destino = 0;
      if (evento.key === "End") destino = tabs.length - 1;
      if (destino !== undefined) {
        evento.preventDefault();
        seleccionarTab(tabs[destino].dataset.tab, true);
      }
    });
  });
  const logo = buscar(".banklogo");
  const respaldo = () => {
    logo.hidden = true;
    buscar("[data-logo-fallback]").hidden = false;
  };
  logo.addEventListener("error", respaldo);
  if (logo.complete && !logo.naturalWidth) respaldo();
}
