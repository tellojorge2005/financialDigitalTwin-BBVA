// Markdown básico construido con nodos: nunca interpretar HTML del mensaje
const MATHML_NS = "http://www.w3.org/1998/Math/MathML";

const SIMBOLOS_MO = {
  times: "×",
  cdot: "·",
  div: "÷",
  pm: "±",
  mp: "∓",
  approx: "≈",
  neq: "≠",
  ne: "≠",
  leq: "≤",
  le: "≤",
  geq: "≥",
  ge: "≥",
  to: "→",
  rightarrow: "→",
  leftarrow: "←",
  Rightarrow: "⇒",
  Leftarrow: "⇐",
  leftrightarrow: "↔",
  sum: "∑",
  prod: "∏",
  int: "∫",
  ldots: "…",
  dots: "…",
  cdots: "⋯",
  "%": "%",
  $: "$",
  "{": "{",
  "}": "}",
  _: "_",
  "#": "#",
  "&": "&",
};

const SIMBOLOS_MI = {
  infty: "∞",
  alpha: "α",
  beta: "β",
  gamma: "γ",
  delta: "δ",
  Delta: "Δ",
  epsilon: "ε",
  varepsilon: "ε",
  theta: "θ",
  lambda: "λ",
  mu: "μ",
  pi: "π",
  sigma: "σ",
  Sigma: "Σ",
  tau: "τ",
  phi: "φ",
  omega: "ω",
  Omega: "Ω",
};

const ESPACIOS_LATEX = {
  ",": "0.17em",
  ":": "0.22em",
  ";": "0.28em",
  " ": "0.25em",
  quad: "1em",
  qquad: "2em",
};

const COMANDOS_TEXTO = new Set([
  "text",
  "textrm",
  "mathrm",
  "textbf",
  "mathbf",
  "textit",
  "operatorname",
]);

function nodo(etiqueta, contenido) {
  const elemento = document.createElement(etiqueta);
  if (contenido !== undefined) elemento.textContent = contenido;
  return elemento;
}

function nodoMath(etiqueta, contenido) {
  const elemento = document.createElementNS(MATHML_NS, etiqueta);
  if (contenido !== undefined) elemento.textContent = contenido;
  return elemento;
}

function leerGrupo(src, inicio) {
  let i = inicio;
  while (i < src.length && /\s/.test(src[i])) i++;
  if (i >= src.length) return { contenido: "", siguiente: i };
  if (src[i] !== "{") {
    if (src[i] === "\\") {
      const m = /^\\(?:[A-Za-z]+|.)/.exec(src.slice(i));
      const tok = m ? m[0] : src[i];
      return { contenido: tok, siguiente: i + tok.length };
    }
    return { contenido: src[i], siguiente: i + 1 };
  }
  let profundidad = 0;
  const comienzo = i + 1;
  for (; i < src.length; i++) {
    if (src[i] === "\\" && i + 1 < src.length) {
      i++;
      continue;
    }
    if (src[i] === "{") profundidad++;
    else if (src[i] === "}") {
      profundidad--;
      if (profundidad === 0) {
        return { contenido: src.slice(comienzo, i), siguiente: i + 1 };
      }
    }
  }
  return { contenido: src.slice(comienzo), siguiente: src.length };
}

function leerTextoPlano(src) {
  return src
    .replace(/\\([$%_{}#&])/g, "$1")
    .replace(/^ +| +$/g, (espacios) => "\u00A0".repeat(espacios.length));
}

function parsearExpresionLatex(src) {
  const mrow = nodoMath("mrow");
  let i = 0;

  function leerAtomo() {
    while (i < src.length && /\s/.test(src[i])) i++;
    if (i >= src.length) return null;
    const ch = src[i];

    if (ch === "{") {
      const grupo = leerGrupo(src, i);
      i = grupo.siguiente;
      return parsearExpresionLatex(grupo.contenido);
    }

    if (ch === "\\") {
      i++;
      if (i >= src.length) return null;
      let cmd = "";
      if (/[A-Za-z]/.test(src[i])) {
        while (i < src.length && /[A-Za-z]/.test(src[i])) cmd += src[i++];
      } else {
        cmd = src[i++];
      }

      if (
        cmd === "!" ||
        cmd === "displaystyle" ||
        cmd === "textstyle" ||
        cmd === "scriptstyle" ||
        cmd === "limits"
      ) {
        return null;
      }

      if (cmd === "left" || cmd === "right") {
        while (i < src.length && /\s/.test(src[i])) i++;
        if (i >= src.length) return null;
        if (src[i] === ".") {
          i++;
          return null;
        }
        if (src[i] === "\\" && i + 1 < src.length) {
          i++;
          return nodoMath("mo", src[i++]);
        }
        return nodoMath("mo", src[i++]);
      }

      if (cmd === "begin" || cmd === "end") {
        const env = leerGrupo(src, i);
        i = env.siguiente;
        return null;
      }

      if (COMANDOS_TEXTO.has(cmd)) {
        const grupo = leerGrupo(src, i);
        i = grupo.siguiente;
        const mtext = nodoMath("mtext", leerTextoPlano(grupo.contenido));
        if (cmd === "textbf" || cmd === "mathbf") {
          mtext.setAttribute("mathvariant", "bold");
        } else if (cmd === "textit") {
          mtext.setAttribute("mathvariant", "italic");
        }
        return mtext;
      }

      if (cmd === "frac" || cmd === "dfrac" || cmd === "tfrac") {
        const num = leerGrupo(src, i);
        i = num.siguiente;
        const den = leerGrupo(src, i);
        i = den.siguiente;
        const mfrac = nodoMath("mfrac");
        mfrac.append(
          parsearExpresionLatex(num.contenido),
          parsearExpresionLatex(den.contenido),
        );
        return mfrac;
      }

      if (cmd === "sqrt") {
        while (i < src.length && /\s/.test(src[i])) i++;
        let indice = null;
        if (src[i] === "[") {
          const cierre = src.indexOf("]", i + 1);
          if (cierre !== -1) {
            indice = src.slice(i + 1, cierre);
            i = cierre + 1;
          }
        }
        const rad = leerGrupo(src, i);
        i = rad.siguiente;
        if (indice !== null) {
          const mroot = nodoMath("mroot");
          mroot.append(
            parsearExpresionLatex(rad.contenido),
            parsearExpresionLatex(indice),
          );
          return mroot;
        }
        const msqrt = nodoMath("msqrt");
        msqrt.append(parsearExpresionLatex(rad.contenido));
        return msqrt;
      }

      if (Object.hasOwn(ESPACIOS_LATEX, cmd)) {
        const mspace = nodoMath("mspace");
        mspace.setAttribute("width", ESPACIOS_LATEX[cmd]);
        return mspace;
      }

      if (Object.hasOwn(SIMBOLOS_MO, cmd)) {
        return nodoMath("mo", SIMBOLOS_MO[cmd]);
      }

      if (Object.hasOwn(SIMBOLOS_MI, cmd)) {
        return nodoMath("mi", SIMBOLOS_MI[cmd]);
      }

      return nodoMath("mi", cmd);
    }

    const numero = /^\d+(?:,\d{3})*(?:\.\d+)?/.exec(src.slice(i));
    if (numero) {
      i += numero[0].length;
      return nodoMath("mn", numero[0]);
    }

    if (/[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]/.test(ch)) {
      i++;
      return nodoMath("mi", ch);
    }

    i++;
    return nodoMath("mo", ch);
  }

  while (i < src.length) {
    while (i < src.length && /\s/.test(src[i])) i++;
    if (i >= src.length) break;

    let base = leerAtomo();
    if (!base) continue;

    let sub = null;
    let sup = null;
    while (i < src.length) {
      let j = i;
      while (j < src.length && /\s/.test(src[j])) j++;
      if (src[j] === "_" && !sub) {
        const g = leerGrupo(src, j + 1);
        sub = parsearExpresionLatex(g.contenido);
        i = g.siguiente;
      } else if (src[j] === "^" && !sup) {
        const g = leerGrupo(src, j + 1);
        sup = parsearExpresionLatex(g.contenido);
        i = g.siguiente;
      } else {
        break;
      }
    }

    if (sub && sup) {
      const msubsup = nodoMath("msubsup");
      msubsup.append(base, sub, sup);
      base = msubsup;
    } else if (sub) {
      const msub = nodoMath("msub");
      msub.append(base, sub);
      base = msub;
    } else if (sup) {
      const msup = nodoMath("msup");
      msup.append(base, sup);
      base = msup;
    }

    mrow.append(base);
  }

  return mrow;
}

function dividirNivelSuperior(src, separador) {
  const partes = [];
  let profundidad = 0;
  let inicio = 0;
  for (let i = 0; i < src.length; i++) {
    if (src[i] === "{") profundidad++;
    else if (src[i] === "}") profundidad = Math.max(0, profundidad - 1);
    else if (
      profundidad === 0 &&
      src.startsWith(separador, i) &&
      (separador !== "&" || src[i - 1] !== "\\")
    ) {
      partes.push(src.slice(inicio, i));
      i += separador.length - 1;
      inicio = i + 1;
    } else if (src[i] === "\\" && separador !== "\\\\") {
      i++;
    }
  }
  partes.push(src.slice(inicio));
  return partes;
}

function renderizarLatex(expresion, enBloque = false) {
  const math = nodoMath("math");
  if (enBloque) math.setAttribute("display", "block");
  const limpia = expresion
    .trim()
    .replace(/^\\begin\{[^}]+\}/, "")
    .replace(/\\end\{[^}]+\}$/, "")
    .trim();
  const filas = dividirNivelSuperior(limpia, "\\\\").filter((f) => f.trim());
  if (filas.length > 1) {
    const tabla = nodoMath("mtable");
    for (const fila of filas) {
      const tr = nodoMath("mtr");
      for (const celda of dividirNivelSuperior(fila, "&")) {
        const td = nodoMath("mtd");
        td.append(parsearExpresionLatex(celda));
        tr.append(td);
      }
      tabla.append(tr);
    }
    math.append(tabla);
  } else {
    math.append(parsearExpresionLatex(limpia.replace(/(?<!\\)&/g, " ")));
  }
  return math;
}

function enLinea(destino, texto, nivel = 0) {
  if (nivel > 4) {
    destino.append(document.createTextNode(texto));
    return;
  }
  const patron =
    /(`[^`\n]+`|\\\([^\n]+?\\\)|\\\[[^\n]+?\\\]|\$\$[^\n]+?\$\$|\*\*[^\n]+?\*\*|__[^\n]+?__|\*[^*\n]+\*|_[^_\n]+_)/g;
  let posicion = 0;
  for (const coincidencia of texto.matchAll(patron)) {
    destino.append(
      document.createTextNode(texto.slice(posicion, coincidencia.index)),
    );
    const pieza = coincidencia[0];
    if (pieza.startsWith("\\(") && pieza.endsWith("\\)")) {
      destino.append(renderizarLatex(pieza.slice(2, -2), false));
    } else if (
      (pieza.startsWith("\\[") && pieza.endsWith("\\]")) ||
      (pieza.startsWith("$$") && pieza.endsWith("$$"))
    ) {
      destino.append(renderizarLatex(pieza.slice(2, -2), false));
    } else {
      const codigo = pieza.startsWith("`");
      const fuerte = pieza.startsWith("**") || pieza.startsWith("__");
      const marca = fuerte ? 2 : 1;
      const elemento = nodo(codigo ? "code" : fuerte ? "strong" : "em");
      if (codigo) elemento.textContent = pieza.slice(1, -1);
      else enLinea(elemento, pieza.slice(marca, -marca), nivel + 1);
      destino.append(elemento);
    }
    posicion = coincidencia.index + pieza.length;
  }
  destino.append(document.createTextNode(texto.slice(posicion)));
}

const celdas = (linea) =>
  linea
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((c) => c.trim());
const separadorTabla = (linea) =>
  linea.includes("|") && celdas(linea).every((c) => /^:?-{3,}:?$/.test(c));
const lista = (linea) => /^\s*(?:[-+*]|\d+[.)])\s+/.test(linea);
const inicioBloqueMath = (linea) => /^\s*(?:\\\[|\$\$)/.test(linea);
const bloque = (linea) =>
  /^(?:#{1,6}\s|>\s?|```|~~~)/.test(linea) ||
  inicioBloqueMath(linea) ||
  lista(linea) ||
  /^\s*(?:---+|\*\*\*+)\s*$/.test(linea);

export function renderizarMarkdown(contenido) {
  const fragmento = document.createDocumentFragment();
  const lineas = String(contenido).replace(/\r\n?/g, "\n").split("\n");
  let i = 0;
  while (i < lineas.length) {
    const linea = lineas[i];
    if (!linea.trim()) {
      i++;
      continue;
    }
    if (/^(```|~~~)/.test(linea)) {
      const marca = linea.slice(0, 3);
      const codigo = [];
      i++;
      while (i < lineas.length && !lineas[i].startsWith(marca))
        codigo.push(lineas[i++]);
      if (i < lineas.length) i++;
      const pre = nodo("pre");
      pre.append(nodo("code", codigo.join("\n")));
      fragmento.append(pre);
      continue;
    }
    const mathBloque = /^\s*(\\\[|\$\$)(.*)$/.exec(linea);
    if (mathBloque) {
      const cierre = mathBloque[1] === "\\[" ? "\\]" : "$$";
      const resto = mathBloque[2];
      const partes = [];
      const posCierre = resto.indexOf(cierre);
      if (posCierre !== -1) {
        partes.push(resto.slice(0, posCierre));
        i++;
      } else {
        if (resto.trim()) partes.push(resto);
        i++;
        while (i < lineas.length && !lineas[i].includes(cierre)) {
          partes.push(lineas[i++]);
        }
        if (i < lineas.length) {
          const antes = lineas[i].slice(0, lineas[i].indexOf(cierre));
          if (antes.trim()) partes.push(antes);
          i++;
        }
      }
      const parrafoMath = nodo("p");
      parrafoMath.append(renderizarLatex(partes.join("\n"), true));
      fragmento.append(parrafoMath);
      continue;
    }
    const titulo = /^(#{1,6})\s+(.+)$/.exec(linea);
    if (titulo) {
      const elemento = nodo(titulo[1].length <= 2 ? "h3" : "h4");
      enLinea(elemento, titulo[2]);
      fragmento.append(elemento);
      i++;
      continue;
    }
    if (
      i + 1 < lineas.length &&
      linea.includes("|") &&
      separadorTabla(lineas[i + 1])
    ) {
      const envoltura = nodo("div");
      envoltura.className = "md-table";
      const tabla = nodo("table");
      const cabecera = nodo("thead");
      const fila = nodo("tr");
      for (const valor of celdas(linea)) {
        const celda = nodo("th");
        celda.scope = "col";
        enLinea(celda, valor);
        fila.append(celda);
      }
      cabecera.append(fila);
      tabla.append(cabecera);
      const cuerpo = nodo("tbody");
      i += 2;
      while (i < lineas.length && lineas[i].trim() && lineas[i].includes("|")) {
        const fila = nodo("tr");
        for (const valor of celdas(lineas[i++])) {
          const celda = nodo("td");
          enLinea(celda, valor);
          fila.append(celda);
        }
        cuerpo.append(fila);
      }
      tabla.append(cuerpo);
      envoltura.append(tabla);
      fragmento.append(envoltura);
      continue;
    }
    if (/^\s*(?:---+|\*\*\*+)\s*$/.test(linea)) {
      fragmento.append(nodo("hr"));
      i++;
      continue;
    }
    if (lista(linea)) {
      const ordenada = /^\s*\d+[.)]\s/.test(linea);
      const elemento = nodo(ordenada ? "ol" : "ul");
      if (ordenada) elemento.start = Number(linea.trim().match(/^\d+/)[0]);
      while (
        i < lineas.length &&
        lista(lineas[i]) &&
        /^\s*\d+[.)]\s/.test(lineas[i]) === ordenada
      ) {
        const item = nodo("li");
        enLinea(item, lineas[i++].replace(/^\s*(?:[-+*]|\d+[.)])\s+/, ""));
        elemento.append(item);
      }
      fragmento.append(elemento);
      continue;
    }
    if (/^>/.test(linea)) {
      const cita = nodo("blockquote");
      while (i < lineas.length && /^>/.test(lineas[i])) {
        if (cita.childNodes.length) cita.append(nodo("br"));
        enLinea(cita, lineas[i++].replace(/^>\s?/, ""));
      }
      fragmento.append(cita);
      continue;
    }
    const parrafo = nodo("p");
    do {
      if (parrafo.childNodes.length) parrafo.append(nodo("br"));
      enLinea(parrafo, lineas[i++]);
    } while (
      i < lineas.length &&
      lineas[i].trim() &&
      !bloque(lineas[i]) &&
      !(i + 1 < lineas.length && separadorTabla(lineas[i + 1]))
    );
    fragmento.append(parrafo);
  }
  return fragmento;
}
