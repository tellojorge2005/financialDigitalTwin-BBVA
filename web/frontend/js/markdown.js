// Markdown básico construido con nodos: nunca interpretar HTML del mensaje
function nodo(etiqueta, contenido) {
    const elemento = document.createElement(etiqueta);
    if (contenido !== undefined) elemento.textContent = contenido;
    return elemento;
}

function enLinea(destino, texto, nivel = 0) {
    if (nivel > 4) {
        destino.append(document.createTextNode(texto));
        return;
    }
    const patron = /(`[^`\n]+`|\*\*[^\n]+?\*\*|__[^\n]+?__|\*[^*\n]+\*|_[^_\n]+_)/g;
    let posicion = 0;
    for (const coincidencia of texto.matchAll(patron)) {
        destino.append(document.createTextNode(texto.slice(posicion, coincidencia.index)));
        const pieza = coincidencia[0];
        const codigo = pieza.startsWith("`");
        const fuerte = pieza.startsWith("**") || pieza.startsWith("__");
        const marca = fuerte ? 2 : 1;
        const elemento = nodo(codigo ? "code" : fuerte ? "strong" : "em");
        if (codigo) elemento.textContent = pieza.slice(1, -1);
        else enLinea(elemento, pieza.slice(marca, -marca), nivel + 1);
        destino.append(elemento);
        posicion = coincidencia.index + pieza.length;
    }
    destino.append(document.createTextNode(texto.slice(posicion)));
}

const celdas = linea => linea.trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map(c => c.trim());
const separadorTabla = linea => linea.includes("|") && celdas(linea).every(c => /^:?-{3,}:?$/.test(c));
const lista = linea => /^\s*(?:[-+*]|\d+[.)])\s+/.test(linea);
const bloque = linea => /^(?:#{1,6}\s|>\s?|```|~~~)/.test(linea) || lista(linea) || /^\s*(?:---+|\*\*\*+)\s*$/.test(linea);

export function renderizarMarkdown(contenido) {
    const fragmento = document.createDocumentFragment();
    const lineas = String(contenido).replace(/\r\n?/g, "\n").split("\n");
    let i = 0;
    while (i < lineas.length) {
        const linea = lineas[i];
        if (!linea.trim()) { i++; continue; }
        if (/^(```|~~~)/.test(linea)) {
            const marca = linea.slice(0, 3);
            const codigo = [];
            i++;
            while (i < lineas.length && !lineas[i].startsWith(marca)) codigo.push(lineas[i++]);
            if (i < lineas.length) i++;
            const pre = nodo("pre");
            pre.append(nodo("code", codigo.join("\n")));
            fragmento.append(pre);
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
        if (i + 1 < lineas.length && linea.includes("|") && separadorTabla(lineas[i + 1])) {
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
            fragmento.append(nodo("hr")); i++; continue;
        }
        if (lista(linea)) {
            const ordenada = /^\s*\d+[.)]\s/.test(linea);
            const elemento = nodo(ordenada ? "ol" : "ul");
            if (ordenada) elemento.start = Number(linea.trim().match(/^\d+/)[0]);
            while (i < lineas.length && lista(lineas[i]) && /^\s*\d+[.)]\s/.test(lineas[i]) === ordenada) {
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
        } while (i < lineas.length && lineas[i].trim() && !bloque(lineas[i])
            && !(i + 1 < lineas.length && separadorTabla(lineas[i + 1])));
        fragmento.append(parrafo);
    }
    return fragmento;
}
