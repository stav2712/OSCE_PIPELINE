/* ---------- Utils ---------- */
const qs = sel => document.querySelector(sel);
const chatStream = qs("#chat-stream");
const loaderDiv  = qs("#loader");
const readyDiv   = qs("#ready");
const form       = qs("#chat-form");
const textarea   = form.querySelector("textarea");

/* reiniciar conversación al pulsar + Nuevo reporte */
document.querySelector(".new-report")?.addEventListener("click", ev => {
  if (location.pathname === "/") {
    ev.preventDefault();
    chatStream.innerHTML = "";
    textarea.focus();
  }
});

/* ---------- Warm-up ---------- */
(async () => {
  while (true) {
    const r = await fetch("/ready");
    if (r.ok) break;                      // 200 → listo

    if (r.status === 425) {               // 425 → faltan datos
      loaderDiv.textContent =
        "🚧 Aún no hay datos. Ve a la pestaña “Proceso ETL” y ejecútalo.";
      // opcional: mostrar un botón que lleve a /etl
      return;                             // ⛔ deja de consultar
    }

    // 503 → sigue intentando cada 700 ms
    await new Promise(res => setTimeout(res, 700));
  }

  /* motor listo ⇒ revelamos la UI */
  loaderDiv.classList.add("hidden");
  readyDiv.classList.remove("hidden");
  form.classList.remove("hidden");
  textarea.focus();
})();

/* ---------- Render helpers ---------- */
function addUserBubble(text) {
  const div = document.createElement("div");
  div.className = "bubble user";
  div.textContent = text;
  chatStream.appendChild(div);
  chatStream.scrollTop = chatStream.scrollHeight;
}

function addBotSkeleton() {
  const bubble = document.createElement("div");
  bubble.className = "bubble bot";
  const tag = document.createElement("div");
  tag.className = "bot-tag";
  tag.textContent = "R";
  const card = document.createElement("div");
  card.className = "bot-card";
  card.innerHTML = `<i class="fa fa-spinner fa-spin"></i> Generando…`;
  bubble.append(tag, card);
  chatStream.appendChild(bubble);
  chatStream.scrollTop = chatStream.scrollHeight;
  return card;
}

/* ---------- Envío ---------- */
form.addEventListener("submit", async ev => {
  ev.preventDefault();
  const q = textarea.value.trim();
  if (!q) return;
  addUserBubble(q);
  textarea.value = "";

  const card = addBotSkeleton();
  try {
    const r = await fetch("/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: q })
    });
    const data = await r.json();

    // Convertir Markdown a HTML para el resumen
    const resumenHtml = marked.parse(data.resumen);

    card.innerHTML = `
      <div>
        <span class="summary-title">SQL:</span>
        <pre>${data.sql}</pre>
      </div>

      <div>
        <span class="summary-title">Resumen:</span>
        <div class="markdown">${resumenHtml}</div>
      </div>

      <div>
        <span class="summary-title">Tabla:</span>
        <div class="table-wrapper">${data.table}</div>
        <a href="${data.excel}" class="dl-link" target="_blank">
          <i class="fa fa-download"></i> Descargar Excel
        </a>
      </div>
    `;
  } catch (e) {
    card.innerHTML = `<span style="color:red">Error: ${e}</span>`;
  }
});

/* Enviar con Enter sin romper saltos Shift+Enter */
textarea.addEventListener("keydown", ev => {
  if (ev.key === "Enter" && !ev.shiftKey) {
    ev.preventDefault();
    form.requestSubmit();
  }
});
