/* =========================================================================
   ETL front-end controller
   ========================================================================= */

/* -------- elementos DOM -------- */
const btn   = document.getElementById("run-etl");
const ov    = document.getElementById("etl-overlay");
const bar   = document.getElementById("etl-bar");
const logEl = document.getElementById("etl-log");
const stream = document.getElementById("etl-stream");
/* -------- socket -------- */
const sock  = io();

/* -------- flag para bloquear navegación -------- */
let blockLeave = false;

/*   🔒  Pregunta al usuario si intenta salir mientras blockLeave = true   */
window.addEventListener("beforeunload", (e) => {
  if (blockLeave) {
    e.preventDefault();   // necesario en algunos navegadores
    e.returnValue = "";   // activa el diálogo genérico
  }
});

/*   🔒  Intercepta enlaces internos mientras corre el ETL                 */
document.addEventListener(
  "click",
  (ev) => {
    if (!blockLeave) return;
    const a = ev.target.closest("a");
    if (a) {
      ev.preventDefault();
      alert("Espera a que termine el ETL antes de navegar o recargar.");
    }
  },
  true /* fase de captura */
);

/* -------- util: añadir línea al log -------- */
/*
function addLog(text) {
  logEl.textContent += text + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}
*/
function addLog(text){
  logEl.textContent   += text + "\n";   // overlay
  stream.textContent  += text + "\n";   // consola fija
  logEl.scrollTop  = logEl.scrollHeight;
  stream.scrollTop = stream.scrollHeight;
}
/* -------- click "Iniciar ETL" -------- */
btn.addEventListener("click", async () => {
  try {
    btn.disabled       = true;        // evita múltiples clics
    blockLeave         = true;        // 🔒 activa bloqueo
    logEl.textContent  = "";
    bar.style.width    = "0%";
    ov.classList.remove("hidden");

    /* crea la tarea en backend */
    const resp = await fetch("/start_etl", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    if (!resp.ok) throw new Error(await resp.text());
    const { job_id } = await resp.json();

    /* únete a la sala */
    setTimeout(() => sock.emit("join", job_id), 20);
  } catch (e) {
    alert("Error al crear la tarea ETL: " + e);
    btn.disabled = false;
    blockLeave   = false;
  }
});

/* -------- recibe progreso -------- */
sock.on("progress", ({ msg, pct }) => {
  addLog(msg);
  if (pct >= 0) bar.style.width = pct + "%";

  /* FIN OK */
  if (pct === 100) {
    blockLeave = false;          // 🔓 permite salir
    addLog("✅ ETL terminado.");
    setTimeout(() => {
      ov.classList.add("hidden");
      btn.disabled = false;
    }, 800);
  }

  /* ERROR */
  if (pct === -1) {
    blockLeave = false;          // 🔓 permite salir
    bar.style.background = "#e74c3c";
    addLog("❌ Proceso abortado.");
    setTimeout(() => {
      btn.disabled = false;
    }, 800);
  }
});
sock.on("detail", ({line}) => addLog(line));
/* -------- log conexión WS (opcional) -------- */
sock.on("connect", () => console.log("WS conectado:", sock.id));
