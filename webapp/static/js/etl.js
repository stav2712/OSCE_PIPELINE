/* =========================================================================
   ETL front-end controller
   ========================================================================= */

/* -------- elementos DOM -------- */
const btn     = document.getElementById("run-etl");
const inpWin  = document.getElementById("win-days");
const ov      = document.getElementById("etl-overlay");
const bar     = document.getElementById("etl-bar");
const logEl   = document.getElementById("etl-log");
const stream  = document.getElementById("etl-stream");

/* -------- socket -------- */
const sock    = io();

/* -------- flag de bloqueo de navegación -------- */
let blockLeave = false;

/* ——— evita salir mientras corre el ETL ——— */
window.addEventListener("beforeunload", (e) => {
  if (blockLeave) {
    e.preventDefault();
    e.returnValue = "";
  }
});
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
  true
);

/* -------- util: añadir línea al log -------- */
function addLog(text) {
  logEl.value    += text + "\n";
  stream.value   += text + "\n";
  logEl.scrollTop  = logEl.scrollHeight;
  stream.scrollTop = stream.scrollHeight;
}

/* -------- click «Iniciar ETL» -------- */
btn.addEventListener("click", async () => {
  // Pregunta de confirmación
  const ok = window.confirm(
    "⚠️ El proceso ETL tarda aproximadamente 1h en la primera ejecución.\n" +
    "¿Estás seguro de que quieres iniciar ahora?"
  );
  if (!ok) return;  // si le da a “Cancelar”, salimos sin hacer nada

  try {
    /* prepara UI */
    btn.disabled      = true;
    blockLeave        = true;
    logEl.textContent = "";
    bar.style.width   = "0%";
    ov.classList.remove("hidden");

    /* payload opcional */
    const win = parseInt(inpWin.value, 10);
    const body = Number.isFinite(win) ? { window_days: win } : {};

    /* crea la tarea en backend */
    const resp = await fetch("/start_etl", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    if (!resp.ok) throw new Error(await resp.text());

    const { job_id, window_days } = await resp.json();
    addLog(`🚀  ETL lanzado (ventana = ${window_days} días)`);

    /* únete a la sala de WebSocket */
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
    blockLeave = false;
    addLog("✅  ETL terminado.");
    setTimeout(() => {
      ov.classList.add("hidden");
      btn.disabled = false;
    }, 800);
  }

  /* ERROR */
  if (pct === -1) {
    blockLeave = false;
    bar.style.background = "#e74c3c";
    addLog("❌  Proceso abortado.");
    setTimeout(() => (btn.disabled = false), 800);
  }
});

/* -------- detalle de log línea-a-línea -------- */
sock.on("detail", ({ line }) => addLog(line));

/* -------- depuración conexión WS -------- */
sock.on("connect", () => console.log("WS conectado:", sock.id));
