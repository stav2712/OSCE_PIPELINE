/*  Gestión de colapso sidebar + burger  */
const sidebar   = document.getElementById("sidebar");
const burgerBtn = document.getElementById("burger");
if (burgerBtn){
  burgerBtn.addEventListener("click",()=>{
    document.body.classList.toggle("sidebar-open");
  });
}

const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)");
if (prefersReduced.matches){
  document.documentElement.style.setProperty("--trans","0ms");
}

/*  Autocerrar overlay al click fuera de la sidebar  */
document.addEventListener("click",ev=>{
  if(!document.body.classList.contains("sidebar-open")) return;
  const path = ev.composedPath();
  if(!path.includes(sidebar) && !path.includes(burgerBtn)){
    document.body.classList.remove("sidebar-open");
  }
});
