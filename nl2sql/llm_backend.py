from __future__ import annotations

import json
import os
from typing import Dict, List
import json, os, platform, httpx

def _force_ascii_headers():
    """
    Sustituye BaseClient._build_headers por una versión propia:
    * Solo cabeceras ASCII                                    ✅
    * Sigue añadiendo Authorization, Content-Type y User-Agent ✅
    """
    try:
        from openai._base_client import BaseClient
    except ImportError:
        return  # el SDK no es el esperado

    if getattr(BaseClient, "_ascii_patch_applied", False):
        return  # ya está

    def _build_headers_ascii(self, options, *a, **kw):
        api_key = (options.api_key if hasattr(options, "api_key") and options.api_key
                   else os.getenv("OPENAI_API_KEY") or self.api_key)

        ua_json = json.dumps(
            {
                "bindings_version": "patched",
                "http_lib": "httpx",
                "lang": "python",
                "lang_version": platform.python_version(),
                "platform": platform.platform(),
                "publisher": "openai",
            },
            ensure_ascii=True,
        )

        hdrs = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "X-OpenAI-Client-User-Agent": ua_json,
        }
        return httpx.Headers(hdrs)

    BaseClient._build_headers = _build_headers_ascii
    BaseClient._ascii_patch_applied = True

_force_ascii_headers()

import os, sys, json, platform

def _ascii_user_agent() -> str:
    data = {
        "bindings_version": "0.0",
        "http_lib": "httpx",
        "lang": "python",
        "lang_version": platform.python_version(),
        "platform": platform.platform(),
        "publisher": "openai",
        "system_locale": "C"      # ←  fuerza ASCII puro
    }
    return json.dumps(data, ensure_ascii=True)

os.environ["OPENAI_USER_AGENT"] = _ascii_user_agent()

# PARCHE: fuerza cabeceras ASCII-safe en el SDK de OpenAI
def _patch_openai_ascii_headers() -> None:
    try:
        from openai._base_client import BaseClient  # SDK ≥ 1.0

        # Evita aplicarlo dos veces.
        if getattr(BaseClient, "_ascii_patch_applied", False):
            return

        _orig_build = BaseClient._build_headers

        def _build_headers_ascii(self, options, *args, **kwargs):
            headers = _orig_build(self, options, *args, **kwargs)

            ua_key = "X-OpenAI-Client-User-Agent"
            if ua_key in headers:
                try:
                    data = json.loads(headers[ua_key])
                except Exception:
                    data = {"agent": "nl2sql"}
                headers[ua_key] = json.dumps(data, ensure_ascii=True)

            # Garantiza ASCII en todos los valores
            for k, v in list(headers.items()):
                if isinstance(v, str):
                    try:
                        v.encode("ascii")
                    except UnicodeEncodeError:
                        headers[k] = (
                            v.encode("ascii", "backslashreplace").decode("ascii")
                        )
            return headers

        BaseClient._build_headers = _build_headers_ascii
        BaseClient._ascii_patch_applied = True  # marca
    except ImportError:
        # SDK demasiado antiguo; recomienda actualizar.
        pass


_patch_openai_ascii_headers()

# 1)  Backend Llama-cpp local
from llama_cpp import Llama

class LlamaBackend:
    def __init__(self, cfg: Dict):
        self.n_ctx = cfg.get("n_ctx", 8192)
        self.n_predict = cfg.get("n_predict", 512)
        self.stop_tokens: List[str] = cfg.get("stop", [])
        self.chat_format = "llama-2"

        self.llm = Llama(
            model_path=cfg["model_path"],
            n_ctx=self.n_ctx,
            n_gpu_layers=cfg.get("n_gpu_layers", 0),
            n_threads=cfg.get("threads"),
            temperature=cfg.get("temperature", 0.0),
            top_p=cfg.get("top_p", 0.95),
            chat_format=self.chat_format,
            verbose=False,
        )

    def generate(self, prompt: str, stop: List[str] | None = None) -> str:
        stop = stop or self.stop_tokens
        messages = [
            {
                "role": "system",
                "content": "You are SQLCoder, a model que genera consultas DuckDB SQL.",
            },
            {"role": "user", "content": prompt},
        ]
        out = self.llm.create_chat_completion(
            messages,
            max_tokens=self.n_predict,
            stop=stop,
        )
        text = out["choices"][0]["message"]["content"].strip()
        if not text:
            raise RuntimeError("La LLM devolvió cadena vacía.")
        return text


# 2)  Backend OpenAI
from openai import OpenAI

class OpenAIBackend:
    def __init__(self, cfg: Dict):
        self.client = OpenAI(api_key=cfg.get("openai_api_key") or os.getenv("OPENAI_API_KEY"))
        self.model = cfg.get("model_name", "gpt-4o")
        # Parámetros de generación
        self.temperature = cfg.get("temperature", 0)
        self.top_p = cfg.get("top_p", 1)
        self.max_tokens = cfg.get("n_predict", 512)
        self.stop: List[str] = cfg.get("stop", [])

    def generate(self, prompt: str, stop: List[str] | None = None) -> str:
        stop = stop or self.stop
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system",
                    "content": "You are SQLCoder, a model that generates SQL queries for DuckDB.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=self.temperature,
            top_p=self.top_p,
            max_tokens=self.max_tokens,
            stop=stop,
        )
        text = response.choices[0].message.content.strip()
        if not text:
            raise RuntimeError("La LLM devolvió cadena vacía.")
        return text


# 3)  Selector de back-end
def get_backend(cfg: Dict):
    model_type = cfg.get("model_type", "llama_cpp")
    if model_type == "openai":
        return OpenAIBackend(cfg)
    elif model_type == "llama_cpp":
        return LlamaBackend(cfg)
    else:
        raise ValueError(f"model_type desconocido: {model_type}")