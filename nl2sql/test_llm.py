from llama_cpp import Llama

llm = Llama(
    model_path="D:/OSCE_PIPELINE/models/sqlcoder-7b-2.Q4_K_M.gguf",
    n_ctx=4096,
    chat_format="llama-2",
    threads= 10,
    temperature= 0.01,
    top_p= 0.9,
    n_predict= 256,
)

prompt = """Eres un experto en SQL. Devuelve *solamente*:
```sql
SELECT * FROM contracts;
```
Nada más
"""

messages = [
    {"role": "system", "content": "You are SQLCoder, a model that writes SQL."},
    {"role": "user", "content": prompt},
]

print("=== respuesta ===")
print(llm.create_chat_completion(messages, max_tokens=100)["choices"][0]["message"]["content"])
