SE USA UN VIRTUALENV DE PYTHON 3.11
1. Ve a https://www.python.org/downloads/release/python-3110/
2. Descarga el instalador “Windows installer (64-bit)”.
3. Durante la instalación, marca “Add Python 3.11 to PATH”

CREACIÓN DEL AMBIENTE VIRTUAL:
  py -3.11 -m venv .venv  

ACTIVACIÓN DEL AMBIENTE VIRTUAL:
  .venv\Scripts\activate

INSTALACIÓN DE LIBRERÍAS (proceso demorado):
  pip install -r requirements.txt

CAMBIAR DE NOMBRE:
  nl2sql\config.example.yaml -> config.yaml

AGREGAR API KEY DE OPENAI EN nl2sql\config.yaml

EJECUTAR
  python main.py

LANZAR PRIMER ETL (robusto en manejo de errores, simplemente volver a ejecutar si ocurre alguno)

REALIZAR PREGUNTAS;)
