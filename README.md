# Executar o servidor
### Primeir ative a vm
```bash
python -m venv env
```
```bash
cd env\Scripts\
```
```bash
\activate
```
```bash
pip install -r .\Requirements.txt
```
```bash
uvicorn app.main:app --reload --reload-dir app --port 5003
```