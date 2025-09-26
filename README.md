# Executar o servidor
### Primeir ative a vm
```bash
python -m venv env
```
```bash
cd .\.venv\Scripts\
```
```bash
\activate
```
```bash
uvicorn app.main:app --reload --reload-dir app --port 5003
```