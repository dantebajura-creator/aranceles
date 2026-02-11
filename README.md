
# Aranceles (ESCO) - versión para publicar

## Importante sobre webcindario.com
- Este proyecto es una app Python/Flask.
- Si tu hosting NO soporta Python (muchos planes de webcindario son solo HTML/PHP), no va a correr ahí directo.
- Solución típica: hostear Flask en un servicio con Python (Render/Railway/PythonAnywhere/VPS) y en tu dominio:
  - usar un subdominio apuntando (CNAME) o
  - embeber con iframe (si te sirve) o
  - reverse proxy si tenés un servidor propio.

## Variables (Sheets)
Windows:
```bat
set ARANCELES_SHEET_ID=1vBSfNxqbg1Z1OMxTub41vXqMiWojp_NiaPXzglXAgCw
set ARANCELES_SHEET_GID=0
set ESCO_BASE_URL=http://190.210.249.97:8003
set ESCO_API_VERSION=9
set FLASK_SECRET=pon_una_clave_larga
```

## Ejecutar
```bash
pip install -r requirements.txt
python app.py
```
