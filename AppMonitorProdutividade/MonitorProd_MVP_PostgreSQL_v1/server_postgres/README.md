# Servidor MonitorProd (PostgreSQL)

## Requisitos
- Python 3.11+ (recomendado)
- PostgreSQL 14+ (recomendado)

## Instalação
1. Crie venv e instale dependências:
   ```
   pip install -r requirements.txt
   ```
2. Crie `.env`:
   ```
   SECRET_KEY=...
   DATABASE_URL=postgresql+psycopg2://monitor:SENHA@HOST:5432/monitor_prod
   MEDIA_DIR=media
   TZ=America/Sao_Paulo
   ```
3. Rode:
   ```
   python server_postgres.py
   ```
4. Exponha com HTTPS via Nginx/Caddy/IIS em produção.

## Endpoints principais
- `POST /api/agent/heartbeat`
- `POST /api/agent/events`
- `POST /api/agent/screenshot`
- `GET  /api/devices`
- `GET  /api/device/{id}/summary/today`
- `GET  /api/device/{id}/last_screenshot`
