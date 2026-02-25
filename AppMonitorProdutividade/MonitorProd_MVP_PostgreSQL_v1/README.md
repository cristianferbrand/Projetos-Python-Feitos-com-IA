# MonitorProd MVP (PostgreSQL + Flet)

Estrutura:
- `server_postgres/` — FastAPI + SQLAlchemy + PostgreSQL (endpoints e mídia)
- `agent/` — Agente Windows (Win32 + psutil + mss + blur opcional)
- `dashboard/` — Painel Flet (lista dispositivos, última captura, top apps)

## Passos rápidos
1) **Servidor**:
   - Edite `server_postgres/.env` com sua `DATABASE_URL`.
   - `pip install -r server_postgres/requirements.txt`
   - `python server_postgres/server_postgres.py`

2) **Agente**:
   - Edite `agent/agent_config.json` (server_url, token, device_name).
   - `pip install -r agent/requirements.txt`
   - `python agent/agent.py`
   - (Opcional) instale como serviço com `agent/install_service_nssm.bat` (ajuste caminhos).

3) **Dashboard**:
   - `pip install -r dashboard/requirements.txt`
   - `python dashboard/dashboard_flet.py` (opcional: configure `API_URL`)

## Segurança e LGPD
- Use **HTTPS**, RBAC no painel e **consentimento informado** dos usuários monitorados.
- Ative **blur** e **blocklist** para janelas/processos sensíveis.
- Defina **retenção** (ex.: 90 dias) e políticas internas publicadas.

Bom uso!