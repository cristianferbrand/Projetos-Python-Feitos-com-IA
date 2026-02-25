# ===============================================
# AUTO UPDATER GUI (Flet) - Windows
# Credenciais SMB + Diagnóstico + Modo Seguro (2 estágios) + Validação UNC
# ===============================================
import asyncio
import datetime as dt
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, Iterable, List, Set, Tuple, Optional

import flet as ft

# ---------- SHIM COLORS/ICONS ----------
C = getattr(ft, "Colors", None) or getattr(ft, "colors", None)
I = getattr(ft, "Icons", None) or getattr(ft, "icons", None)

def get_icon(*names: str):
    for n in names:
        try:
            return getattr(I, n)
        except Exception:
            continue
    for n in ("UPDATE", "SYSTEM_UPDATE", "CHECK", "HELP"):
        try:
            return getattr(I, n)
        except Exception:
            continue
    return None

# ---------- CONSTS ----------
APP_TITLE = "Auto Updater GUI"
DEFAULT_CONFIG_FILE = "updater_config.json"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOCK_NAME = ".updater.lock"

# ---------- BASIC HELPERS ----------
def is_frozen() -> bool:
    return getattr(sys, "frozen", False)

def self_path() -> Path:
    return Path(sys.executable if is_frozen() else __file__).resolve()

def self_dir() -> Path:
    return self_path().parent

def make_writable(path: Path):
    try:
        if path.exists():
            os.chmod(str(path), stat.S_IWRITE | stat.S_IREAD)
    except Exception:
        pass

def onerror_rm(func, path, exc_info):
    try:
        make_writable(Path(path))
        func(path)
    except Exception:
        pass

def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def list_count_files(src: Path) -> int:
    if not src.exists():
        return 0
    count = 0
    for _, _, files in os.walk(src):
        count += len(files)
    return count

# ---------- SMB / NETWORK HELPERS ----------
def is_unc(path_str: str) -> bool:
    return isinstance(path_str, str) and path_str.startswith("\\\\")

def unc_root(path_str: str) -> Optional[str]:
    if not is_unc(path_str):
        return None
    p = path_str.strip("\\")
    parts = [x for x in p.split("\\") if x]
    if len(parts) >= 2:
        return f"\\\\{parts[0]}\\{parts[1]}"
    return None

def is_valid_unc(path_str: str) -> bool:
    if not is_unc(path_str):
        return False
    p = path_str.strip("\\")
    parts = [x for x in p.split("\\") if x]
    return len(parts) >= 2

def unc_server(path_str: str) -> Optional[str]:
    if not is_unc(path_str):
        return None
    p = path_str.strip("\\")
    parts = [x for x in p.split("\\") if x]
    if len(parts) >= 1:
        return parts[0]
    return None

def run_cmd(args: List[str]) -> Tuple[int, str]:
    res = subprocess.run(["cmd", "/c"] + args, capture_output=True, text=True)
    out = (res.stdout or "") + (res.stderr or "")
    return res.returncode, out

def net_helpmsg(code: int) -> str:
    rc, out = run_cmd(["net", "helpmsg", str(code)])
    return out.strip()

def extract_system_error_code(text: str) -> Optional[int]:
    m = re.search(r"(?:Erro de sistema|System error)\s+(\d+)", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def explain_common(code: int) -> str:
    hints = {
        5:   "Acesso negado: permissões insuficientes no compartilhamento/pasta.",
        53:  "Caminho/servidor não encontrado: DNS, firewall ou servidor offline.",
        64:  "Nome de rede não encontrado: problema de rede/roteamento/DNS.",
        67:  "Compartilhamento inválido/não encontrado.",
        85:  "Rota já está em uso (conflito de mapeamento).",
        86:  "Senha incorreta.",
        1219:"Conexões múltiplas com credenciais diferentes: desconecte conexões existentes e tente novamente.",
        1326:"Falha de logon: usuário ou senha inválidos.",
        1909:"Conta bloqueada ou desabilitada.",
        121: "Tempo limite expirou: rede instável.",
    }
    return hints.get(code, "Veja a mensagem do Windows acima para detalhes.")

def ping_server(server: str, timeout_ms: int = 1200) -> Tuple[int, str]:
    rc, out = run_cmd(["ping", "-n", "1", "-w", str(timeout_ms), server])
    return rc, out

def net_use_delete(unc: str) -> Tuple[int, str]:
    return run_cmd(["net", "use", unc, "/delete", "/y"])

def net_use_connect(unc: str, username: str, password: str, domain: Optional[str]) -> Tuple[int, str, Optional[int], str]:
    user_spec = f"{domain}\\{username}" if domain else username
    rc, out = run_cmd(["net", "use", unc, "/user:" + user_spec, password, "/persistent:no"])
    code = extract_system_error_code(out)
    helpmsg = net_helpmsg(code) if code is not None else ""
    return rc, out, code, helpmsg

# ---------- ENGINE ----------
class UpdateEngine:
    def __init__(self, logger: Callable[[str], None]):
        self.logger = logger

    def _log(self, msg: str):
        stamp = dt.datetime.now().strftime("%H:%M:%S")
        self.logger(f"[{stamp}] {msg}")

    def _copy_tree(self, src: Path, dst: Path, exclude_names: Set[str], manifest: list = None):
        if manifest is None:
            manifest = []
        for root, dirs, files in os.walk(src):
            rel = Path(root).relative_to(src)
            dest_dir = dst / rel
            dest_dir.mkdir(parents=True, exist_ok=True)
            for d in dirs:
                if d in exclude_names:
                    continue
                (dest_dir / d).mkdir(parents=True, exist_ok=True)
            for f in files:
                if f in exclude_names:
                    self._log(f"[skip] {rel / f} (excluído/keep)")
                    continue
                s = Path(root) / f
                d = dest_dir / f
                ensure_parent(d)
                shutil.copy2(s, d)
                relpath = str((rel / f).as_posix())
                manifest.append(relpath)
                self._log(f"[copy] {relpath}")

    def _delete_all_in_dir_except(self, dest: Path, keep_names: Set[str]):
        for item in dest.iterdir():
            name = item.name
            if name in keep_names:
                self._log(f"[keep] {item}")
                continue
            try:
                if item.is_dir():
                    self._log(f"[delete dir] {item}")
                    shutil.rmtree(item, onerror=onerror_rm)
                else:
                    self._log(f"[delete file] {item}")
                    make_writable(item)
                    item.unlink(missing_ok=True)
            except Exception as e:
                self._log(f"Falha ao excluir {item}: {e}")

    def _zip_backup(self, src_dir: Path, backup_dir: Path, keep_names: Set[str]) -> Path:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_base = backup_dir / f"backup_{stamp}"
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for root, dirs, files in os.walk(src_dir):
                rel = Path(root).relative_to(src_dir)
                dst_root = tmpdir / rel
                dst_root.mkdir(parents=True, exist_ok=True)
                for d in dirs:
                    if d in keep_names:
                        continue
                    (dst_root / d).mkdir(parents=True, exist_ok=True)
                for f in files:
                    if f in keep_names:
                        continue
                    s = Path(root) / f
                    d = dst_root / f
                    shutil.copy2(s, d)
            shutil.make_archive(str(zip_base), "zip", tmpdir)
        return zip_base.with_suffix(".zip")

    def taskkill_safe(self, process_name: str) -> str:
        try:
            here = self_path()
            curr_exe_name = Path(sys.executable).name
            if process_name.lower() in {curr_exe_name.lower(), here.name.lower()}:
                return f"[skip kill] '{process_name}' é o processo do atualizador. Ignorado."
            pid = os.getpid()
            cmd = f'taskkill /IM "{process_name}" /T /F /FI "PID ne {pid}"'
            res = subprocess.run(["cmd", "/c", cmd], capture_output=True, text=True, shell=False)
            out = (res.stdout or "") + (res.stderr or "")
            return f"taskkill {process_name} -> code {res.returncode}; {out.strip()}"
        except Exception as e:
            return f"Erro ao finalizar {process_name}: {e}"

    def write_swapper_bat(self, dest: Path, staging_dir: Path, keep_names: Set[str], restart_cmd: Optional[str]) -> Path:
        bat = dest / "_swap_apply_update.bat"
        keep_list = " ".join([f'"{name}"' for name in sorted(keep_names)])
        lines = r"""
@echo off
setlocal ENABLEDELAYEDEXPANSION
set DEST={dest}
set STAGING={staging}
set PID={pid}
set NOW=%DATE:/=-%_%TIME::=-%
set NOW=%NOW: =_%
set LOGDIR=%DEST%\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
set LOG=%LOGDIR%\swapper_%%NOW%%.log

echo [Swapper] Aguardando o encerramento do atualizador (PID %PID%)... | tee -a "%LOG%" >nul 2>nul
:waitloop
for /f "tokens=2 delims=," %%a in ('tasklist /FI "PID eq %PID%" /FO CSV ^| findstr /I "%PID%"') do (
  timeout /t 1 /nobreak >nul
  goto waitloop
)

echo [Swapper] Limpando destino (exceto itens keep)... | tee -a "%LOG%" >nul 2>nul
pushd "%DEST%"
for /f "delims=" %%F in ('dir /b /a') do (
  set NAME=%%F
  set skip=0
  for %%K in ({keep_items}) do (
    if /I "%%F"==%%~K set skip=1
  )
  if "!skip!"=="1" (
    echo [keep] %%F | tee -a "%LOG%" >nul 2>nul
  ) else (
    rmdir /s /q "%%F" 2>nul
    del /f /q "%%F" 2>nul
    echo [del] %%F | tee -a "%LOG%" >nul 2>nul
  )
)
popd

echo [Swapper] Aplicando staging -> destino... (isso pode demorar) | tee -a "%LOG%" >nul 2>nul
robocopy "%STAGING%" "%DEST%" /E /COPY:DAT /R:2 /W:2 /TEE /LOG+:"%LOG%"
set RC=%ERRORLEVEL%
echo [Swapper] ROBOCOPY RC=%RC% | tee -a "%LOG%" >nul 2>nul

if %RC% GEQ 8 (
  echo [Swapper] ERRO: falha de cópia. Staging será mantido. | tee -a "%LOG%" >nul 2>nul
  echo Consulte o log em "%LOG%". | tee -a "%LOG%" >nul 2>nul
  exit /b %RC%
)

for %%M in ("%STAGING%\copiados_*.txt") do (
  if not exist "%DEST%\%%~nxM" copy "%%~fM" "%DEST%\%%~nxM" >nul
)

echo [Swapper] Removendo pasta de staging... | tee -a "%LOG%" >nul 2>nul
rmdir /s /q "%STAGING%" 2>nul

{restart_block}

echo [Swapper] Concluido. | tee -a "%LOG%" >nul 2>nul
"""
        restart_block = ""
        if restart_cmd:
            restart_block = f'echo [Swapper] Reiniciando app... | tee -a "%LOG%" >nul 2>nul\nstart "" {restart_cmd}\n'
        content = lines.format(
            dest=str(dest),
            staging=str(staging_dir),
            pid=os.getpid(),
            keep_items=keep_list,
            restart_block=restart_block
        )
        bat.write_text(content, encoding="utf-8")
        return bat

    async def run(
        self,
        source_path_str: str,
        dest: Path,
        processes_to_kill: Iterable[str],
        keep_names: Set[str],
        create_backup_zip: bool = True,
        retries: int = 2,
        retry_wait: int = 2,
        dry_run: bool = False,
        auth: Optional[dict] = None,
        safe_mode: bool = False,
        restart_after: Optional[str] = None,
    ) -> Tuple[int, Optional[Path]]:
        lock_path = dest / DEFAULT_LOCK_NAME
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(str(os.getpid()))
        except FileExistsError:
            self._log("Outro processo do atualizador está em execução. Abortando.")
            return 9, None

        swapper_bat = None
        try:
            self._log(f"Origem: {source_path_str}")
            self._log(f"Destino: {dest}")
            self._log(f"Keep: {sorted(keep_names)}")
            self._log(f"Processos a finalizar: {list(processes_to_kill)}")
            self._log(f"Dry-run: {dry_run}")
            self._log(f"Modo seguro (2 estágios): {safe_mode}")

            for p in processes_to_kill:
                if not p:
                    continue
                out = await asyncio.to_thread(self.taskkill_safe, p)
                self._log(out)

            if is_unc(source_path_str) and auth and auth.get("use_credentials"):
                if not is_valid_unc(source_path_str):
                    self._log("Caminho UNC inválido. Use \\\\SERVIDOR\\\\Compartilhamento\\\\pasta.")
                    return 3, None
                unc = unc_root(source_path_str)
                domain = auth.get("domain") or ""
                username = auth.get("username") or ""
                password = auth.get("password") or ""
                self._log(f"Tentando autenticar no compartilhamento: {unc} (usuário: {domain+'\\' if domain else ''}{username})")
                rc, out, code, helpmsg = await asyncio.to_thread(net_use_connect, unc, username, password, domain)
                masked_out = out.replace(password, "******") if password else out
                if rc != 0:
                    self._log(f"`net use` falhou (rc={rc}).")
                    if code is not None:
                        self._log(f"Código Windows: {code} — {helpmsg or 'sem mensagem'}")
                        self._log(f"Dica: {explain_common(code)}")
                        if code == 1219:
                            self._log("Tentando desconectar conexões existentes...")
                            _rc_del, _out_del = await asyncio.to_thread(net_use_delete, unc)
                            self._log(_out_del.strip())
                            self._log("Re-tentando autenticar...")
                            rc2, out2, code2, help2 = await asyncio.to_thread(net_use_connect, unc, username, password, domain)
                            if rc2 != 0:
                                self._log(f"Falhou novamente (rc={rc2}). Código {code2 or 'N/A'} — {help2 or 'sem mensagem'}")
                                server = unc_server(source_path_str)
                                if server:
                                    prc, pout = await asyncio.to_thread(ping_server, server)
                                    self._log(("PING OK" if prc == 0 else "PING FALHOU") + f" para {server}.")
                                return 3, None
                            else:
                                self._log("Autenticado após desconectar conexões anteriores.")
                        else:
                            server = unc_server(source_path_str)
                            if server:
                                prc, pout = await asyncio.to_thread(ping_server, server)
                                self._log(("PING OK" if prc == 0 else "PING FALHOU") + f" para {server}.")
                            return 3, None
                    else:
                        self._log("Não foi possível identificar o código do erro do Windows.")
                        self._log(masked_out.strip())
                        return 3, None
                else:
                    self._log("Autenticado com sucesso no compartilhamento.")

            if is_unc(source_path_str) and not is_valid_unc(source_path_str):
                self._log("Caminho UNC inválido. Use \\\\SERVIDOR\\\\Compartilhamento\\\\pasta.")
                return 3, None
            src_path = Path(source_path_str)
            if not src_path.exists():
                self._log(f"ERRO: pasta de origem não encontrada: {source_path_str}")
                if is_unc(source_path_str):
                    self._log("Verifique se o caminho está correto (\\\\servidor\\compartilhamento\\pasta).")
                return 3, None

            if not dest.exists():
                if dry_run:
                    self._log(f"[mkdest] {dest}")
                else:
                    dest.mkdir(parents=True, exist_ok=True)

            if create_backup_zip:
                backups_dir = dest / "backups"
                if not dry_run:
                    backups_dir.mkdir(parents=True, exist_ok=True)
                    zip_file = await asyncio.to_thread(self._zip_backup, dest, backups_dir, keep_names)
                    self._log(f"Backup criado: {zip_file}")
                else:
                    self._log(f"[backup] Geraria zip em {backups_dir} (dry-run)")

            staging_parent = dest / ".update_staging"
            staging_dir = staging_parent / dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            if dry_run:
                self._log(f"[staging] Prepararia staging em {staging_dir}")
            else:
                staging_dir.mkdir(parents=True, exist_ok=True)

            attempt = 0
            manifest = []
            while True:
                try:
                    attempt += 1
                    self._log(f"Cópia origem -> staging (tentativa {attempt})")
                    if dry_run:
                        total = list_count_files(src_path)
                        self._log(f"[dry-run] Copiaria ~{total} arquivos para staging.")
                    else:
                        await asyncio.to_thread(self._copy_tree, src_path, staging_dir, keep_names, manifest)
                        break
                except Exception as e:
                    self._log(f"Falha na cópia: {e}")
                    if attempt <= retries:
                        self._log(f"Aguardando {retry_wait}s e tentando novamente...")
                        await asyncio.sleep(retry_wait)
                    else:
                        self._log("Limite de tentativas atingido. Abortando.")
                        return 4, None

            # Grava lista de arquivos copiados (manifest) na raiz do staging
            if not dry_run:
                try:
                    manifest_name = f"copiados_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                    manifest_file = staging_dir / manifest_name
                    manifest_file.write_text("\n".join(manifest), encoding="utf-8")
                    self._log(f"Manifesto gravado: {manifest_file}")
                except Exception as me:
                    self._log(f"Aviso: não foi possível gravar manifesto: {me}")

            if safe_mode:
                self._log("Gerando BAT de aplicação (2 estágios)...")
                protected = keep_names | {staging_parent.name, DEFAULT_LOG_DIR, "backups"}
                swapper_bat = self.write_swapper_bat(dest, staging_dir, protected, restart_after)
                self._log(f"Gerado: {swapper_bat}")
                return 0, swapper_bat

            self._log("Limpando destino (exceto itens 'keep')...")
            protected = keep_names | {staging_parent.name, DEFAULT_LOG_DIR, "backups"}
            if dry_run:
                self._log(f"[dry-run] Excluiria tudo de {dest} exceto {sorted(protected)}")
            else:
                await asyncio.to_thread(self._delete_all_in_dir_except, dest, protected)

            self._log("Movendo staging -> destino...")
            if dry_run:
                self._log("[dry-run] Não moverá arquivos (simulação).")
            else:
                manifest_moved = False
                for item in staging_dir.iterdir():
                    target = dest / item.name
                    if item.name in keep_names:
                        self._log(f"[skip move] {item.name} (keep)")
                        continue
                    if target.exists():
                        if target.is_dir():
                            shutil.rmtree(target, onerror=onerror_rm)
                        else:
                            make_writable(target)
                            target.unlink(missing_ok=True)
                    if item.is_file() and item.name.lower().startswith("copiados_") and item.suffix.lower() == ".txt":
                        # força manifest na raiz do DEST
                        shutil.copy2(str(item), str(dest / item.name))
                        manifest_moved = True
                    else:
                        shutil.move(str(item), str(target))
                if not manifest_moved:
                    # tente localizar manifesto e copiar
                    for mf in staging_dir.glob("copiados_*.txt"):
                        try:
                            shutil.copy2(str(mf), str(dest / mf.name))
                            break
                        except Exception:
                            pass
                try:
                    shutil.rmtree(staging_parent, onerror=onerror_rm)
                except Exception as e:
                    self._log(f"Aviso: não foi possível remover staging: {e}")

            self._log("Atualização concluída.")
            return 0, None
        finally:
            try:
                if lock_path.exists():
                    lock_path.unlink(missing_ok=True)
            except Exception:
                pass

# ---------- CONFIG ----------
def default_config(dest: Path) -> dict:
    return {
        "source_path": r"\\SERVIDOR\Share\AppBuild",
        "dest_path": str(dest),
        "keep": [
            "auto_updater_gui.py",
            "auto_updater_gui.exe",
            "updater_config.json",
            "logs",
            "backups",
        ],
        "processes_to_kill": ["MeuApp.exe"],
        "max_retries": 2,
        "retry_wait_seconds": 2,
        "create_backup_zip": True,
        "auth": {
            "use_credentials": False,
            "domain": "",
            "username": "",
            "password": "",
            "save_password": False,
            "delete_connection_after": False
        },
        "safe_mode": True,
        "restart_after": ""
    }

def load_config_from(dest: Path) -> dict:
    cfg_path = dest / DEFAULT_CONFIG_FILE
    if cfg_path.exists():
        try:
            with cfg_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_config_to(dest: Path, cfg: dict):
    cfg_path = dest / DEFAULT_CONFIG_FILE
    with cfg_path.open("w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

# ---------- UI ----------
async def main(page: ft.Page):
    page.title = APP_TITLE
    page.window_width = 1024
    page.window_height = 760
    page.padding = 16
    page.scroll = "auto"
    page.theme_mode = ft.ThemeMode.DARK
    page.horizontal_alignment = ft.CrossAxisAlignment.STRETCH

    log_list = ft.ListView(expand=1, spacing=4, auto_scroll=True)
    def ui_log(msg: str):
        log_list.controls.append(ft.Text(msg, selectable=True, size=12))
        page.update()

    engine = UpdateEngine(logger=ui_log)
    dest_dir = self_dir()

    pick_source = ft.FilePicker(on_result=lambda ev: (source_tf.__setattr__("value", ev.path or source_tf.value), page.update()))
    pick_dest = ft.FilePicker(on_result=lambda ev: (dest_tf.__setattr__("value", ev.path or dest_tf.value), page.update()))
    page.overlay.extend([pick_source, pick_dest])

    source_tf = ft.TextField(label="Pasta de origem (rede/UNC)", hint_text=r"\\SERVIDOR\Share\AppBuild", expand=1)
    dest_tf = ft.TextField(label="Pasta destino (onde o atualizador está)", value=str(dest_dir), expand=1, read_only=True)

    btn_source = ft.OutlinedButton("Procurar…", icon=get_icon("FOLDER_OPEN","FOLDER"), on_click=lambda e: pick_source.get_directory_path(dialog_title="Selecionar pasta de origem"))
    btn_dest = ft.OutlinedButton("Procurar…", icon=get_icon("FOLDER_OPEN","FOLDER"), on_click=lambda e: pick_dest.get_directory_path(dialog_title="Selecionar pasta destino"))

    keep_tf = ft.TextField(label="Itens 'keep' (separados por ;)", value="auto_updater_gui.py;auto_updater_gui.exe;updater_config.json;logs;backups", expand=1)
    kill_tf = ft.TextField(label="Processos para finalizar (separados por ;)", value="MeuApp.exe", expand=1)
    backup_switch = ft.Switch(label="Criar backup .zip antes", value=True)
    dry_run_switch = ft.Switch(label="Simular atualização (dry-run)", value=False)
    safe_mode_sw = ft.Switch(label="Modo seguro (2 estágios via BAT)", value=True)
    restart_tf = ft.TextField(label="Comando para reiniciar app após aplicar (opcional)", hint_text="MeuApp.exe", expand=1)

    # Credenciais SMB
    use_creds_sw = ft.Switch(label="Usar credenciais (SMB)", value=False)
    domain_tf = ft.TextField(label="Domínio (opcional)", hint_text="MEUDOMINIO", expand=1, disabled=True)
    user_tf = ft.TextField(label="Usuário", hint_text="usuario", expand=1, disabled=True)
    pass_tf = ft.TextField(label="Senha", password=True, can_reveal_password=True, expand=1, disabled=True)
    save_pw_sw = ft.Switch(label="Salvar senha no config (texto puro ⚠️)", value=False, disabled=True)
    del_conn_sw = ft.Switch(label="Desconectar após rodar (net use /delete)", value=False, disabled=True)

    def toggle_creds(enabled: bool):
        domain_tf.disabled = not enabled
        user_tf.disabled = not enabled
        pass_tf.disabled = not enabled
        save_pw_sw.disabled = not enabled
        del_conn_sw.disabled = not enabled
        page.update()

    def on_creds_change(e):
        toggle_creds(use_creds_sw.value)
    use_creds_sw.on_change = on_creds_change

    progress = ft.ProgressBar(visible=False)
    status_text = ft.Text("Pronto.", size=12)

    async def test_source(e):
        src_str = (source_tf.value or "").strip()
        if not src_str:
            ui_log("Informe a pasta de origem.")
            return
        if is_unc(src_str) and not is_valid_unc(src_str):
            ui_log("Caminho UNC inválido: use \\\\SERVIDOR\\\\Compartilhamento\\\\pasta")
            return
        if is_unc(src_str) and use_creds_sw.value:
            unc = unc_root(src_str)
            ui_log(f"Tentando autenticar em {unc}...")
            rc, out, code, helpmsg = await asyncio.to_thread(net_use_connect, unc, (user_tf.value or ""), (pass_tf.value or ""), (domain_tf.value or ""))
            masked = out.replace(pass_tf.value or "", "******") if (pass_tf.value) else out
            if rc != 0:
                ui_log(f"`net use` falhou (rc={rc}).")
                if code is not None:
                    ui_log(f"Código Windows: {code} — {helpmsg or 'sem mensagem'}")
                    ui_log(f"Dica: {explain_common(code)}")
                    server = unc_server(src_str)
                    if server:
                        prc, _ = await asyncio.to_thread(ping_server, server)
                        ui_log(("PING OK" if prc == 0 else "PING FALHOU") + f" para {server}.")
                else:
                    ui_log(masked.strip())
                return
            else:
                ui_log("Autenticado com sucesso.")
                if del_conn_sw.value:
                    _rc, _out = await asyncio.to_thread(net_use_delete, unc)
                    ui_log(_out.strip())
        p = Path(src_str)
        if not p.exists():
            ui_log("ERRO: origem não encontrada após autenticação.")
            return
        count = await asyncio.to_thread(list_count_files, p)
        ui_log(f"OK: origem acessível. ~{count} arquivos.")

    def parse_list(text: str) -> List[str]:
        return [s.strip() for s in (text or "").split(";") if s.strip()]

    def build_keep_set() -> Set[str]:
        names = set(parse_list(keep_tf.value))
        names.add(self_path().name)
        names.update({"logs", "backups"})
        return names

    def build_auth_cfg() -> dict:
        return {
            "use_credentials": use_creds_sw.value,
            "domain": (domain_tf.value or ""),
            "username": (user_tf.value or ""),
            "password": (pass_tf.value or ""),
            "save_password": save_pw_sw.value,
            "delete_connection_after": del_conn_sw.value
        }

    async def do_update(simular: bool):
        progress.visible = True
        status_text.value = "Atualizando..." if not simular else "Simulando..."
        page.update()

        auth_cfg = build_auth_cfg()
        cfg = {
            "source_path": source_tf.value.strip(),
            "dest_path": dest_tf.value.strip() or str(self_dir()),
            "keep": sorted(list(build_keep_set())),
            "processes_to_kill": parse_list(kill_tf.value),
            "max_retries": 2,
            "retry_wait_seconds": 2,
            "create_backup_zip": backup_switch.value,
            "auth": {**auth_cfg, "password": (auth_cfg["password"] if auth_cfg["save_password"] else "")},
            "safe_mode": safe_mode_sw.value,
            "restart_after": restart_tf.value.strip(),
        }
        await asyncio.to_thread(save_config_to, self_dir(), cfg)

        rc, swapper_bat = await engine.run(
            source_path_str=cfg["source_path"],
            dest=Path(cfg["dest_path"]),
            processes_to_kill=cfg["processes_to_kill"],
            keep_names=set(cfg["keep"]),
            create_backup_zip=cfg["create_backup_zip"],
            retries=cfg["max_retries"],
            retry_wait=cfg["retry_wait_seconds"],
            dry_run=simular,
            auth=auth_cfg,
            safe_mode=cfg["safe_mode"],
            restart_after=(cfg["restart_after"] or None)
        )
        if rc == 0 and not simular and cfg["safe_mode"] and swapper_bat:
            ui_log("Aplicador gerado. A janela será fechada para aplicar a atualização.")
            try:
                subprocess.Popen(['cmd', '/c', 'start', '""', str(swapper_bat)], cwd=str(self_dir()))
            except Exception as e:
                ui_log(f"Falha ao iniciar aplicador: {e}")
            await asyncio.sleep(0.5)
            page.window_close()
            return

        status_text.value = "Concluído." if rc == 0 else f"Finalizado com código {rc}."
        progress.visible = False
        page.update()

    async def on_update_now(e):
        await do_update(simular=False)

    async def on_simulate(e):
        await do_update(simular=True)

    async def on_save_config(e):
        auth_cfg = build_auth_cfg()
        cfg = {
            "source_path": source_tf.value.strip(),
            "dest_path": dest_tf.value.strip() or str(self_dir()),
            "keep": sorted(list(build_keep_set())),
            "processes_to_kill": parse_list(kill_tf.value),
            "max_retries": 2,
            "retry_wait_seconds": 2,
            "create_backup_zip": backup_switch.value,
            "auth": {**auth_cfg, "password": (auth_cfg["password"] if auth_cfg["save_password"] else "")},
            "safe_mode": safe_mode_sw.value,
            "restart_after": restart_tf.value.strip(),
        }
        await asyncio.to_thread(save_config_to, self_dir(), cfg)
        ui_log("Configuração salva.")

    async def on_load_config(e):
        cfg = await asyncio.to_thread(load_config_from, self_dir())
        if not cfg:
            ui_log("Nenhuma configuração encontrada. Um arquivo será criado ao salvar.")
            return
        source_tf.value = cfg.get("source_path", source_tf.value)
        dest_tf.value = cfg.get("dest_path", dest_tf.value)
        keep_tf.value = ";".join(cfg.get("keep", keep_tf.value.split(";")))
        kill_tf.value = ";".join(cfg.get("processes_to_kill", kill_tf.value.split(";")))
        backup_switch.value = bool(cfg.get("create_backup_zip", True))
        safe_mode_sw.value = bool(cfg.get("safe_mode", True))
        restart_tf.value = cfg.get("restart_after", "")

        auth = cfg.get("auth", {})
        use_creds_sw.value = bool(auth.get("use_credentials", False))
        domain_tf.value = auth.get("domain", "")
        user_tf.value = auth.get("username", "")
        pass_tf.value = auth.get("password", "")
        save_pw_sw.value = bool(auth.get("save_password", False))
        del_conn_sw.value = bool(auth.get("delete_connection_after", False))
        toggle_creds(use_creds_sw.value)
        page.update()
        ui_log("Configuração carregada.")

    async def on_open_logs(e):
        logs_dir = self_dir() / DEFAULT_LOG_DIR
        logs_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.startfile(str(logs_dir))
        except Exception as ex:
            ui_log(f"Não foi possível abrir o diretório de logs: {ex}")

    async def on_clear_log(e):
        log_list.controls.clear()
        page.update()

    async def on_gen_schedule_bat(e):
        here = self_path()
        if here.suffix.lower() == ".exe":
            exec_cmd = f'"{here}"'
        else:
            exec_cmd = f'py -3 "{here}"'
        bat = self_dir() / "agendar_atualizador.bat"
        bat.write_text(f"""@echo off
echo Criando/Atualizando tarefa agendada 'AutoUpdaterApp'...
SCHTASKS /Create /F /SC MINUTE /MO 30 /TN "AutoUpdaterApp" /TR "{exec_cmd}" /RL HIGHEST /RU SYSTEM
if %ERRORLEVEL% EQU 0 (
    echo Tarefa criada com sucesso.
) else (
    echo Falha ao criar a tarefa. Tente executar este BAT como Administrador.
)
pause
""", encoding="utf-8")
        ui_log(f"Arquivo gerado: {bat}")
        try:
            os.startfile(str(bat.parent))
        except Exception:
            pass

    header = ft.Row(
        controls=[
            ft.Icon(get_icon("SYSTEM_UPDATE_ALT","SYSTEM_UPDATE","UPDATE"), size=24),
            ft.Text(APP_TITLE, size=20, weight=ft.FontWeight.BOLD),
            ft.Container(expand=1),
            ft.IconButton(get_icon("DESCRIPTION","ARTICLE"), tooltip="Salvar config", on_click=on_save_config),
            ft.IconButton(get_icon("CLOUD_DOWNLOAD","CLOUD"), tooltip="Carregar config", on_click=on_load_config),
        ],
        alignment=ft.MainAxisAlignment.START,
        vertical_alignment=ft.CrossAxisAlignment.CENTER,
    )

    src_row = ft.Row([source_tf, btn_source], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER)
    dst_row = ft.Row([dest_tf, btn_dest], alignment=ft.MainAxisAlignment.START, vertical_alignment=ft.CrossAxisAlignment.CENTER)

    options = ft.Row([keep_tf], alignment=ft.MainAxisAlignment.START)
    options2 = ft.Row([kill_tf], alignment=ft.MainAxisAlignment.START)
    switches = ft.Row([backup_switch, dry_run_switch, safe_mode_sw], alignment=ft.MainAxisAlignment.START)
    restart_row = ft.Row([restart_tf], alignment=ft.MainAxisAlignment.START)

    creds = ft.Column([
        ft.Row([ft.Text("Acesso à origem (SMB)", weight=ft.FontWeight.BOLD)]),
        ft.Row([use_creds_sw]),
        ft.Row([domain_tf, user_tf, pass_tf], vertical_alignment=ft.CrossAxisAlignment.CENTER),
        ft.Row([save_pw_sw, del_conn_sw], vertical_alignment=ft.CrossAxisAlignment.CENTER),
    ])

    actions = ft.Row(
        controls=[
            ft.ElevatedButton("Testar Origem / Credenciais", icon=get_icon("CHECK","TASK_ALT","DONE"), on_click=test_source),
            ft.ElevatedButton("Simular (Dry‑run)", icon=get_icon("PLAY_CIRCLE","PLAY_ARROW"), on_click=on_simulate),
            ft.FilledButton("ATUALIZAR AGORA", icon=get_icon("SYSTEM_UPDATE","UPDATE"), on_click=on_update_now),
            ft.OutlinedButton("Abrir Logs", icon=get_icon("FOLDER","FOLDER_OPEN"), on_click=on_open_logs),
            ft.OutlinedButton("Gerar BAT de Agendamento", icon=get_icon("SCHEDULE","EVENT","ALARM"), on_click=on_gen_schedule_bat),
            ft.OutlinedButton("Limpar Log", icon=get_icon("CLEAR_ALL","DELETE_SWEEP"), on_click=on_clear_log),
        ],
        wrap=True, spacing=8, run_spacing=8,
    )

    log_card = ft.Card(
        content=ft.Container(
            content=ft.Column([ft.Text("Log de Execução", size=14, weight=ft.FontWeight.BOLD), log_list], tight=True),
            padding=12,
        )
    )

    status_bar = ft.Row([progress, status_text])

    page.add(header, ft.Divider(), src_row, dst_row, options, options2, switches, restart_row, ft.Divider(), creds, ft.Divider(), actions, log_card, status_bar)

    cfg = load_config_from(self_dir())
    if not cfg:
        cfg = default_config(self_dir())
        save_config_to(self_dir(), cfg)
    source_tf.value = cfg.get("source_path", source_tf.value)
    dest_tf.value = cfg.get("dest_path", dest_tf.value)
    keep_tf.value = ";".join(cfg.get("keep", keep_tf.value.split(";")))
    kill_tf.value = ";".join(cfg.get("processes_to_kill", kill_tf.value.split(";")))
    backup_switch.value = bool(cfg.get("create_backup_zip", True))
    safe_mode_sw.value = bool(cfg.get("safe_mode", True))
    restart_tf.value = cfg.get("restart_after", "")

    auth = cfg.get("auth", {})
    use_creds_sw.value = bool(auth.get("use_credentials", False))
    domain_tf.value = auth.get("domain", "")
    user_tf.value = auth.get("username", "")
    pass_tf.value = auth.get("password", "")
    save_pw_sw.value = bool(auth.get("save_password", False))
    del_conn_sw.value = bool(auth.get("delete_connection_after", False))
    toggle_creds(use_creds_sw.value)

    page.update()

if __name__ == "__main__":
    ft.app(target=main)