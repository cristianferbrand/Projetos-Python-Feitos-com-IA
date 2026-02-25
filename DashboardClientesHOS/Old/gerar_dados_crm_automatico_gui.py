import csv
import fdb
import os
import schedule
import time
import json
import sys
import threading
from datetime import datetime
import tkinter as tk
from tkinter import ttk, messagebox

# ============================
# Consultas SQL (somente leitura)
# (mantidas do script original)
# ============================
query_hos = """
SELECT 
    CLIENTES.CODIGO AS CRM,
    CLIENTES.FANTASIA AS FANTASIA,
    CLIENTES.CNPJ AS CNPJ,
    (CLIENTES.FONE || ' / ' || CLIENTES.FONE2) AS TELEFONE,
    CIDADES.NOME AS CIDADE,
    CLIENTES.ENDERECO AS ENDERECO,
    CLIENTES.BAIRRO AS BAIRRO,
    CLIENTES.CEP AS CEP,
    CIDADES.CODIGO_IBGE AS CODIGO_IBGE,
    CLIENTES.ESTADO AS ESTADO,
    CLIENTES.ATIVO AS ATIVO,
    CLIENTES.STATUS AS STATUS,
    CLIENTES_WEB.QTD_DIAS_HAB AS DIAS_LIBERADOS,
    COALESCE(list(PRODUTOS.DESCRICAO || ': ' || CLIENTE_PRODUTOS.NUM_ESTACOES), 'Nenhum módulo') AS MODULOS
FROM 
    CLIENTES
JOIN 
    CLIENTES_WEB 
    ON CLIENTES_WEB.COD_CLIENTE = CLIENTES.CODIGO
JOIN 
    CIDADES 
    ON CIDADES.CODIGO = CLIENTES.CIDADE
JOIN 
    CLIENTE_PRODUTOS 
    ON CLIENTE_PRODUTOS.CODIGO_CLIENTE = CLIENTES.CODIGO
JOIN 
    PRODUTOS 
    ON PRODUTOS.CODIGO = CLIENTE_PRODUTOS.CODIGO_PRODUTO
WHERE 
    CLIENTES.STATUS = 'CLIENTE'
GROUP BY 
    CLIENTES.CODIGO,
    CLIENTES.FANTASIA,
    CLIENTES.CNPJ,
    TELEFONE,
    CIDADES.NOME,
    CLIENTES.ENDERECO,
    CLIENTES.BAIRRO,
    CLIENTES.CEP,
    CIDADES.CODIGO_IBGE,
    CLIENTES.ESTADO,
    CLIENTES.ATIVO,
    CLIENTES.STATUS,
    CLIENTES_WEB.QTD_DIAS_HAB;
"""

query_rep = """
SELECT 
    REPRESENTANTES.CODIGO AS COD_REP, 
    REPRESENTANTES.NOME AS NOME_REP,
    COUNT(CLIENTES.CODIGO) AS QTD_CLIENTES
FROM CLIENTES
JOIN REPRESENTANTES ON REPRESENTANTES.CODIGO = CLIENTES.REPRESENTANTE
JOIN CIDADES ON CIDADES.CODIGO = CLIENTES.CIDADE
WHERE CLIENTES.STATUS = 'CLIENTE'
GROUP BY 1, 2
ORDER BY 3 DESC;
"""

query_modulos = """
SELECT 
    PRODUTOS.DESCRICAO AS MODULO
    ,COUNT(CLIENTE_PRODUTOS.CODIGO_CLIENTE) AS QUANTIDADE
FROM PRODUTOS
JOIN CLIENTE_PRODUTOS ON CLIENTE_PRODUTOS.CODIGO_PRODUTO = PRODUTOS.CODIGO
JOIN CLIENTES ON CLIENTES.CODIGO = CLIENTE_PRODUTOS.CODIGO_CLIENTE
AND CLIENTES.STATUS = 'CLIENTE'
GROUP BY PRODUTOS.DESCRICAO
ORDER BY 2 DESC;
"""

# ============================
# Configuração do diretório de saída via JSON
# ============================
script_dir = os.path.dirname(os.path.abspath(__file__))

def get_output_dir() -> str:
    """
    Lê o caminho de saída (csv_folder) do arquivo JSON de configuração.
    - Procura por 'mapa_cliente_config.json' no mesmo diretório do script.
    - Se não encontrar ou houver erro ao ler, cai para o diretório do próprio script.
    - Garante a existência do diretório (cria se não existir).
    """
    config_candidates = [os.path.join(script_dir, "mapa_cliente_config.json")]

    config_path = None
    for candidate in config_candidates:
        if os.path.exists(candidate):
            config_path = candidate
            break

    if config_path is None:
        print("⚠️  Arquivo de configuração JSON não encontrado. Usando pasta do script.")
        return script_dir

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        folder = data.get("csv_folder")
        if not folder or not isinstance(folder, str):
            raise KeyError("Chave 'csv_folder' ausente ou inválida no arquivo JSON.")

        folder = os.path.normpath(folder)
        os.makedirs(folder, exist_ok=True)
        return folder

    except Exception as e:
        print(f"⚠️  Erro ao ler config '{config_path}': {e}. Usando pasta do script.")
        return script_dir

# ============================
# Execução de consulta e geração de CSV
# ============================

def execute_query_and_save_to_csv(query: str, csv_filename: str) -> None:
    try:
        # Conexão (somente leitura)
        con = fdb.connect(
            dsn="192.168.1.9/3050:crm_hos",
            user="SYSDBA",
            password="had63rg@"
        )

        # Transação READ ONLY com nível de isolamento correto
        tpb = fdb.TPB()
        tpb.read_only = True
        tpb.isolation_level = fdb.isc_tpb_read_committed
        con.begin(tpb=tpb)

        cur = con.cursor()
        cur.execute(query)

        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(columns)   # Cabeçalhos
            writer.writerows(rows)     # Linhas

        print(f"✅ CSV gerado: {csv_filename}")

    except fdb.DatabaseError as e:
        print(f"❌ Erro ao acessar o banco de dados: {e}")

    except Exception as e:
        print(f"❌ Erro inesperado: {e}")

    finally:
        if 'con' in locals() and con:
            con.close()

# ============================
# Processo principal (releitura do JSON a cada execução)
# ============================

def executar_processos() -> None:
    print("Iniciando o processo de geração dos CSVs...")

    out_dir = get_output_dir()  # <-- Sempre lê do JSON
    print(f"📁 Diretório de saída: {out_dir}")

    csv_hos = os.path.join(out_dir, "clientes_hos.csv")
    csv_rep = os.path.join(out_dir, "clientes_rep.csv")
    csv_modulos = os.path.join(out_dir, "clientes_modulos.csv")

    execute_query_and_save_to_csv(query_hos, csv_hos)
    execute_query_and_save_to_csv(query_rep, csv_rep)
    execute_query_and_save_to_csv(query_modulos, csv_modulos)

    print("Processo concluído.")

# ============================
# Utilitário para redirecionar stdout/stderr para o Text do Tkinter
# ============================

class TextRedirector:
    def __init__(self, text_widget, original_stream):
        self.text_widget = text_widget
        self.original_stream = original_stream
        self.lock = threading.Lock()

    def write(self, s: str):
        if not s:
            return
        # Escreve no console real
        try:
            self.original_stream.write(s)
        except Exception:
            pass
        # Envia para a UI sem quebrar thread-safety
        self.text_widget.after(0, self._append, s)

    def flush(self):
        try:
            self.original_stream.flush()
        except Exception:
            pass

    def _append(self, s: str):
        self.text_widget.insert(tk.END, s)
        self.text_widget.see(tk.END)

# ============================
# GUI com agendamento por MINUTOS e console embutido
# ============================

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Gerar Dados CRM - Agendador (minutos)")
        self.geometry("900x600")

        # Estado
        self.scheduler_thread = None
        self.stop_event = threading.Event()
        self.run_lock = threading.Lock()  # Evita concorrência nas execuções
        self.current_job = None
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr

        # UI
        self._build_ui()

        # Redireciona stdout/stderr para o Text
        sys.stdout = TextRedirector(self.txt_console, self._original_stdout)
        sys.stderr = TextRedirector(self.txt_console, self._original_stderr)

        # Atualiza label de próxima execução periodicamente
        self.after(1000, self._tick_next_run)

    def _build_ui(self):
        container = ttk.Frame(self, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        # Linha de controles
        controls = ttk.Frame(container)
        controls.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(controls, text="Intervalo (min):").pack(side=tk.LEFT, padx=(0, 6))

        self.cbo_interval = ttk.Combobox(
            controls,
            values=["5", "10", "30", "60", "120"],
            width=7,
            state="readonly"
        )
        self.cbo_interval.set("10")
        self.cbo_interval.pack(side=tk.LEFT)

        ttk.Label(controls, text="ou personalizado:").pack(side=tk.LEFT, padx=(10, 6))
        self.ent_custom = ttk.Entry(controls, width=7)
        self.ent_custom.pack(side=tk.LEFT)

        self.btn_start = ttk.Button(controls, text="Iniciar", command=self.start_scheduler)
        self.btn_start.pack(side=tk.LEFT, padx=(10, 4))

        self.btn_stop = ttk.Button(controls, text="Parar", command=self.stop_scheduler, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT)

        self.btn_run_now = ttk.Button(controls, text="Executar agora", command=self.run_now)
        self.btn_run_now.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_clear = ttk.Button(controls, text="Limpar console", command=self.clear_console)
        self.btn_clear.pack(side=tk.LEFT, padx=(10, 0))

        # Status
        status = ttk.Frame(container)
        status.pack(fill=tk.X, pady=(0, 10))

        self.lbl_status = ttk.Label(status, text="Status: parado")
        self.lbl_status.pack(side=tk.LEFT)

        self.lbl_next = ttk.Label(status, text="  |  Próxima execução: —")
        self.lbl_next.pack(side=tk.LEFT)

        # Console
        console_frame = ttk.LabelFrame(container, text="Console")
        console_frame.pack(fill=tk.BOTH, expand=True)

        self.txt_console = tk.Text(console_frame, height=20, wrap=tk.NONE)
        self.txt_console.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scroll_y = ttk.Scrollbar(console_frame, orient=tk.VERTICAL, command=self.txt_console.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt_console.configure(yscrollcommand=scroll_y.set)

        # Dica
        hint = ttk.Label(container, foreground="#555",
                         text=("Dica: o diretório de saída é lido de 'mapa_cliente_config.json' (chave 'csv_folder').\n"
                               "Se não existir, os CSVs serão salvos na pasta deste script."))
        hint.pack(fill=tk.X, pady=(8, 0))

    # =============== Scheduler Controls ===============
    def _get_interval_minutes(self) -> int:
        # Prioriza valor personalizado, se preenchido
        custom = self.ent_custom.get().strip()
        if custom:
            if not custom.isdigit() or int(custom) <= 0:
                raise ValueError("Informe um número inteiro de minutos > 0 no campo personalizado.")
            return int(custom)
        # Caso contrário, usa o combobox
        val = self.cbo_interval.get().strip()
        if not val.isdigit():
            raise ValueError("Selecione um intervalo válido no combobox ou informe um personalizado.")
        return int(val)

    def start_scheduler(self):
        try:
            minutes = self._get_interval_minutes()
        except ValueError as e:
            messagebox.showerror("Intervalo inválido", str(e))
            return

        # Limpa jobs anteriores e agenda o novo
        schedule.clear()
        self.current_job = schedule.every(minutes).minutes.do(self._job_wrapper)
        print(f"⏰ Tarefa agendada para executar a cada {minutes} minuto(s).")

        # Controla thread do scheduler
        self.stop_event.clear()
        if self.scheduler_thread is None or not self.scheduler_thread.is_alive():
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()

        self.lbl_status.config(text="Status: executando")
        self.btn_start.config(state=tk.DISABLED)
        self.btn_stop.config(state=tk.NORMAL)

    def stop_scheduler(self):
        self.stop_event.set()
        schedule.clear()
        self.current_job = None
        self.lbl_status.config(text="Status: parado")
        self.lbl_next.config(text="  |  Próxima execução: —")
        self.btn_start.config(state=tk.NORMAL)
        self.btn_stop.config(state=tk.DISABLED)
        print("🛑 Agendamento parado.")

    def _scheduler_loop(self):
        while not self.stop_event.is_set():
            try:
                schedule.run_pending()
            except Exception as e:
                print(f"❌ Erro no agendador: {e}")
            time.sleep(0.5)

    def _job_wrapper(self):
        # Evita sobreposição se uma execução ainda estiver em andamento
        if self.run_lock.locked():
            print("⏳ Execução anterior ainda em andamento. Aguardando próxima janela...")
            return
        with self.run_lock:
            try:
                ini = datetime.now()
                print(f"\n=== Início: {ini.strftime('%d/%m/%Y %H:%M:%S')} ===")
                executar_processos()
                fim = datetime.now()
                print(f"=== Fim: {fim.strftime('%d/%m/%Y %H:%M:%S')}  (duração: {(fim-ini).seconds}s) ===\n")
            except Exception as e:
                print(f"❌ Erro durante a execução: {e}")

    def run_now(self):
        # Executa manualmente sem bloquear a UI
        threading.Thread(target=self._job_wrapper, daemon=True).start()

    def clear_console(self):
        self.txt_console.delete("1.0", tk.END)

    def _tick_next_run(self):
        # Atualiza a informação de próxima execução a cada 1s
        try:
            if self.current_job is not None and schedule.next_run() is not None:
                nr = schedule.next_run()
                self.lbl_next.config(text=f"  |  Próxima execução: {nr.strftime('%d/%m/%Y %H:%M:%S')}")
            else:
                self.lbl_next.config(text="  |  Próxima execução: —")
        except Exception:
            self.lbl_next.config(text="  |  Próxima execução: —")
        finally:
            self.after(1000, self._tick_next_run)

    def on_close(self):
        try:
            self.stop_scheduler()
        finally:
            # Restaura stdout/stderr originais
            sys.stdout = self._original_stdout
            sys.stderr = self._original_stderr
            self.destroy()

# ============================
# Main
# ============================

if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    print("Agendador pronto. Selecione o intervalo e clique em 'Iniciar'.")
    app.mainloop()