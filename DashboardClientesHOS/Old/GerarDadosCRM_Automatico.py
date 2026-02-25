import csv
import fdb
import os
import schedule
import time
import json
from datetime import datetime

# ============================
# Consultas SQL (somente leitura)
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
LEFT JOIN 
    CLIENTE_PRODUTOS 
    ON CLIENTE_PRODUTOS.CODIGO_CLIENTE = CLIENTES.CODIGO
LEFT JOIN 
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

        # Normaliza o caminho e garante que exista
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

    finally:
        if 'con' in locals() and con:
            con.close()

# ============================
# Formatação robusta de horários para o schedule
# ============================
def format_time_str(t: str) -> str:
    """
    Aceita '8:00', '08:00', '8:00:00' ou '08:00:00' e retorna
    uma string no formato HH:MM ou HH:MM:SS (ambos aceitos pelo schedule).
    Lança ValueError se o formato for inválido.
    """
    t = t.strip()
    parts = t.split(":")
    if len(parts) not in (2, 3):
        raise ValueError(f"Formato de hora inválido: '{t}'. Use HH:MM ou HH:MM:SS.")
    try:
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) == 3 else None
    except Exception:
        raise ValueError(f"Formato de hora inválido: '{t}'.")

    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"Hora/minuto fora do intervalo: '{t}'.")
    if s is not None and not (0 <= s <= 59):
        raise ValueError(f"Segundos fora do intervalo: '{t}'.")

    # Se veio com segundos, mantém HH:MM:SS; caso contrário HH:MM
    if s is not None:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}"

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
# Agendamento
# ============================
# Você pode escrever os horários com ou sem zero à esquerda que o helper irá normalizar
horarios = ["8:00", "10:00", "12:00", "14:00", "16:00", "18:00"]

for horario in horarios:
    try:
        hfmt = format_time_str(horario)
        schedule.every().day.at(hfmt).do(executar_processos)
        print(f"⏰ Tarefa agendada para {hfmt}")
    except ValueError as e:
        print(f"⚠️  Horário ignorado: {e}")

print("Agendamento configurado. Aguardando horários para execução...")

# Loop para manter o script ativo e monitorar os agendamentos
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
