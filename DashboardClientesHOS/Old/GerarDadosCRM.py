import csv
import fdb
import os  # Para lidar com diretórios e caminhos
import schedule
import time

# Consultas SQL (somente leitura)
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

# Caminho para salvar os arquivos CSV na mesma pasta do script
script_dir = os.path.dirname(os.path.abspath(__file__))  # Diretório do script
csv_hos = os.path.join(script_dir, "clientes_hos.csv")
csv_rep = os.path.join(script_dir, "clientes_rep.csv")
csv_modulos = os.path.join(script_dir, "clientes_modulos.csv")

# Função para executar a consulta e gerar o CSV
def execute_query_and_save_to_csv(query, csv_filename):
    try:
        # Usando a mesma conexão que funcionou no teste
        con = fdb.connect(
            dsn="192.168.1.9/3050:crm_hos",
            user="SYSDBA",
            password="had63rg@"
        )

        # Configurar a transação como READ ONLY com nível de isolamento
        tpb = fdb.TPB()
        tpb.read_only = True  # Define a transação como somente leitura
        tpb.isolation_level = fdb.isc_tpb_read_committed  # Nível de isolamento correto
        con.begin(tpb=tpb)  # Inicia a transação com essas configurações

        cur = con.cursor()

        # Executar a consulta
        cur.execute(query)

        # Recuperar os resultados
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

        # Gerar o CSV
        with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(columns)  # Escrever cabeçalhos
            writer.writerows(rows)  # Escrever linhas

        print(f"Arquivo CSV gerado com sucesso: {csv_filename}")

    except fdb.DatabaseError as e:
        print(f"Erro ao acessar o banco de dados: {e}")

    finally:
        # Fechar conexão com o banco
        if 'con' in locals() and con:
            con.close()

# Função principal para executar o processo completo
def executar_processos():
    print("Iniciando o processo de geração dos CSVs...")
    execute_query_and_save_to_csv(query_hos, csv_hos)
    execute_query_and_save_to_csv(query_rep, csv_rep)
    execute_query_and_save_to_csv(query_modulos, csv_modulos)
    print("Processo concluído.")

# Agendar o script para rodar entre 08:00 e 18:00 a cada 2 horas
horarios = ["8:00", "10:00", "12:00", "14:00", "16:00", "18:00"]
for horario in horarios:
    schedule.every().day.at(horario).do(executar_processos)

print("Agendamento configurado. Aguardando horários para execução...")

# Loop para manter o script ativo e monitorar os agendamentos
while True:
    schedule.run_pending()
    time.sleep(1)