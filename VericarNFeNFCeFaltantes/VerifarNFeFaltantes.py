import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import fdb
import threading

# Função para verificar números faltantes no banco de dados Firebird
def verificar_numeros_faltantes():
    def processar():
        try:
            # Obtém os valores dos campos
            periodo_inicio = entry_inicio.get()
            periodo_fim = entry_fim.get()
            serie = entry_serie.get()
            empresa = entry_empresa.get()
            caminho_banco = entry_banco.get()

            if not caminho_banco:
                messagebox.showerror("Erro", "Por favor, selecione o caminho do banco de dados.")
                return

            # Conectar ao banco Firebird
            con = fdb.connect(dsn=caminho_banco, user='SYSDBA', password='masterkey')
            cur = con.cursor()
            con.begin()

            progress_bar['value'] = 10
            root.update_idletasks()

            # Buscar a numeração mínima e máxima
            cur.execute(f"""
                SELECT MIN(NR_NOTA), MAX(NR_NOTA)
                FROM CAB_NF
                WHERE DATA_EMISSAO BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
                  AND SERIE = {serie}
                  AND EMPRESA = {empresa}
            """
            )
            min_max = cur.fetchone()
            if not min_max or not min_max[0]:
                messagebox.showinfo("Resultado", "Nenhuma nota encontrada no período informado.")
                return

            min_nota, max_nota = map(int, min_max)

            progress_bar['value'] = 30
            root.update_idletasks()

            # Buscar números emitidos
            cur.execute(f"""
                SELECT NR_NOTA FROM CAB_NF
                WHERE DATA_EMISSAO BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
                  AND SERIE = {serie}
                  AND EMPRESA = {empresa}
            """
            )
            notas_emitidas = {int(row[0]) for row in cur.fetchall()}

            progress_bar['value'] = 50
            root.update_idletasks()

            # Buscar números inutilizados
            cur.execute(f"""
                SELECT NUMERACAO_INICIAL, NUMERACAO_FINAL FROM NFE_INUTILIZACAO_NUMERACAO
                WHERE EMPRESA = {empresa} AND MODELO = 55
            """
            )
            inutilizados = set()
            for inicio, fim in cur.fetchall():
                inutilizados.update(range(int(inicio), int(fim) + 1))

            progress_bar['value'] = 70
            root.update_idletasks()

            # Buscar números emitidos em outra data
            cur.execute(f"""
                SELECT NR_NOTA, DATA_EMISSAO FROM CAB_NF
                WHERE NR_NOTA BETWEEN {min_nota} AND {max_nota}
                  AND SERIE = {serie}
                  AND EMPRESA = {empresa}
                  AND DATA_EMISSAO NOT BETWEEN '{periodo_inicio}' AND '{periodo_fim}'
            """
            )
            notas_outra_data = {int(row[0]): row[1] for row in cur.fetchall()}

            numeros_faltantes = []
            detalhes_faltantes = []
            for num in range(min_nota, max_nota + 1):
                if num not in notas_emitidas and num not in inutilizados:
                    if num in notas_outra_data:
                        detalhes_faltantes.append(f"Número {num} emitido em outra data: {notas_outra_data[num]}")
                    else:
                        numeros_faltantes.append(num)

            con.commit()
            con.close()

            progress_bar['value'] = 100
            root.update_idletasks()

            mostrar_tabela(numeros_faltantes, detalhes_faltantes)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao consultar o banco de dados: {e}")
        finally:
            progress_bar['value'] = 0

    threading.Thread(target=processar).start()

# Função para selecionar banco de dados
def selecionar_banco():
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos Firebird", "*.FDB")])
    if caminho:
        entry_banco.delete(0, tk.END)
        entry_banco.insert(0, caminho)

# Função para exibir tabela com números faltantes
def mostrar_tabela(numeros_faltantes, detalhes_faltantes):
    janela_tabela = tk.Toplevel(root)
    janela_tabela.title("Números Faltantes")
    janela_tabela.geometry("500x400")

    frame = tk.Frame(janela_tabela)
    frame.pack(fill=tk.BOTH, expand=True)

    scrollbar = tk.Scrollbar(frame)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    text_area = tk.Text(frame, wrap=tk.WORD, width=50, height=20, yscrollcommand=scrollbar.set)
    text_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    scrollbar.config(command=text_area.yview)

    if numeros_faltantes or detalhes_faltantes:
        text_area.insert(tk.END, f"Números Faltantes ({len(numeros_faltantes)} registros):\n" + "\n".join(map(str, numeros_faltantes)) + "\n\n")
        text_area.insert(tk.END, "Detalhes de números emitidos em outra data:\n" + "\n".join(detalhes_faltantes))
    else:
        text_area.insert(tk.END, "Nenhum número faltante encontrado.")

# Criar interface gráfica
root = tk.Tk()
root.title("Verificar Números Faltantes")
root.geometry("450x350")

# Criar campos de entrada
tk.Label(root, text="Período Início (YYYY-MM-DD):").pack()
entry_inicio = tk.Entry(root)
entry_inicio.pack()

tk.Label(root, text="Período Fim (YYYY-MM-DD):").pack()
entry_fim = tk.Entry(root)
entry_fim.pack()

tk.Label(root, text="Série:").pack()
entry_serie = tk.Entry(root)
entry_serie.pack()

tk.Label(root, text="Empresa:").pack()
entry_empresa = tk.Entry(root)
entry_empresa.pack()

tk.Label(root, text="Banco de Dados Firebird:").pack()
entry_banco = tk.Entry(root)
entry_banco.insert(0, "C:/Mercfarma/CADASTRO.FDB")
entry_banco.pack()
tk.Button(root, text="Selecionar Banco", command=selecionar_banco).pack()

progress_bar = ttk.Progressbar(root, orient=tk.HORIZONTAL, length=300, mode='determinate')
progress_bar.pack(pady=10)

tk.Button(root, text="Verificar", command=verificar_numeros_faltantes).pack()

root.mainloop()