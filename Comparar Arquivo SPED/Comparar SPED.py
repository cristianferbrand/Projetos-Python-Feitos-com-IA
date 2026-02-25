import difflib
import tkinter as tk
from tkinter import scrolledtext, filedialog, ttk
import threading

def comparar_arquivos():
    def processar_comparacao():
        arquivo1 = filedialog.askopenfilename(title="Selecione o primeiro arquivo TXT")
        arquivo2 = filedialog.askopenfilename(title="Selecione o segundo arquivo TXT")
        
        if not arquivo1 or not arquivo2:
            return
        
        with open(arquivo1, 'r', encoding='latin-1') as file1, open(arquivo2, 'r', encoding='latin-1') as file2:
            linhas1 = file1.readlines()
            linhas2 = file2.readlines()
        
        resultado.delete(1.0, tk.END)
        progress_bar.start()
        
        resultado.insert(tk.END, f"{'Campo':<10}{'Arquivo 1':<40}{'Arquivo 2':<40}\n", "header")
        resultado.insert(tk.END, "=" * 90 + "\n", "header")
        
        for linha1, linha2 in zip(linhas1, linhas2):
            if linha1 != linha2:
                campos1 = linha1.strip().split("|")
                campos2 = linha2.strip().split("|")
                
                for i, (campo1, campo2) in enumerate(zip(campos1, campos2)):
                    if campo1 != campo2:
                        resultado.insert(tk.END, f"{i:<10}{campo1:<40}{campo2:<40}\n", "diff")
        
        resultado.tag_config("diff", foreground="red")
        resultado.tag_config("header", foreground="blue", font=("Courier", 10, "bold"))
        progress_bar.stop()
    
    thread = threading.Thread(target=processar_comparacao)
    thread.start()

def criar_interface():
    global resultado, progress_bar
    
    root = tk.Tk()
    root.title("Comparação de Arquivos SPED")
    root.geometry("900x600")
    
    btn_comparar = tk.Button(root, text="Selecionar e Comparar Arquivos", command=comparar_arquivos)
    btn_comparar.pack(pady=10)
    
    progress_bar = ttk.Progressbar(root, mode='indeterminate')
    progress_bar.pack(fill=tk.X, padx=10, pady=5)
    
    frame_texto = tk.Frame(root)
    frame_texto.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
    
    scrollbar = tk.Scrollbar(frame_texto)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    
    resultado = tk.Text(frame_texto, wrap=tk.WORD, yscrollcommand=scrollbar.set, width=120, height=30)
    resultado.pack(fill=tk.BOTH, expand=True)
    
    scrollbar.config(command=resultado.yview)
    
    root.mainloop()

if __name__ == "__main__":
    criar_interface()