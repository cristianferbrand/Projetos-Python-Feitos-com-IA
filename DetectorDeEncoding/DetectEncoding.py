import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import chardet
import threading
import time

def detect_file_encoding(file_path, progress_bar, progress_label):
    """
    Detecta o encoding de um arquivo usando a biblioteca chardet.

    :param file_path: Caminho para o arquivo a ser analisado.
    :param progress_bar: Widget de barra de progresso para atualização.
    :param progress_label: Label para mostrar a porcentagem de progresso.
    """
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            total_size = len(raw_data)
            
            # Simulação de progresso para exibição
            chunks = 10
            chunk_size = total_size // chunks
            for i in range(chunks):
                time.sleep(0.2)  # Simula o processamento
                progress = int(((i + 1) / chunks) * 100)
                progress_bar["value"] = progress
                progress_label.config(text=f"Progresso: {progress}%")
            
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            
            progress_bar["value"] = 100
            progress_label.config(text="Progresso: 100%")
            
            messagebox.showinfo(
                "Resultado da Análise",
                f"Arquivo: {file_path}\nEncoding detectado: {encoding}\nConfiabilidade: {confidence * 100:.2f}%"
            )
    except FileNotFoundError:
        messagebox.showerror("Erro", "Arquivo não encontrado.")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao processar o arquivo: {e}")
    finally:
        progress_bar["value"] = 0
        progress_label.config(text="Pronto para análise")

def start_analysis(progress_bar, progress_label):
    """Inicia o processo de análise em uma thread separada."""
    file_path = filedialog.askopenfilename(title="Selecione um arquivo")
    if not file_path:
        return
    
    thread = threading.Thread(target=detect_file_encoding, args=(file_path, progress_bar, progress_label))
    thread.start()

# Configuração da interface gráfica
def create_gui():
    root = tk.Tk()
    root.title("Detector de Encoding")
    root.geometry("400x250")
    
    tk.Label(root, text="Detector de Encoding de Arquivos", font=("Arial", 14)).pack(pady=10)
    
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
    progress_bar.pack(pady=10)
    
    progress_label = tk.Label(root, text="Pronto para análise", font=("Arial", 10))
    progress_label.pack(pady=5)
    
    tk.Button(root, text="Selecionar Arquivo", command=lambda: start_analysis(progress_bar, progress_label), width=20, height=2).pack(pady=20)
    
    tk.Button(root, text="Sair", command=root.quit, width=10).pack(pady=10)
    
    root.mainloop()

if __name__ == "__main__":
    create_gui()