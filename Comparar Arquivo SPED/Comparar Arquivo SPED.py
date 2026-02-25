from difflib import unified_diff

def compare_files(file1_path, file2_path):
    # Abrindo os arquivos com codificação 'latin-1' (ou ajuste se necessário)
    with open(file1_path, 'r', encoding='latin-1') as file1, open(file2_path, 'r', encoding='latin-1') as file2:
        file1_lines = file1.readlines()
        file2_lines = file2.readlines()

    # Computar as diferenças entre os dois arquivos
    diff = unified_diff(file1_lines, file2_lines, fromfile='Arquivo Correto', tofile='Arquivo Errado', lineterm='')

    # Exibir as diferenças
    for line in diff:
        print(line)

# Caminhos dos arquivos
file1_path = 'C:\\Users\\crist\\OneDrive\\Desktop\\SpedFiscal_Certo.txt'
file2_path = 'C:\\Users\\crist\\OneDrive\\Desktop\\SpedFiscal_Errado.txt'

# Executar a comparação
compare_files(file1_path, file2_path)
