import pandas as pd

# Leitura dos arquivos em formato CSV
arquivo1 = pd.read_csv('C:\\Users\\crist\\OneDrive\\Desktop\\Python\\Comparar SEFAZ VS HOS\\ExtratoNFe.csv', delimiter=';', decimal=',')
arquivo2 = pd.read_csv('C:\\Users\\crist\\OneDrive\\Desktop\\Python\\Comparar SEFAZ VS HOS\\ExtratoHOS.csv', delimiter=';', decimal=',')

# Renomeando as colunas do arquivo 2 para que fiquem consistentes
arquivo2.rename(columns={'Numero': 'Numero', 'Total_NF-e': 'Total_NF-e'}, inplace=True)

# Removendo possíveis espaços em branco nas colunas 'Numero'
arquivo1['Numero'] = arquivo1['Numero'].astype(str).str.strip()
arquivo2['Numero'] = arquivo2['Numero'].astype(str).str.strip()

# Convertendo as colunas 'Numero' e 'Total_NF-e' para numéricas, garantindo que a conversão seja consistente com o formato brasileiro
arquivo1['Numero'] = pd.to_numeric(arquivo1['Numero'], errors='coerce')
arquivo2['Numero'] = pd.to_numeric(arquivo2['Numero'], errors='coerce')

# Convertendo 'Total_NF-e' para numérico com suporte a valores com vírgula
arquivo1['Total_NF-e'] = arquivo1['Total_NF-e'].replace(',', '.', regex=True).astype(float)
arquivo2['Total_NF-e'] = arquivo2['Total_NF-e'].replace(',', '.', regex=True).astype(float)

# Fazendo a junção dos dois arquivos com base no campo 'Numero'
df_comparacao = pd.merge(arquivo1[['Numero', 'Total_NF-e']], arquivo2[['Numero', 'Total_NF-e']],
                         on='Numero', suffixes=('_arquivo1', '_arquivo2'))

# Comparando os valores de 'Total_NF-e' entre os dois arquivos
df_diferencas = df_comparacao[df_comparacao['Total_NF-e_arquivo1'] != df_comparacao['Total_NF-e_arquivo2']]

# Salvando as notas que possuem divergências no valor 'Total_NF-e' em um novo arquivo CSV
df_diferencas.to_csv('C:\\Users\\crist\\OneDrive\\Desktop\\Python\\Comparar SEFAZ VS HOS\\Divergencias_NFe.csv', sep=';', decimal=',', index=False)

# Exibindo as primeiras linhas das divergências no console
print(df_diferencas.head())