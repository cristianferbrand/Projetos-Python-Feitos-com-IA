import requests
from bs4 import BeautifulSoup

# URL da página de Notas Técnicas
url = "https://www.nfe.fazenda.gov.br/portal/listaConteudo.aspx?tipoConteudo=04BIflQt1aY="

# Faz a requisição para a página
response = requests.get(url)

# Parseia o conteúdo HTML com BeautifulSoup
soup = BeautifulSoup(response.content, "html.parser")

# Encontra os títulos e links das Notas Técnicas
notes = soup.find_all('a', href=True)

# Lista os títulos, links das Notas Técnicas e conteúdo extra
for note in notes:
    title_span = note.find_next('span', class_='tituloConteudo')
    if title_span:
        title = title_span.get_text()
        link = note['href']
        full_link = f"https://www.nfe.fazenda.gov.br/portal/{link}"  # Concatena o domínio
        
        # Tenta encontrar o conteúdo extra após o <br> e trata possíveis erros
        try:
            br_tag = title_span.find_next('br')
            extra_content = br_tag.next_sibling.strip() if br_tag and br_tag.next_sibling else "Sem descrição adicional"
        except Exception as e:
            extra_content = "Erro ao buscar a descrição adicional"
        
        print(f"Nota Técnica: {title} - Link: {full_link} - Descrição: {extra_content}")