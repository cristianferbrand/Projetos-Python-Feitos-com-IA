def ajustar_cod_cta_c170_c175(arquivo_entrada, arquivo_saida):
    with open(arquivo_entrada, 'r', encoding='latin-1') as entrada, open(arquivo_saida, 'w', encoding='latin-1') as saida:
        for linha in entrada:
            campos = linha.strip().split('|')
            
            # Verifica se é um registro C170 e ajusta o campo COD_CTA
            if len(campos) > 1 and campos[1] == 'C170':
                # O campo COD_CTA é o penúltimo antes do fechamento '|'
                if len(campos) >= 36 and campos[-2] == '':
                    campos[-2] = '1'  # Ajusta o campo COD_CTA para '1'
            
            # Verifica se é um registro C175 e ajusta o campo COD_CTA
            elif len(campos) > 1 and campos[1] == 'C175':
                # O campo COD_CTA é o penúltimo antes do fechamento '|'
                if len(campos) >= 20 and campos[-3] == '':
                    campos[-3] = '1'  # Ajusta o campo COD_CTA para '1'
                
            # Reconstrói a linha ajustada
            linha_ajustada = '|'.join(campos)
            saida.write(linha_ajustada + '\n')

# Uso do código
arquivo_entrada = 'C:\\Users\\crist\\OneDrive\\Desktop\\39050 - Pharma Vida Matriz - WA\\SpedContri.txt'
arquivo_saida = 'C:\\Users\\crist\\OneDrive\\Desktop\\39050 - Pharma Vida Matriz - WA\\sped_efd_contribuicoes_ajustado.txt'

ajustar_cod_cta_c170_c175(arquivo_entrada, arquivo_saida)