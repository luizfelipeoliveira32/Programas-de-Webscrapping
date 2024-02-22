import requests
from bs4 import BeautifulSoup
import pandas as pd

'''URL do site para acesso:
    url = "https://csdbr.com/dados-de-mercado/?de=07-02-2024&ate=07-02-2024&volume_operacoes=Estoque"
'''
def coleta_estoque_csdbr(data_referencia):
    print('Inicio da coleta.')
    # Converter a string de data para um objeto datetime e depois formata a data de referência no formato necessário para a URL    
    data_referencia = pd.to_datetime(data_referencia)
    data_formatada = data_referencia.strftime("%d-%m-%Y")
    
    # Cabeçalho de agente do usuário para simular um navegador
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    
    url = f"https://csdbr.com/dados-de-mercado/?de={data_formatada}&ate={data_formatada}&volume_operacoes=Estoque"
    
    # Faz a requisição GET para obter o conteúdo da página
    response = requests.get(url, headers=headers)
    
    # Verifica se a requisição foi bem sucedida
    if response.status_code == 200:
        # Parseia o conteúdo HTML da página
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Encontra a dados_coletados de dados
        table = soup.find('table')
        
        # Inicializa uma lista para armazenar os dados
        data = []
        
        # Verifica se a dados_coletados foi encontrada
        if table:
            # Itera sobre as linhas da dados_coletados, ignorando o cabeçalho
            for row in table.find_all('tr')[1:]:
                # Extrai os dados de cada coluna da linha
                columns = row.find_all('td')
                date = columns[0].get_text(strip=True)
                asset_type = columns[1].get_text(strip=True)
                
                # Remove os pontos de separação de milhares e converte para float
                quantity = float(columns[2].get_text(strip=True).replace('.', ''))
                volume = float(columns[3].get_text(strip=True).replace('.', ''))
                
                # Adiciona os dados à lista
                data.append({
                    'Data': date,
                    'Tipo de Ativo': asset_type,
                    'Quantidade de Contratos': quantity,
                    'Volume Financeiro R$': volume
                })
        print('Final da coleta.')        
        return pd.DataFrame(data)
    else:
        print("Falha ao obter a página:", response.status_code)
        return None


# Chamada da função e impressão dos resultados
data_referencia = input('Digite a data de coleta: ')
coleta = coleta_estoque_csdbr(data_referencia)
coleta.to_excel(f'{data_referencia} EstoqueCSDBR.xlsx', index=False)
