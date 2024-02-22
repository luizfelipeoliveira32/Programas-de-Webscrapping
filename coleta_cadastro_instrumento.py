import requests
import pandas as pd
import concurrent.futures
from pandas.tseries.offsets import BDay
import time

'''Site de consulta original: 
https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/boletim-diario-do-mercado/ 
'''


def coleta(data_referencia):
    url = "https://arquivos.b3.com.br/bdi/table/InstrumentRegistration/{date}/{date}/{num}/1000"
    tempo_inicial = time.time()

    def process_page(date, num):
        print(f'Getting page {num}')
        response = requests.post(url.format(
            num=num, date=date.strftime('%Y-%m-%d')), json={})

        # Verifica se a resposta foi bem-sucedida e se contém dados
        if response.status_code == 200 and response.text:
            json_data = response.json()

            if not json_data['table']['values']:
                return None

            values = [x[:-1] for x in json_data['table']['values']]
            return values
        else:
            print(
                f'Failed to get page {num}. Status code: {response.status_code}')
            return None

    entity_values = []

    for date in pd.date_range(data_referencia, data_referencia, freq=BDay()):
        print(f'Processando o dia: {date.strftime("%Y-%m-%d")}')
        first_response = requests.post(url.format(
            num=1, date=date.strftime('%Y-%m-%d')), json={})
        first_response_json = first_response.json()
        num_pages = first_response_json['table']['pageCount']
        columns = [column['friendlyName']
                   for column in first_response_json['table']['columns']]
        page_numbers = range(1, num_pages + 1)

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(process_page, date, num)
                       for num in page_numbers]

        for future in concurrent.futures.as_completed(futures):
            values = future.result()
            if values:
                entity_values.extend(values)

    entity_dataframe = pd.DataFrame(entity_values, columns=columns)
    # Convertendo valores numéricos para float e mantendo os não numéricos como string
    entity_dataframe['Número de Série'] = pd.to_numeric(entity_dataframe['Número de Série'], errors='coerce').fillna(entity_dataframe['Número de Série'])

    tempo_final = time.time()
    total_tempo = tempo_final - tempo_inicial
    print(
        f'Coleta de dados para a data {data_referencia} concluida e levou {total_tempo}s.')
    return entity_dataframe


# Parâmetro de entrada da função
data_referencia = input('Digite a data de coleta: ')

df_resultados = coleta(data_referencia)
# Exportação dos resultados para um arquivo excel
df_resultados.to_excel(f'{data_referencia} CadastroInstrumento.xlsx',
                       index=False, sheet_name='DadosColetados')
