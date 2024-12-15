import requests
from dotenv import load_dotenv
import os
import pandas as pd
import time
import gspread
from google.oauth2.service_account import Credentials


load_dotenv()
TOKEN = os.getenv('token')

def autenticar_google_sheets(json_key_file):
    """Função para autenticar ao google sheets, o json_key_file é o arquivo obtidido no Console da Google"""
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_file(json_key_file, scopes=scopes)
    return gspread.authorize(credentials)

def construir_url(endpoint, parametros=None, pagina=1):
    """Função para construir a url que será utilizada para retornar os dados da Api Tiny"""
    url = f"https://api.tiny.com.br/api2/{endpoint}.php?token={TOKEN}&formato=JSON&pagina={pagina}"
    return f"{url}&{parametros}" if parametros else url

def requisicao_api(url, contador_requisicoes):
    """Função para fazer a requisição na API, como ela tem limite de 30 registros por minuto, estamos limitando para não ultrapassar"""
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    if contador_requisicoes >= 30:
        time.sleep(60)
        contador_requisicoes = 0
    return response.json(), contador_requisicoes + 1

def buscar_endpoint_paginada(endpoint, nome_endpoint=None):
    url_inicial = construir_url(endpoint)
    dados_iniciais, contador_requisicoes = requisicao_api(url_inicial, 0)
    numero_paginas = int(dados_iniciais['retorno']['numero_paginas'])
    todos_dados = []

    for pagina in range(1, numero_paginas + 1):
        url_pagina = construir_url(endpoint, pagina=pagina)
        dados_pagina, contador_requisicoes = requisicao_api(url_pagina, contador_requisicoes)
        todos_dados.extend(coluna[nome_endpoint.replace('s','')] for coluna in dados_pagina['retorno'][nome_endpoint])

    return pd.DataFrame(todos_dados)

def main():
    produtos_df = buscar_endpoint_paginada('produtos.pesquisa', 'produtos')
    print(produtos_df)

if __name__ == "__main__":
    main()
