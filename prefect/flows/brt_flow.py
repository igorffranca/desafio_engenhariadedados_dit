import os
from datetime import datetime, timedelta
import requests
import pandas as pd
from prefect import Flow, task, Parameter
from prefect.schedules import IntervalSchedule
from sqlalchemy import create_engine, types
from dotenv import load_dotenv

# Carrega variável de ambiente
load_dotenv()


@task
def extrai_dados_api():
    """Extrai os dados da api"""
    url = "https://dados.mobilidade.rio/gps/brt"
    resposta = requests.get(url)
    resposta.raise_for_status()
    return resposta.json()


@task
def processa_dados(data):
    """Transforma os dados do JSON em dataframe pandas"""

    if isinstance(data, dict) and 'veiculos' in data:
        veiculos = data['veiculos']
    elif isinstance(data, list):
        veiculos = [item['veiculos'] for item in data if 'veiculos' in item]
    else:
        veiculos = data

    # Converte em dataframe pandas
    df = pd.DataFrame(veiculos)

    # Converte dataHora em datatime
    df['dataHora'] = pd.to_datetime(df['dataHora'], unit='ms')

    # Adiciona uma coluna de atualização que vai ser usada na carga incremental
    df['updated_at'] = datetime.now()

    # Tratamento para linhas que vem em branco, na coluna de direção, se vier em branco, preenche com 0
    df['direcao'] = pd.to_numeric(df['direcao'].replace(' ', '0'), errors='coerce')

    # Tratamento nas colunas latitude, longitude, velocidade, hodometro e direcao para serem numéricas
    colunas_numericas = ['latitude', 'longitude', 'velocidade', 'hodometro', 'direcao']
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


@task
def salva_csv(df, timestamp):
    """Salva os dados em um CSV"""
    arquivo = f"data/raw/brt_data_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs(os.path.dirname(arquivo), exist_ok=True)
    df.to_csv(arquivo, index=False)
    return arquivo


@task
def carrega_dados(df, nome_tabela):
    """Carrega dados no PostgreSQL"""
    database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/brt_data')
    engine = create_engine(database_url)

    # Define SQLAlchemy types for each column
    dtype_map = {
        'codigo': types.String(50),
        'placa': types.String(20),
        'linha': types.String(10),
        'latitude': types.Float(),
        'longitude': types.Float(),
        'dataHora': types.DateTime(),
        'velocidade': types.Float(),
        'id_migracao_trajeto': types.String(100),
        'sentido': types.String(10),
        'trajeto': types.String(200),
        'hodometro': types.Float(),
        'direcao': types.Float(),
        'ignicao': types.Integer(),
        'updated_at': types.DateTime()
    }

    # Create table and load data
    df.to_sql(
        nome_tabela,
        engine,
        if_exists='append',
        index=False,
        dtype=dtype_map,
        method='multi'
    )


def cria_fluxo():
    schedule = IntervalSchedule(interval=timedelta(seconds=60))  # Run every 60 seconds

    with Flow("brt-data-pipeline", schedule=schedule) as flow:
        # Parameters
        nome_tabela = Parameter("brt_data", default="raw_brt_data")

        # Execute pipeline
        raw_data = extrai_dados_api()
        processed_data = processa_dados(raw_data)
        data_atual = datetime.now()
        csv_file = salva_csv(processed_data, data_atual)
        carrega_dados(processed_data, nome_tabela)

    return flow


if __name__ == "__main__":
    flow = cria_fluxo()
    flow.run()