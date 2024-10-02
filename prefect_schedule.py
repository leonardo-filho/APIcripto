from prefect import task, flow
from prefect.deployments import Deployment
from prefect.client.schemas.schedules import CronSchedule  # Use o módulo correto para agendamento
import requests
import pandas as pd
from time import sleep
import sqlite3
from datetime import datetime, timedelta

# Definir constantes
base_url = "https://api.coingecko.com/api/v3/coins/{coin}/history?x_cg_demo_api_key=CG-DRb45giJyEopije47dp1eo5V"
headers = {"accept": "application/json"}
coins = ["bitcoin", "ethereum", "solana"]
file_path = r"C:\Users\leron\OneDrive\Área de Trabalho\ec10\gold_data\cryptocurrency_data.csv"
sqlite_db_path = r"C:\Users\leron\OneDrive\Área de Trabalho\ec10\gold_data\cryptocurrency_data.db"
table_name = "cryptocurrency_data"

@task
def fetch_data_for_date(coin, date_str):
    """Função para buscar dados para uma moeda e data específica."""
    url = base_url.format(coin=coin)
    params = {"date": date_str, "localization": False}
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if "market_data" in data:
            extracted_data = {
                'data': date_str,
                'coin': coin,
                'location': data.get('localization', {}).get('pt', 'N/A'),
                'current_price_usd': data['market_data']['current_price'].get('usd', None),
                'current_price_eur': data['market_data']['current_price'].get('eur', None),
                'current_price_brl': data['market_data']['current_price'].get('brl', None),
                'market_cap_usd': data['market_data']['market_cap'].get('usd', None),
                'market_cap_eur': data['market_data']['market_cap'].get('eur', None),
                'market_cap_brl': data['market_data']['market_cap'].get('brl', None),
                'total_volume_usd': data['market_data']['total_volume'].get('usd', None),
                'total_volume_eur': data['market_data']['total_volume'].get('eur', None),
                'total_volume_brl': data['market_data']['total_volume'].get('brl', None)
            }
            return extracted_data
    elif response.status_code == 429:
        print(f"Erro ao buscar dados para {coin} em {date_str}: {response.status_code} (Limite de requisições excedido). Aguardando...")
        sleep(15)
        return fetch_data_for_date(coin, date_str)
    else:
        print(f"Erro ao buscar dados para {coin} em {date_str}: {response.status_code}")
    return None

@task
def collect_data(start_date_str, end_date_str):
    """Função para coletar dados de criptomoedas no intervalo de datas especificado."""
    all_data = []
    start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%Y")
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%d-%m-%Y")
        print(f"Buscando dados para {date_str}...")

        data_found = False
        for coin in coins:
            data = fetch_data_for_date(coin, date_str)
            if data:
                all_data.append(data)
                data_found = True

            sleep(5)

        if not data_found:
            print(f"Sem dados disponíveis para {date_str}. Parando a execução.")
            break

        current_date += timedelta(days=1)

    return all_data

@task
def save_to_csv(data, path):
    """Salvar os dados coletados em um arquivo CSV."""
    df = pd.DataFrame(data)
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"Arquivo salvo com sucesso em: {path}")

@task
def save_to_sqlite(data, db_path, table):
    """Salvar os dados coletados em um banco de dados SQLite."""
    df = pd.DataFrame(data)
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Dados salvos com sucesso na tabela '{table}' no banco de dados '{db_path}'.")

@task
def format_data(data):
    """Aplicar formatação de colunas financeiras para duas casas decimais."""
    df = pd.DataFrame(data)
    df['current_price_usd'] = df['current_price_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
    df['current_price_eur'] = df['current_price_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
    df['current_price_brl'] = df['current_price_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)
    df['market_cap_usd'] = df['market_cap_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
    df['market_cap_eur'] = df['market_cap_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
    df['market_cap_brl'] = df['market_cap_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)
    df['total_volume_usd'] = df['total_volume_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
    df['total_volume_eur'] = df['total_volume_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
    df['total_volume_brl'] = df['total_volume_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)
    return df

@flow
def crypto_pipeline(start_date: str = "15-09-2024", end_date: str = "15-10-2024"):
    """Fluxo principal do pipeline."""
    raw_data = collect_data(start_date, end_date)
    formatted_data = format_data(raw_data)
    save_to_csv(formatted_data, file_path)
    save_to_sqlite(formatted_data, sqlite_db_path, table_name)

# Configuração do agendamento usando a lista schedules
deployment = Deployment.build_from_flow(
    flow=crypto_pipeline,
    name="Daily Crypto Pipeline",
    schedules=[CronSchedule(cron="0 7 * * *")],  # Executa todo dia às 7am usando a lista schedules
)

if __name__ == "__main__":
    deployment.apply()


#SELECT 
#    coin, 
#    AVG(CAST(REPLACE(REPLACE(REPLACE(current_price_usd, '$', ''), ',', ''), ' ', '') AS REAL)) AS avg_price_usd,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(current_price_eur, '€', ''), ',', ''), ' ', '') AS REAL)) AS avg_price_eur,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(current_price_brl, 'R$', ''), ',', ''), ' ', '') AS REAL)) AS avg_price_brl,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(market_cap_usd, '$', ''), ',', ''), ' ', '') AS REAL)) AS avg_market_cap_usd,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(market_cap_eur, '€', ''), ',', ''), ' ', '') AS REAL)) AS avg_market_cap_eur,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(market_cap_brl, 'R$', ''), ',', ''), ' ', '') AS REAL)) AS avg_market_cap_brl,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(total_volume_usd, '$', ''), ',', ''), ' ', '') AS REAL)) AS avg_volume_usd,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(total_volume_eur, '€', ''), ',', ''), ' ', '') AS REAL)) AS avg_volume_eur,
#    AVG(CAST(REPLACE(REPLACE(REPLACE(total_volume_brl, 'R$', ''), ',', ''), ' ', '') AS REAL)) AS avg_volume_brl
#FROM cryptocurrency_data
#GROUP BY coin;

