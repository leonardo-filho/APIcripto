import requests
import pandas as pd
from time import sleep
import sqlite3

# Definir a URL base para a API
base_url = "https://api.coingecko.com/api/v3/coins/{coin}/history?x_cg_demo_api_key=CG-DRb45giJyEopije47dp1eo5V"

# Definir os parâmetros fixos para a solicitação
headers = {"accept": "application/json"}

# Lista para armazenar os dados coletados
all_data = []

# Moedas que desejamos coletar os dados
coins = ["bitcoin", "ethereum", "solana"]

# Função para realizar a requisição com tratamento de erros
def fetch_data_for_date(coin, date_str):
    url = base_url.format(coin=coin)
    params = {"date": date_str, "localization": False}
    response = requests.get(url, headers=headers, params=params)
    
    # Verificar se a resposta foi bem sucedida
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
        sleep(15)  # Aguardar 15 segundos e tentar novamente
        return fetch_data_for_date(coin, date_str)
    else:
        print(f"Erro ao buscar dados para {coin} em {date_str}: {response.status_code}")
    return None

# Loop por cada dia de agosto de 2024 e cada moeda
for day in range(1, 32):  # Dias de 1 a 31
    date_str = f"{day:02d}-09-2024"
    
    # Fazer a requisição para cada moeda
    for coin in coins:
        # Tentar buscar os dados para a data e moeda específica
        data = fetch_data_for_date(coin, date_str)
        if data:
            all_data.append(data)
        
        # Esperar alguns segundos para evitar limites da API
        sleep(5)  # Aguardar 5 segundos entre cada requisição para evitar limite de taxa

# Criar um DataFrame com todos os dados coletados
df = pd.DataFrame(all_data)
print(df.head())

# Definir o caminho para salvar o arquivo CSV
file_path = r"C:\Users\leron\OneDrive\Área de Trabalho\ec10\gold_data\cryptocurrency_data.csv"

# Salvar o DataFrame como CSV
df.to_csv(file_path, index=False, encoding='utf-8')

print(f"Arquivo salvo com sucesso em: {file_path}")

# Limitar as colunas financeiras a duas casas decimais e adicionar o símbolo correspondente
df['current_price_usd'] = df['current_price_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
df['current_price_eur'] = df['current_price_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
df['current_price_brl'] = df['current_price_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)

df['market_cap_usd'] = df['market_cap_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
df['market_cap_eur'] = df['market_cap_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
df['market_cap_brl'] = df['market_cap_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)

df['total_volume_usd'] = df['total_volume_usd'].apply(lambda x: f"${x:.2f}" if pd.notnull(x) else x)
df['total_volume_eur'] = df['total_volume_eur'].apply(lambda x: f"€{x:.2f}" if pd.notnull(x) else x)
df['total_volume_brl'] = df['total_volume_brl'].apply(lambda x: f"R${x:.2f}" if pd.notnull(x) else x)

# Nome do arquivo SQLite onde os dados serão salvos
sqlite_db_path = r"C:\Users\leron\OneDrive\Área de Trabalho\ec10\gold_data\cryptocurrency_data.db"

# Nome da tabela no banco de dados SQLite
table_name = "cryptocurrency_data"

# Conectar ao banco de dados SQLite (ou criar se não existir)
conn = sqlite3.connect(sqlite_db_path)

# Salvar o DataFrame como uma tabela no SQLite
df.to_sql(table_name, conn, if_exists='replace', index=False)

# Fechar a conexão com o banco de dados
conn.close()

print(f"Dados salvos com sucesso na tabela '{table_name}' no banco de dados '{sqlite_db_path}'.")