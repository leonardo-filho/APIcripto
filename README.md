Projeto de Automação de Coleta de Dados de Criptomoedas
Este repositório contém um projeto de automação para coleta de dados históricos de criptomoedas usando a API do CoinGecko e armazenamento em um banco de dados SQLite. O projeto inclui scripts em Python para automação e agendamento de execução, integração com Prefect e SQL para análise de dados.

Descrição
O objetivo deste projeto é automatizar a coleta e armazenamento de dados históricos das seguintes criptomoedas:

Bitcoin (bitcoin)
Ethereum (ethereum)
Solana (solana)
Os dados coletados incluem:

Preço atual (current_price_usd, current_price_eur, current_price_brl)
Market Cap (market_cap_usd, market_cap_eur, market_cap_brl)
Volume Total (total_volume_usd, total_volume_eur, total_volume_brl)
Os dados são coletados para um intervalo de tempo específico e armazenados no formato CSV e também em um banco de dados SQLite para análise posterior.
