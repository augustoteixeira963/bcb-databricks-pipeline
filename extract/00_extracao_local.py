import requests
import json
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_sessao_resiliente() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def baixar_dados(url: str, nome_arquivo: str):
    session = get_sessao_resiliente()
    
    # Adicionamos headers para burlar o bloqueio antibot do BCB
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    try:
        # Passamos os headers na requisição
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        dados = response.json()
        
        # Data Quality 1: Falha explícita se payload vazio
        if not dados:
            raise ValueError(f"Payload vazio retornado da URL: {url}")
            
        caminho = Path(__file__).parent / nome_arquivo
        with open(caminho, 'w', encoding='utf-8') as f:
            json.dump(dados, f, indent=2)
        logging.info(f"Sucesso: {nome_arquivo} salvo com {len(dados)} registros.")
        
    except Exception as e:
        logging.error(f"Erro ao processar {nome_arquivo}: {e}")
        raise

if __name__ == "__main__":
    url_selic = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
    url_ipca = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json&dataInicial=01/01/2020&dataFinal=31/12/2024"
    
    baixar_dados(url_selic, "selic.json")
    baixar_dados(url_ipca, "ipca.json")