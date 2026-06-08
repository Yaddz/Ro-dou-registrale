import os
import re
import yaml
import json
import logging
import requests
import math
import glob
import copy
from typing import Set, Optional, Dict, List
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Configurando LOG
logging.basicConfig(level=logging.INFO)

DATA_DIR = "data"
METADATA_FILE = os.path.join(DATA_DIR, "monitored_companies.json")

# Extração de dados e validação!
def extrair_cnpj(cnpj_bruto: str) -> Optional[str]:
    """Limpa e valida o formato básico de um CNPJ."""
    if not cnpj_bruto:
        return None
    return str(cnpj_bruto).strip()

# Comunicação com a API
def get_monitored_data(url_base: str, endpoint: str, headers: dict) -> List[Dict]:
    """Busca dados completos dos clientes na API GestãoClick."""
    clientes_completos = []
    pagina_atual = 1
    url_completa = f"{url_base.rstrip('/')}/{endpoint.lstrip('/')}"
    
    while True:
        logging.info(f"Buscando {url_completa} - Página {pagina_atual}")
        
        try:
            resposta = requests.get(url_completa, params={"pagina": pagina_atual}, headers=headers, timeout=30)
            if resposta.status_code == 404: break
            resposta.raise_for_status()
            
            dados_json = resposta.json()
            itens = dados_json.get("data", [])
            
            if not itens: break
            
            for item in itens:
                cnpj = item.get("cnpj")
                if cnpj:
                    # Extração segura de endereço
                    endereco = {}
                    if item.get("enderecos") and len(item.get("enderecos")) > 0:
                        endereco = item.get("enderecos")[0].get("endereco", {})

                    clientes_completos.append({
                        "nome": item.get("razao_social") or item.get("nome") or "N/A",
                        "cnpj": str(cnpj).strip(),
                        "uf": endereco.get("estado") or "N/A",
                        "cidade": endereco.get("nome_cidade") or "N/A",
                        "email": item.get("email") or "N/A",
                        "telefone": item.get("telefone") or item.get("celular") or "N/A",
                        "situacao": "Ativa" if str(item.get("ativo")) == "1" else "Inativa"
                    })
                    
            proxima_pagina = dados_json.get("meta", {}).get("proxima_pagina")
            if not proxima_pagina or int(proxima_pagina) <= pagina_atual: break
            pagina_atual = int(proxima_pagina)

        except Exception as erro:
            logging.error(f"Erro na página {pagina_atual}: {erro}")
            break
        
    return clientes_completos

# Validar e atualizar o YAML
def atualizar_configuracoes(caminho_arquivo: str, clientes: List[Dict]):
    """Atualiza os arquivos YAML de busca e salva o metadado central."""
    
    if not caminho_arquivo:
        logging.error("Caminho do arquivo YAML não fornecido.")
        return

    # Tenta localizar o arquivo se o caminho absoluto falhar (ex: rodando fora do Docker)
    if not os.path.exists(caminho_arquivo):
        logging.warning(f"Caminho original não encontrado: {caminho_arquivo}")
        nome_arquivo = os.path.basename(caminho_arquivo)
        tentativas = [
            os.path.join("dag_confs", nome_arquivo),
            os.path.join("..", "dag_confs", nome_arquivo),
            nome_arquivo
        ]
        for t in tentativas:
            if os.path.exists(t):
                logging.info(f"Arquivo localizado em caminho alternativo: {t}")
                caminho_arquivo = t
                break
        else:
            logging.error(f"Arquivo base não encontrado após várias tentativas: {caminho_arquivo}")
            raise FileNotFoundError(f"Arquivo base não encontrado: {caminho_arquivo}")

    # Salva Metadados para o Dashboard
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    with open(METADATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(clientes, f, indent=4, ensure_ascii=False)
    logging.info(f"Metadados salvos em {METADATA_FILE}")

    # Ler o arquivo base
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            config_template = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Erro ao ler template: {e}")
        return

    diretorio = os.path.dirname(caminho_arquivo)
    nome_arquivo_base = os.path.splitext(os.path.basename(caminho_arquivo))[0]

    # Limpar partes antigas
    for f_antigo in glob.glob(os.path.join(diretorio, f"{nome_arquivo_base}_part_*.yaml")):
        try: os.remove(f_antigo)
        except: pass

    # Divisão em chunks
    cnpjs = sorted(list(set([c['cnpj'] for c in clientes])))
    CHUNK_SIZE = 1850
    num_dags = math.ceil(len(cnpjs) / CHUNK_SIZE)
    
    for i in range(num_dags):
        chunk = cnpjs[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
        config_parte = copy.deepcopy(config_template)
        
        # ID Único
        if 'dag' in config_parte and 'id' in config_parte['dag']:
            config_parte['dag']['id'] = f"{config_parte['dag']['id']}_part_{i+1}"
        
        # Termos
        sessao_busca = config_parte['dag']['search']
        alvo_busca = sessao_busca[0] if isinstance(sessao_busca, list) else sessao_busca
        alvo_busca['terms'] = chunk

        # Salvar
        caminho_parte = os.path.join(diretorio, f"{nome_arquivo_base}_part_{i+1}.yaml")
        with open(caminho_parte, 'w', encoding='utf-8') as f:
            yaml.safe_dump(config_parte, f, allow_unicode=True, sort_keys=False)
        logging.info(f"Parte {i+1} salva com {len(chunk)} CNPJs")

# Função principal (para Airflow ou CLI)
def executar_sincronizacao():
    if load_dotenv: load_dotenv(override=True)

    url_api = os.getenv("BASE_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    secret_token = os.getenv("SECRET_ACCESS_TOKEN")
    arquivo_yaml = os.getenv("YAML_PATH")

    if not all([url_api, access_token, secret_token]):
        logging.error("Credenciais ausentes no .env")
        return

    headers = {"access-token": access_token, "secret-access-token": secret_token, "Accept": "application/json"}
    
    logging.info("Iniciando sincronização completa...")
    clientes = get_monitored_data(url_api, "clientes", headers)

    if not clientes:
        logging.warning("Nenhum dado retornado da API")
        return
    
    atualizar_configuracoes(arquivo_yaml, clientes)

# Boilerplate Airflow
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    
    with DAG(
        dag_id='sync_cnpj_gestaoclick',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['sync', 'gestaoclick'],
    ) as dag:
        tarefa = PythonOperator(task_id='tarefa_atualizar_cnpjs', python_callable=executar_sincronizacao)
except ImportError:
    if __name__ == "__main__":
        executar_sincronizacao()
