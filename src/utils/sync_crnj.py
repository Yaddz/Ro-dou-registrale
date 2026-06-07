import os
import re
import yaml
import logging
import requests
import math
import glob
import copy
from typing import Set, Optional
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Configurando LOG
logging.basicConfig(level=logging.INFO)

# Extração de dados e validação!
def extrair_cnpj(cnpj_bruto: str) -> Set[str]:
    """Recebe um CNPJ sujo, limpa e retorna versões com e sem máscara se for um CNPJ válido."""
    resultados = set()
    if not cnpj_bruto:
        return resultados

    cnpj_str = str(cnpj_bruto).strip()
    
    resultados.add(cnpj_str)    #Ex. 12.345.678/0001-90
        
    return resultados

def extrair_cnpj_regex(cnpj: str) -> Set[str]:
    """Caso a extração convêncional não funcione, esta função extrai o CNPJs perdidos em um texto usando REGEX."""
    cnpj_encontrados = set()
    padrao_cnpj = r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})|(\d{14})'
    
    matches = re.findall(padrao_cnpj, cnpj)
    for grupo in matches:
        valor = grupo[0] or grupo[1]
        cnpj_encontrados.update(extrair_cnpj(valor))
    
    return cnpj_encontrados


# Comunicação com a API
def cnpj_endpoint(url_base: str, endpoint: str, headers: dict) -> Set[str]:
    """Faz a requisição para a API considerando o limite maximo de 100 registros por página e extrai os CNPJs."""
    cnpjs = set()
    pagina_atual = 1
    url_completa = f"{url_base.rstrip('/')}/{endpoint.lstrip('/')}"
    
    while True:
        logging.info(f"Buscando {url_completa} - Página {pagina_atual}")
        
        try:
            resposta = requests.get(url_completa, params={"pagina": pagina_atual}, headers=headers, timeout=30)
            
            if resposta.status_code == 404:
                logging.warning(f"Endpoint não encontrado: {endpoint} (404)")
                break
        
            resposta.raise_for_status()
        
            try: 
                # Tenta ler como JSON
                dados_json = resposta.json()
                itens = dados_json.get("data", [])
                
                if not itens:
                    break
                
                for item in itens:
                    cnpj_cru = item.get("cnpj") or item.get("cpf_cnpj")
                    cnpjs.update(extrair_cnpj(cnpj_cru))
                    
                # Paginação
                proxima_pagina = dados_json.get("meta", {}).get("proxima_pagina")
                if not proxima_pagina or int(proxima_pagina) <= pagina_atual:
                    break
                
                pagina_atual = int(proxima_pagina)

            except ValueError:
                # Caso falhe ao ler JSON, usar o REGEX
                logging.warning(f"JSON malformado em {endpoint}. Usando REGEX para extrair CNPJs.")
                cnpjs.update(extrair_cnpj_regex(resposta.text))
                break
        
        except Exception as erro:
            logging.error(f"Erro ao buscar {endpoint} na página {pagina_atual}: {erro}")
            break
        
    return cnpjs

# Validar e atualizar o YAML
def atualizar_yaml(caminho_arquivo: str, novos_cnpjs: Set[str]):
    """Valida a existência do arquivo, divide os CNPJs em múltiplos arquivos YAML se necessário."""
    
    # Validações
    if not caminho_arquivo:
        logging.error("A variável de ambiente 'YAML_PATH' está vazia ou não foi definida.")
        return

    if not os.path.exists(caminho_arquivo):
        logging.error(f"ABORTADO: O arquivo YAML não foi encontrado no caminho especificado: {caminho_arquivo}")
        return

    if not os.path.isfile(caminho_arquivo):
        logging.error(f"ABORTADO: O caminho fornecido existe, mas não é um arquivo válido: {caminho_arquivo}")
        return

    # Ler o arquivo base para usar como template
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            config_template = yaml.safe_load(f)
    except Exception as e:
        logging.error(f"ABORTADO: Ocorreu um erro ao tentar ler o arquivo YAML: {e}")
        return

    diretorio = os.path.dirname(caminho_arquivo)
    nome_arquivo_base = os.path.splitext(os.path.basename(caminho_arquivo))[0]

    # Limpar arquivos de partes antigas para evitar DAGs órfãs
    padrao_antigos = os.path.join(diretorio, f"{nome_arquivo_base}_part_*.yaml")
    for f_antigo in glob.glob(padrao_antigos):
        try:
            os.remove(f_antigo)
            logging.info(f"Removido arquivo antigo: {f_antigo}")
        except Exception as e:
            logging.warning(f"Erro ao remover arquivo antigo {f_antigo}: {e}")

    # Processamento e divisão
    cnpjs_novos_ordenados = sorted(list(novos_cnpjs))
    total_cnpjs = len(cnpjs_novos_ordenados)
    
    # Define o tamanho do chunk (1850 conforme exemplo do usuário: 18500 / 10 = 1850)
    CHUNK_SIZE = 1850
    num_dags = math.ceil(total_cnpjs / CHUNK_SIZE)
    
    logging.info(f"Dividindo {total_cnpjs} CNPJs em {num_dags} DAG(s) (máx {CHUNK_SIZE} por DAG).")

    for i in range(num_dags):
        inicio = i * CHUNK_SIZE
        fim = min((i + 1) * CHUNK_SIZE, total_cnpjs)
        chunk = cnpjs_novos_ordenados[inicio:fim]
        
        # Cria uma cópia profunda do template para esta parte
        config_parte = copy.deepcopy(config_template)
        
        # Ajusta o ID da DAG para ser único no Airflow
        try:
            if 'dag' in config_parte and 'id' in config_parte['dag']:
                config_parte['dag']['id'] = f"{config_parte['dag']['id']}_part_{i+1}"
            else:
                logging.warning(f"Estrutura 'dag.id' não encontrada no template para parte {i+1}.")
        except Exception as e:
            logging.error(f"Erro ao ajustar ID na parte {i+1}: {e}")

        # Insere o chunk de CNPJs na primeira busca encontrada
        try:
            sessao_busca = config_parte['dag']['search']
            alvo_busca = sessao_busca[0] if isinstance(sessao_busca, list) else sessao_busca
            alvo_busca['terms'] = chunk
        except (KeyError, TypeError, IndexError) as e:
            logging.error(f"Erro ao inserir termos na parte {i+1}: {e}")
            continue

        # Salva o novo arquivo de parte
        caminho_parte = os.path.join(diretorio, f"{nome_arquivo_base}_part_{i+1}.yaml")
        try:
            with open(caminho_parte, 'w', encoding='utf-8') as f:
                yaml.safe_dump(config_parte, f, allow_unicode=True, sort_keys=False)
            logging.info(f"Parte {i+1} salva: {caminho_parte} ({len(chunk)} CNPJs)")
        except Exception as e:
            logging.error(f"Erro ao salvar arquivo da parte {i+1}: {e}")

# Função principal
def executar_sincronizacao():
    """Função principal executada pelo Airflow. Orquestra todas as outras funções."""
    
    # Carrega as variáveis de ambiente do arquivo .env
    if load_dotenv:
        load_dotenv()

    # Busca as configurações
    url_api = os.getenv("BASE_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    secret_token = os.getenv("SECRET_ACCESS_TOKEN")
    arquivo_yaml = os.getenv("YAML_PATH")

    # Validação de segurança para garantir que o .env foi lido corretamente
    if not all([url_api, access_token, secret_token]):
        logging.error("Credenciais ausentes! Verifique se BASE_URL, ACCESS_TOKEN e SECRET_ACCESS_TOKEN estão no seu .env.")
        return

    headers = {
        "access-token": access_token,
        "secret-access-token": secret_token,
        "Accept": "application/json"
    }

    todos_cnpjs = set()
    endpoints = ["clientes"]

    logging.info(f"Iniciando varredura na API: {url_api}...")
    
    for endpoint in endpoints:
        cnpjs_encontrados = cnpj_endpoint(url_api, endpoint, headers)
        todos_cnpjs.update(cnpjs_encontrados)
        logging.info(f"Total parcial: {len(todos_cnpjs)} CNPJs únicos após ler '{endpoint}'.")

    if not todos_cnpjs:
        logging.warning("Nenhum CNPJ encontrado na API. Abortando atualização.")
        return
    
    # Chama a função que vai validar e atualizar o arquivo YAML
    atualizar_yaml(arquivo_yaml, todos_cnpjs)

# Configuração das DAGs 
argumentos_padrao = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sync_cnpj_gestaoclick',
    default_args=argumentos_padrao,
    description='Sincroniza CNPJs da API GestãoClick para o YAML configurado',
    schedule_interval='@daily',
    catchup=False,
    tags=['sync', 'gestaoclick', 'cnpj'],
) as dag:

    tarefa_sincronizacao = PythonOperator(
        task_id='tarefa_atualizar_cnpjs',
        python_callable=executar_sincronizacao,
    )