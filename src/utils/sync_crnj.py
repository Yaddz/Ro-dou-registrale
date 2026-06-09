import os
import logging
import requests
import json
from typing import Dict, List
from datetime import datetime
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker

# Importa modelos
import sys
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, '..', '..'))
from src.database.models import Company, SystemLog

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Configurando LOG
logging.basicConfig(level=logging.INFO)

# Configuração do Banco de Dados
# O caminho data/rodou.db deve estar acessível tanto pelo Docker quanto localmente
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/rodou.db")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

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

# Função principal (para Airflow ou CLI)
def executar_sincronizacao():
    if load_dotenv: load_dotenv(override=True)

    url_api = os.getenv("BASE_URL")
    access_token = os.getenv("ACCESS_TOKEN")
    secret_token = os.getenv("SECRET_ACCESS_TOKEN")

    if not all([url_api, access_token, secret_token]):
        # Tenta carregar do banco de dados (SystemConfig) futuramente ou mantém fallback de arquivo se necessário
        logging.error("Credenciais ausentes no ambiente.")
        return

    headers = {"access-token": access_token, "secret-access-token": secret_token, "Accept": "application/json"}
    
    logging.info(f"Iniciando sincronização completa via API: {url_api}...")
    clientes_api = get_monitored_data(url_api, "clientes", headers)

    if not clientes_api:
        logging.warning("Nenhum dado retornado da API")
        return

    session = Session()
    try:
        # 1. Pega todos os CNPJs atuais no banco
        db_companies = session.query(Company).all()
        db_cnpj_map = {c.cnpj: c for c in db_companies}
        
        cnpjs_na_api = set()
        novos_count = 0
        atualizados_count = 0

        # 2. Upsert (Update or Insert)
        for c_api in clientes_api:
            cnpj = c_api['cnpj']
            cnpjs_na_api.add(cnpj)
            
            if cnpj in db_cnpj_map:
                # Update
                comp = db_cnpj_map[cnpj]
                comp.name = c_api['nome']
                comp.uf = c_api['uf']
                comp.city = c_api['cidade']
                comp.email = c_api['email']
                comp.phone = c_api['telefone']
                comp.situation = c_api['situacao']
                comp.is_active = True # Reativa se voltou a aparecer na API
                comp.last_sync = datetime.utcnow()
                atualizados_count += 1
            else:
                # Insert
                new_comp = Company(
                    cnpj=cnpj,
                    name=c_api['nome'],
                    uf=c_api['uf'],
                    city=c_api['cidade'],
                    email=c_api['email'],
                    phone=c_api['telefone'],
                    situation=c_api['situacao'],
                    is_active=True
                )
                session.add(new_comp)
                novos_count += 1

        # 3. Marcar como inativos os que sumiram da API
        inativados_count = 0
        for cnpj, comp in db_cnpj_map.items():
            if cnpj not in cnpjs_na_api and comp.is_active:
                comp.is_active = False
                inativados_count += 1

        session.commit()
        
        msg = f"Sincronização concluída: {novos_count} novos, {atualizados_count} atualizados, {inativados_count} inativados."
        logging.info(msg)
        
        # Registra log no banco
        session.add(SystemLog(event="Sincronização API", details=msg))
        session.commit()

    except Exception as e:
        session.rollback()
        logging.error(f"Erro durante persistência no banco: {e}")
    finally:
        session.close()

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
