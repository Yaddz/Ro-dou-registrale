import os
import csv
import logging
import requests
from dotenv import load_dotenv

# Configuração simples de log para acompanharmos o progresso no terminal
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def exportar_clientes_para_csv():
    # 1. Carrega as senhas do arquivo .env
    load_dotenv()
    
    url_base = os.getenv("BASE_URL", "https://api.gestaoclick.com").rstrip('/')
    endpoint = "clientes"
    url_completa = f"{url_base}/{endpoint}"
    
    access_token = os.getenv("ACCESS_TOKEN")
    secret_token = os.getenv("SECRET_ACCESS_TOKEN")
    
    if not all([access_token, secret_token]):
        logging.error("Credenciais ausentes! Verifique o seu arquivo .env.")
        return

    headers = {
        "access-token": access_token,
        "secret-access-token": secret_token,
        "Accept": "application/json"
    }

    # 2. Configuração do arquivo CSV
    nome_arquivo = "clientes_resumo.csv"
    cabecalhos = ["Nome_ou_Razao_Social", "CNPJ_CPF", "Situacao"]
    
    # Abre o arquivo CSV em modo de escrita ('w')
    # Usamos delimiter=';' porque o Excel em português entende melhor esse formato
    with open(nome_arquivo, mode='w', newline='', encoding='utf-8') as arquivo_csv:
        escritor = csv.DictWriter(arquivo_csv, fieldnames=cabecalhos, delimiter=';')
        escritor.writeheader()
        
        pagina_atual = 1
        total_salvos = 0
        
        logging.info("Iniciando o download e extração dos dados...")
        
        # 3. Loop de Paginação (Vai rodar até a última página)
        while True:
            logging.info(f"Lendo página {pagina_atual}...")
            
            try:
                resposta = requests.get(url_completa, params={"pagina": pagina_atual}, headers=headers, timeout=30)
                resposta.raise_for_status()
                
                dados = resposta.json()
                itens = dados.get("data", [])
                
                if not itens:
                    break # Interrompe o loop se a página vier vazia
                    
                for item in itens:
                    # Captura os dados principais. 
                    # Usamos 'or' caso o campo venha vazio ou com outro nome na API
                    nome = item.get("nome") or item.get("razao_social") or "N/A"
                    cnpj = item.get("cnpj") or item.get("cpf_cnpj") or "N/A"
                    situacao = item.get("situacao") or item.get("ativo") or "N/A"
                    
                    # Escreve a linha no Excel
                    escritor.writerow({
                        "Nome_ou_Razao_Social": str(nome).strip(),
                        "CNPJ_CPF": str(cnpj).strip(),
                        "Situacao": str(situacao).strip()
                    })
                    total_salvos += 1
                    
                # Checa se existe uma próxima página no metadado da API
                proxima = dados.get("meta", {}).get("proxima_pagina")
                if not proxima or int(proxima) <= pagina_atual:
                    break
                    
                pagina_atual = int(proxima)
                
            except Exception as e:
                logging.error(f"Ocorreu um erro ao processar a página {pagina_atual}: {e}")
                break
                
        logging.info(f"Extração concluída! {total_salvos} empresas foram salvas no arquivo '{nome_arquivo}'.")

# Executa a função quando rodamos o arquivo no terminal
if __name__ == "__main__":
    exportar_clientes_para_csv()