import os
import glob
import yaml
import json
import csv
import sys
import re
import ast
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, send_file
from dotenv import load_dotenv, set_key
from functools import wraps

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Adiciona o diretório src ao path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, 'src'))

try:
    from utils.sync_crnj import executar_sincronizacao
except ImportError:
    executar_sincronizacao = None

from flask_session import Session

# Carrega variáveis de ambiente
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "rodou-secret-key-123")

# Configuração de Sessão em SERVIDOR (FileSystem)
app.config.update(
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'flask_sessions'),
    SESSION_PERMANENT=True,
    SESSION_REFRESH_EACH_REQUEST=False,
    SESSION_USE_SIGNER=True,
    SESSION_COOKIE_NAME='registrale_secure_sid',
    SESSION_COOKIE_SAMESITE='Strict',
    SESSION_COOKIE_HTTPONLY=True,
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=30)
)
Session(app)

@app.after_request
def add_header(response):
    """Previne o cache do navegador para evitar que páginas logadas apareçam após logout ou em novas janelas."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# Caminhos de persistência ABSOLUTOS
DATA_DIR = os.path.join(BASE_DIR, "data")
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
METADATA_FILE = os.path.join(DATA_DIR, "monitored_companies.json")
HISTORY_FILE = os.path.join(DATA_DIR, "sync_history.json")
LOGS_DIR = os.path.join(BASE_DIR, "mnt", "airflow-logs")

def load_json(file_path, default=[]):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erro ao carregar {file_path}: {e}")
            return default
    return default

def save_json(file_path, data):
    try:
        # Garante que a pasta exista
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception as e:
        logger.error(f"Erro fatal ao salvar {file_path}: {e}")
        return False

def add_history_event(evento, detalhes):
    history = load_json(HISTORY_FILE, [])
    history.insert(0, {
        "data": datetime.now().strftime('%d/%m %H:%M'),
        "evento": evento,
        "detalhes": detalhes
    })
    save_json(HISTORY_FILE, history[:50])

def normalize_cnpj(cnpj):
    if not cnpj: return ""
    return re.sub(r'\D', '', str(cnpj))

def get_monitored_cnpjs():
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
    active_cnpjs = set()
    for f_path in yaml_files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                search = config.get('dag', {}).get('search', [])
                terms = search[0].get('terms', []) if isinstance(search, list) else search.get('terms', [])
                if isinstance(terms, list):
                    for t in terms:
                        active_cnpjs.add(normalize_cnpj(t))
        except: continue
    return active_cnpjs

def get_companies_data():
    active_cnpjs = get_monitored_cnpjs()
    all_metadata = load_json(METADATA_FILE, [])
    empresas = []
    for meta in all_metadata:
        cnpj_bruto = meta.get('cnpj')
        cnpj_norm = normalize_cnpj(cnpj_bruto)
        is_active = cnpj_norm in active_cnpjs
        empresas.append({
            "nome": meta.get("nome", "N/A"),
            "cnpj": cnpj_bruto,
            "uf": meta.get("uf", "N/A"),
            "cidade": meta.get("cidade", "N/A"),
            "email": meta.get("email", "N/A"),
            "telefone": meta.get("telefone", "N/A"),
            "situacao": meta.get("situacao", "Ativa"),
            "status": is_active
        })
    return sorted(empresas, key=lambda x: x['nome'])

def get_real_mentions():
    """Varre os logs do Airflow para extrair as menções reais encontradas."""
    mentions = []
    if not os.path.exists(LOGS_DIR): return mentions

    # Procura logs de execução de busca
    log_files = glob.glob(os.path.join(LOGS_DIR, "dag_id=pesquisa_cnpj*", "run_id=*", "task_id=exec_searchs.exec_search_*", "attempt=*.log"), recursive=True)
    
    metadata = load_json(METADATA_FILE, [])
    cnpj_map = {normalize_cnpj(m['cnpj']): m['nome'] for m in metadata}

    for log_path in log_files:
        try:
            # Otimização: Lê apenas o final do arquivo onde as mensagens de conclusão ficam
            with open(log_path, 'rb') as f:
                size = os.path.getsize(log_path)
                if size > 100000: # 100KB
                    f.seek(size - 100000)
                content = f.read().decode('utf-8', errors='ignore')
                
                # Regex otimizada sem backtracking excessivo
                matches = re.finditer(r"\[(.*?)\].*?Done\. Returned value was: (\{.*?\})$", content, re.MULTILINE)
                
                for match in matches:
                    log_time = match.group(1)
                    dict_str = match.group(2).strip()
                    
                    try:
                        result_dict = ast.literal_eval(dict_str)
                        results = result_dict.get('result', {}).get('single_group', {})
                        if not results: continue

                        for cnpj_log, content_group in results.items():
                            cnpj_norm = normalize_cnpj(cnpj_log)
                            depts = content_group.get('single_department', [])
                            for pub in depts:
                                mentions.append({
                                    "id": pub.get('id', str(datetime.now().timestamp())),
                                    "empresa": cnpj_map.get(cnpj_norm, cnpj_log),
                                    "cnpj": cnpj_log,
                                    "cnpj_norm": cnpj_norm,
                                    "secao": pub.get('section', 'DOU'),
                                    "data": pub.get('date', 'N/A'),
                                    "detected_at": log_time,
                                    "trecho": pub.get('abstract', '').replace("<span class='highlight' style='background:#FFA;'>", "").replace("</span>", "").replace("<span class='highlight'>", ""),
                                    "link": pub.get('href', '#')
                                })
                    except: continue
        except: continue

    # Ordenação Robusta
    try:
        mentions.sort(key=lambda x: (
            datetime.strptime(x['data'], '%d/%m/%Y') if x['data'] != 'N/A' else datetime.min,
            x['detected_at']
        ), reverse=True)
    except: pass

    return mentions

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        users = load_json(USERS_FILE)
        user = next((u for u in users if u['username'] == username and u['password'] == password), None)
        if user:
            session.permanent = True
            session['user'] = user
            session['expires_at'] = (datetime.now() + app.permanent_session_lifetime).timestamp()
            return redirect(url_for('index'))
        return render_template('login.html', error="Usuário ou senha inválidos")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/api/mentions')
@login_required
def api_mentions():
    return jsonify(get_real_mentions())

def get_last_search_time():
    if not os.path.exists(LOGS_DIR): return "N/A"
    log_files = glob.glob(os.path.join(LOGS_DIR, "dag_id=pesquisa_cnpj*", "run_id=*", "task_id=exec_searchs.exec_search_*", "attempt=*.log"), recursive=True)
    if not log_files: return "N/A"
    try:
        latest_log = max(log_files, key=os.path.getmtime)
        return datetime.fromtimestamp(os.path.getmtime(latest_log)).strftime('%d/%m %H:%M')
    except:
        return "N/A"

def get_next_search_time():
    now = datetime.now()
    # Baseado no DEFAULT_SCHEDULE "0 5 * * *" do gerador de DAGs
    next_run = now.replace(hour=5, minute=0, second=0, microsecond=0)
    if now >= next_run:
        next_run += timedelta(days=1)
    return next_run.strftime('%d/%m %H:%M')

@app.route('/')
@login_required
def index():
    # Verifica expiração absoluta
    expires_at = session.get('expires_at')
    if expires_at and datetime.now().timestamp() > expires_at:
        session.clear()
        return redirect(url_for('login'))

    is_master = session['user']['role'] == 'master'
    settings = load_json(SETTINGS_FILE, {"smtp":{}, "api_keys":{}}) if is_master else {"smtp":{}, "api_keys":{}}
    users_list = load_json(USERS_FILE) if is_master else []
    history = load_json(HISTORY_FILE, [])
    all_mentions = get_real_mentions()
    
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
    last_sync = "N/A"
    if yaml_files:
        mtime = os.path.getmtime(yaml_files[0])
        last_sync = datetime.fromtimestamp(mtime).strftime('%d/%m %H:%M')

    last_search = get_last_search_time()
    next_search = get_next_search_time()
    
    # Calcula tempo restante para o frontend
    time_left = 0
    if expires_at:
        time_left = max(0, int(expires_at - datetime.now().timestamp()))

    # Data inicial para o Alpine
    init_data = {
        "mencoes_recentes": all_mentions[:20],
        "kpis": {
            "cnpjs": len(load_json(METADATA_FILE, [])),
            "ativos": len(get_monitored_cnpjs()),
            "mencoes_hoje": len([m for m in all_mentions if m['data'] == datetime.now().strftime('%d/%m/%Y')]),
            "este_mes": len([m for m in all_mentions if datetime.now().strftime('/%m/%Y') in m['data']])
        }
    }

    return render_template('index.html', 
                           user=session['user'],
                           init_data=init_data,
                           mencoes=all_mentions[:20],
                           last_sync=last_sync,
                           last_search=last_search,
                           next_search=next_search,
                           time_left=time_left,
                           settings=settings,
                           users=users_list,
                           historico=history if history else [{"data": last_sync, "evento": "Status", "detalhes": "Aguardando sincronização."}])

@app.route('/api/companies')
@login_required
def api_companies():
    return jsonify(get_companies_data())

@app.route('/api/company_history/<path:cnpj>')
@login_required
def company_history(cnpj):
    all_mentions = get_real_mentions()
    cnpj_norm = normalize_cnpj(cnpj)
    history = [m for m in all_mentions if m['cnpj_norm'] == cnpj_norm]
    return jsonify(history)

def get_routines():
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "*.yaml"))
    
    routines = []
    sync_parts = []
    sync_base_data = None
    
    for f_path in yaml_files:
        name = os.path.basename(f_path)
        
        # Identifica se é uma parte da rotina de sincronização
        if "pesquisa_cnpj" in name.lower():
            if "_part_" in name.lower():
                sync_parts.append(f_path)
                continue
            elif name.lower() == "pesquisa_cnpj.yaml":
                # É o arquivo base, vamos processar para pegar as configurações padrão
                try:
                    with open(f_path, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data and 'dag' in data:
                            dag = data.get('dag', {})
                            search = dag.get('search', {})
                            if isinstance(search, list): search = search[0]
                            report = dag.get('report', {})
                            sync_base_data = {
                                "id": dag.get('id', name),
                                "file": name,
                                "description": dag.get('description', ''),
                                "schedule": dag.get('schedule', '0 5 * * *'),
                                "terms": search.get('terms', []),
                                "organs": search.get('organs', []),
                                "sections": search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]),
                                "emails": report.get('emails', []),
                                "subject": report.get('subject', ''),
                                "type": "sync"
                            }
                except: pass
                continue

        # Processa outras rotinas customizadas
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if not data or 'dag' not in data: continue
                dag = data.get('dag', {})
                search = dag.get('search', {})
                if isinstance(search, list): search = search[0]
                report = dag.get('report', {})
                
                routines.append({
                    "id": dag.get('id', name),
                    "file": name,
                    "description": dag.get('description', ''),
                    "schedule": dag.get('schedule', '0 5 * * *'),
                    "terms": search.get('terms', []),
                    "organs": search.get('organs', []),
                    "sections": search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]),
                    "emails": report.get('emails', []),
                    "subject": report.get('subject', ''),
                    "type": "custom"
                })
        except Exception as e: 
            logger.error(f"Erro ao ler rotina {name}: {e}")
            continue
    
    # Consolida a Rotina de Sincronização
    total_cnpjs = 0
    # Soma termos de todas as partes encontradas
    for sp in sync_parts:
        try:
            with open(sp, 'r', encoding='utf-8') as f:
                d = yaml.safe_load(f)
                s = d.get('dag', {}).get('search', {})
                if isinstance(s, list): s = s[0]
                total_cnpjs += len(s.get('terms', []))
        except: continue
    
    # Soma termos do arquivo base se ele tiver termos diretos (raro mas possível)
    if sync_base_data and isinstance(sync_base_data.get('terms'), list):
        if "_part_" not in sync_base_data['file']: # Evita duplicidade se base for confundida
             total_cnpjs += len(sync_base_data['terms'])

    # Monta o registro único de Sincronização
    sync_routine = {
        "id": "Sincronização Automática (GestãoClick)",
        "file": "Pesquisa_cnpj.yaml",
        "description": sync_base_data.get('description') if sync_base_data and sync_base_data.get('description') else f"Sincronização automática via API. Monitorando {total_cnpjs} CNPJs.",
        "schedule": sync_base_data.get('schedule', '0 5 * * *') if sync_base_data else "0 5 * * *",
        "terms": [f"{total_cnpjs} CNPJs monitorados"],
        "organs": sync_base_data.get('organs', ["Diversos"]) if sync_base_data else ["Diversos"],
        "sections": sync_base_data.get('sections', ["SECAO_1", "SECAO_2", "SECAO_3"]) if sync_base_data else ["SECAO_1", "SECAO_2", "SECAO_3"],
        "emails": sync_base_data.get('emails', []) if sync_base_data else [],
        "subject": sync_base_data.get('subject', '') if sync_base_data else '',
        "type": "sync"
    }
    
    routines.insert(0, sync_routine)
    return routines

@app.route('/api/routines', methods=['GET', 'POST'])
@login_required
def manage_routines():
    if request.method == 'GET':
        return jsonify(get_routines())
    
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    
    # Se for edição de arquivo existente ou criação de novo
    filename = data.get('file')
    if not filename:
        new_id = re.sub(r'\W+', '_', data['name'].lower())
        filename = f"{new_id}.yaml"
        
    file_path = os.path.join(BASE_DIR, "dag_confs", filename)
    
    # Se o arquivo já existe, carrega para manter campos não editados
    existing_data = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = yaml.safe_load(f)
        except: pass

    # Monta a estrutura preservando campos do Airflow
    new_dag = existing_data or {"dag": {}}
    dag = new_dag["dag"]
    
    dag["id"] = dag.get("id") or re.sub(r'\.[^.]*$', '', filename)
    dag["description"] = data.get('description', dag.get('description', ''))
    dag["schedule"] = data.get('schedule', dag.get('schedule', '0 5 * * *'))
    dag["tags"] = dag.get("tags", ["custom"])
    dag["owner"] = dag.get("owner", ["admin"])
    
    # Search config
    search = dag.get("search", {})
    if isinstance(search, list): 
        search = search[0] if len(search) > 0 else {}
    
    search["header"] = data.get('name', search.get('header', 'Busca'))
    search["organs"] = data.get('organs', search.get('organs', []))
    
    # Se for a rotina de sync, não sobrescreve os termos (pois eles vêm do GestãoClick)
    if filename != "Pesquisa_cnpj.yaml":
        search["terms"] = data.get('terms', search.get('terms', []))
        # Garantia Pydantic: deve ter pelo menos um termo ou critério
        if not search["terms"]:
            search["terms"] = ["TERMO_PROVISORIO_AJUSTE_PAINEL"]
    
    search["dou_sections"] = data.get('sections', search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]))
    search["field"] = search.get("field", "TUDO")
    search["is_exact_search"] = search.get("is_exact_search", True)
    search["full_text"] = search.get("full_text", True)
    search["date"] = search.get("date", "DIA")
    
    # O Pydantic espera uma LISTA de SearchConfigs
    dag["search"] = [search]
    
    # Report config
    report = dag.get("report", {})
    report["title"] = data.get('name', report.get('title', 'Alerta'))
    report["emails"] = data.get('emails', report.get('emails', []))
    report["subject"] = data.get('subject', report.get('subject', ''))
    
    dag["report"] = report
    
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(new_dag, f, allow_unicode=True, sort_keys=False)
    
    return jsonify({"status": "success", "message": "Rotina salva com sucesso!"})

@app.route('/api/routines/<path:file>', methods=['DELETE'])
@login_required
def delete_routine(file):
    if session['user']['role'] != 'master': return jsonify({"status": "error", "message": "Acesso negado."}), 403
    
    if file == "Pesquisa_cnpj.yaml" or "_part_" in file:
        return jsonify({"status": "error", "message": "Não é possível excluir as rotinas do sistema (Sync)."}), 400
        
    file_path = os.path.join(BASE_DIR, "dag_confs", file)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            add_history_event("Rotina Excluída", f"Rotina {file} removida do sistema.")
            return jsonify({"status": "success", "message": "Rotina excluída com sucesso!"})
        except Exception as e:
            return jsonify({"status": "error", "message": f"Erro ao excluir o arquivo: {str(e)}"}), 500
    
    return jsonify({"status": "error", "message": "Arquivo não encontrado."}), 404

@app.route('/api/sync', methods=['POST'])
@login_required
def manual_sync_route():
    return trigger_sync_logic()

def trigger_sync_logic():
    if not executar_sincronizacao:
        return jsonify({"status": "error", "message": "Função de sincronização não encontrada."}), 500
    try:
        executar_sincronizacao()
        add_history_event("Sincronização OK", "Sincronização realizada.")
        return jsonify({"status": "success", "message": "Concluído!"})
    except Exception as e:
        add_history_event("Erro Sync", str(e))
        logger.error(f"Erro na sincronização: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def trigger_airflow_dag(dag_id):
    """Tenta disparar uma DAG no Airflow via API REST ou Docker CLI."""
    import requests
    try:
        airflow_url = os.getenv('AIRFLOW_URL', 'http://localhost:8080')
        url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        auth = ("airflow", "airflow")
        response = requests.post(url, json={}, auth=auth, timeout=5)
        
        if response.status_code in [200, 201]:
            return True, f"DAG {dag_id} disparada via API."
        else:
            return False, f"Erro Airflow API ({response.status_code}): {response.text}"
    except Exception as e:
        # Fallback para docker exec caso a API não esteja acessível
        import subprocess
        try:
            result = subprocess.run(["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", "dags", "trigger", dag_id], capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                return True, f"DAG {dag_id} disparada via Docker CLI."
            else:
                return False, f"Erro Docker/Airflow: {result.stderr or result.stdout}"
        except Exception as e2:
            return False, f"Falha API REST ({str(e)}) e falha Docker CLI ({str(e2)})"

@app.route('/api/routines/trigger/<path:file>', methods=['POST'])
@login_required
def trigger_routine(file):
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    
    # Caso especial: Rotina de Sincronização (pode ter múltiplas partes)
    if file == "Pesquisa_cnpj.yaml":
        parts = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
        if not parts:
            # Se não tem partes, tenta a principal
            file_path = os.path.join(dag_confs_path, file)
            if not os.path.exists(file_path):
                return jsonify({"status": "error", "message": "Arquivo base não encontrado."}), 404
            parts = [file_path]
            
        success_count = 0
        errors = []
        for p in parts:
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    dag_id = data.get('dag', {}).get('id')
                    if dag_id:
                        ok, msg = trigger_airflow_dag(dag_id)
                        if ok: success_count += 1
                        else: errors.append(msg)
            except: continue
        
        if success_count > 0:
            add_history_event("Busca Iniciada", f"Disparadas {success_count} partes da pesquisa sync.")
            return jsonify({"status": "success", "message": f"Busca iniciada em {success_count} instâncias!"})
        else:
            return jsonify({"status": "error", "message": "Falha ao disparar busca no Airflow.", "details": errors}), 500

    # Rotinas Customizadas
    file_path = os.path.join(dag_confs_path, file)
    if not os.path.exists(file_path):
        return jsonify({"status": "error", "message": "Arquivo de rotina não encontrado."}), 404
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            dag_id = data.get('dag', {}).get('id')
            if not dag_id: dag_id = re.sub(r'\.[^.]*$', '', file)
            
            ok, msg = trigger_airflow_dag(dag_id)
            if ok:
                add_history_event("Busca Manual", f"Disparada rotina: {dag_id}")
                return jsonify({"status": "success", "message": f"Busca {dag_id} iniciada!"})
            else:
                # Mesmo se o comando falhar, registramos a tentativa
                add_history_event("Busca (Tentativa)", f"Tentativa de disparar {dag_id}: {msg}")
                return jsonify({"status": "warning", "message": "Busca solicitada, mas houve erro no Airflow.", "details": msg})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/save_settings', methods=['POST'])
@login_required
def save_settings():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    if save_json(SETTINGS_FILE, data):
        env_path = os.path.join(BASE_DIR, '.env')
        
        # Mapeamento para GestãoClick
        if 'api_keys' in data:
            ak = data['api_keys']
            mappings = {
                "gestaoclick_access_token": "ACCESS_TOKEN",
                "gestaoclick_secret_token": "SECRET_ACCESS_TOKEN",
                "gestaoclick_base_url": "BASE_URL",
                "yaml_path": "YAML_PATH"
            }
            for key, env_var in mappings.items():
                val = ak.get(key)
                if val:
                    set_key(env_path, env_var, val)
                    os.environ[env_var] = val
        
        # Mapeamento para SMTP (Airflow)
        if 'smtp' in data:
            smtp = data['smtp']
            smtp_mappings = {
                "server": "AIRFLOW__SMTP__SMTP_HOST",
                "port": "AIRFLOW__SMTP__SMTP_PORT",
                "user": "AIRFLOW__SMTP__SMTP_USER",
                "password": "AIRFLOW__SMTP__SMTP_PASSWORD"
            }
            for key, env_var in smtp_mappings.items():
                val = smtp.get(key)
                if val:
                    set_key(env_path, env_var, str(val))
                    os.environ[env_var] = str(val)
            
            # Adicionalmente define o FROM se o user for um email
            if smtp.get('user') and "@" in smtp.get('user'):
                set_key(env_path, "AIRFLOW__SMTP__SMTP_MAIL_FROM", smtp.get('user'))
                os.environ["AIRFLOW__SMTP__SMTP_MAIL_FROM"] = smtp.get('user')

        return jsonify({"status": "success", "message": "Configurações salvas e aplicadas!"})
    return jsonify({"status": "error", "message": "Erro ao salvar no arquivo de configurações."}), 500

@app.route('/api/users', methods=['GET', 'POST', 'DELETE'])
@login_required
def manage_users():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    users = load_json(USERS_FILE, [])
    if request.method == 'GET':
        return jsonify([{"username": u['username'], "role": u['role']} for u in users])
    if request.method == 'POST':
        data = request.json
        if not data.get('username') or not data.get('password'):
            return jsonify({"status": "error", "message": "Campos obrigatórios"}), 400
        if any(u['username'] == data['username'] for u in users):
            return jsonify({"status": "error", "message": "Já existe"}), 400
        users.append({"username": data['username'], "password": data['password'], "role": data.get('role', 'user')})
        if save_json(USERS_FILE, users): return jsonify({"status": "success", "message": "Criado!"})
    elif request.method == 'DELETE':
        username = request.args.get('username')
        if username == session['user']['username']: return jsonify({"status": "error"}), 400
        users = [u for u in users if u['username'] != username]
        if save_json(USERS_FILE, users): return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 500

@app.route('/api/export_report')
@login_required
def export_report():
    empresas = get_companies_data()
    output = os.path.join(DATA_DIR, "relatorio.csv")
    with open(output, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(["Empresa", "CNPJ", "UF", "Situacao", "Monitorado"])
        for e in empresas: w.writerow([e['nome'], e['cnpj'], e['uf'], e['situacao'], "Sim" if e['status'] else "Não"])
    return send_file(output, as_attachment=True)

if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(USERS_FILE): save_json(USERS_FILE, [{"username": "admin", "password": "admin", "role": "master"}])
    if not os.path.exists(SETTINGS_FILE): save_json(SETTINGS_FILE, {"smtp":{}, "api_keys":{}})
    app.run(host='0.0.0.0', debug=False, port=5000)
