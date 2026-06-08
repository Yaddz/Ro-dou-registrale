import os
import glob
import yaml
import json
import csv
import sys
import re
import ast
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, send_file
from dotenv import load_dotenv, set_key
from functools import wraps

# Adiciona o diretório src ao path para importar o sync
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
try:
    from utils.sync_crnj import executar_sincronizacao
except ImportError:
    executar_sincronizacao = None

# Carrega variáveis de ambiente
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "rodou-secret-key-123")
app.permanent_session_lifetime = timedelta(minutes=30)

# Caminhos de persistência
DATA_DIR = "data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
METADATA_FILE = os.path.join(DATA_DIR, "monitored_companies.json")
HISTORY_FILE = os.path.join(DATA_DIR, "sync_history.json")
LOGS_DIR = os.path.join("mnt", "airflow-logs")

def load_json(file_path, default=[]):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return default
    return default

def save_json(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def add_history_event(evento, detalhes):
    history = load_json(HISTORY_FILE, [])
    history.insert(0, {
        "data": datetime.now().strftime('%d/%m %H:%M'),
        "evento": evento,
        "detalhes": detalhes
    })
    save_json(HISTORY_FILE, history[:50]) # Mantém apenas as últimas 50

def normalize_cnpj(cnpj):
    """Remove caracteres não numéricos para comparação."""
    if not cnpj: return ""
    return re.sub(r'\D', '', str(cnpj))

def get_monitored_cnpjs():
    """Lê os CNPJs que estão atualmente nos arquivos YAML (monitorados)."""
    dag_confs_path = "dag_confs"
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
    """Retorna todas as empresas do metadado e indica se estão ativas nos YAMLs."""
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
    if not os.path.exists(LOGS_DIR):
        return mentions

    # Procura logs de execução de busca (exec_search_*)
    log_files = glob.glob(os.path.join(LOGS_DIR, "dag_id=pesquisa_cnpj*", "run_id=*", "task_id=exec_searchs.exec_search_*", "attempt=*.log"), recursive=True)
    
    metadata = load_json(METADATA_FILE, [])
    cnpj_map = {normalize_cnpj(m['cnpj']): m['nome'] for m in metadata}

    for log_path in log_files:
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # Procura a linha de retorno de sucesso usando regex multilinhas (não guloso)
                matches = re.findall(r"Done\. Returned value was: (\{.*?\})(?=\n| \[|$)", content, re.DOTALL)
                for m in matches:
                    try:
                        # Limpa string para literal_eval
                        m_clean = m.strip()
                        result_dict = ast.literal_eval(m_clean)
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
                                    "trecho": pub.get('abstract', '').replace("<span class='highlight' style='background:#FFA;'>", "").replace("</span>", "").replace("<span class='highlight'>", ""),
                                    "link": pub.get('href', '#')
                                })
                    except: continue
        except: continue

    # Ordenar por data (descendente)
    try:
        mentions.sort(key=lambda x: datetime.strptime(x['data'], '%d/%m/%Y'), reverse=True)
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
            return redirect(url_for('index'))
        return render_template('login.html', error="Usuário ou senha inválidos")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

@app.route('/api/mentions')
@login_required
def api_mentions():
    return jsonify(get_real_mentions())

@app.route('/')
@login_required
def index():
    # Carrega o mínimo necessário para o load inicial rápido
    settings = load_json(SETTINGS_FILE, {"smtp":{}, "api_keys":{}})
    history = load_json(HISTORY_FILE, [])
    mencoes_recentes = get_real_mentions()[:20]
    
    # Contagem total de empresas
    all_metadata = load_json(METADATA_FILE, [])
    total_cnpjs = len(all_metadata)
    
    # Status Sync
    dag_confs_path = "dag_confs"
    yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
    last_sync = "N/A"
    if yaml_files:
        mtime = os.path.getmtime(yaml_files[0])
        last_sync = datetime.fromtimestamp(mtime).strftime('%d/%m %H:%M')
    elif history:
        last_sync = history[0]['data']

    # KPI Calculation
    active_cnpjs_count = len(get_monitored_cnpjs())
    
    # Mencoes Hoje
    hoje_str = datetime.now().strftime('%d/%m/%Y')
    mencoes_hoje = len([m for m in get_real_mentions() if m['data'] == hoje_str])
    
    # Este Mês
    mes_atual = datetime.now().strftime('/%m/%Y')
    mencoes_mes = len([m for m in get_real_mentions() if mes_atual in m['data']])

    return render_template('index.html', 
                           user=session['user'],
                           kpis={
                               "cnpjs": total_cnpjs, 
                               "ativos": active_cnpjs_count,
                               "mencoes_hoje": mencoes_hoje, 
                               "este_mes": mencoes_mes
                           }, 
                           mencoes=mencoes_recentes,
                           last_sync=last_sync,
                           settings=settings,
                           users=load_json(USERS_FILE),
                           historico=history if history else [{"data": last_sync, "evento": "Status", "detalhes": "Aguardando primeira sincronização."}])

@app.route('/api/companies')
@login_required
def api_companies():
    return jsonify(get_companies_data())

@app.route('/api/company_history/<path:cnpj>')
@login_required
def company_history(cnpj):
    """Retorna todas as menções históricas de um CNPJ específico."""
    all_mentions = get_real_mentions()
    cnpj_norm = normalize_cnpj(cnpj)
    # Filtra comparando ambos normalizados
    history = [m for m in all_mentions if m['cnpj_norm'] == cnpj_norm]
    return jsonify(history)

@app.route('/api/sync', methods=['POST'])
@login_required
def trigger_sync():
    if not executar_sincronizacao:
        return jsonify({"status": "error", "message": "Função de sincronização não encontrada."}), 500
    try:
        executar_sincronizacao()
        add_history_event("Sincronização OK", "Sincronização com GestãoClick realizada com sucesso.")
        return jsonify({"status": "success", "message": "Sincronização concluída com sucesso!"})
    except Exception as e:
        error_msg = str(e)
        if "Arquivo base não encontrado" in error_msg:
            error_msg = "Erro: Arquivo YAML Base não encontrado. Verifique o caminho nas Integrações."
        add_history_event("Erro Sync", error_msg)
        return jsonify({"status": "error", "message": error_msg}), 500

@app.route('/api/save_settings', methods=['POST'])
@login_required
def save_settings():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    save_json(SETTINGS_FILE, data)
    if 'api_keys' in data:
        env_path = '.env'
        ak = data['api_keys']
        try:
            mappings = {"gestaoclick_access_token": "ACCESS_TOKEN", "gestaoclick_secret_token": "SECRET_ACCESS_TOKEN", "gestaoclick_base_url": "BASE_URL", "yaml_path": "YAML_PATH"}
            for key, env_var in mappings.items():
                val = ak.get(key)
                if val:
                    set_key(env_path, env_var, val)
                    os.environ[env_var] = val
            y_path = ak.get('yaml_path')
            if y_path and not os.path.exists(y_path):
                return jsonify({"status": "warning", "message": f"Configurações salvas, mas o arquivo YAML não foi encontrado em: {y_path}."})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "success", "message": "Configurações salvas com sucesso!"})

@app.route('/api/users', methods=['GET', 'POST', 'DELETE'])
@login_required
def manage_users():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    users = load_json(USERS_FILE)
    if request.method == 'GET': return jsonify(users)
    if request.method == 'POST':
        data = request.json
        if any(u['username'] == data['username'] for u in users): return jsonify({"status": "error", "message": "Usuário já existe"}), 400
        users.append(data); save_json(USERS_FILE, users)
        return jsonify({"status": "success"})
    elif request.method == 'DELETE':
        username = request.args.get('username')
        if username == session['user']['username']: return jsonify({"status": "error", "message": "Não é possível excluir a si mesmo"}), 400
        users = [u for u in users if u['username'] != username]
        save_json(USERS_FILE, users)
        return jsonify({"status": "success"})

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
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    if not os.path.exists(USERS_FILE): save_json(USERS_FILE, [{"username": "admin", "password": "admin", "role": "master"}])
    if not os.path.exists(SETTINGS_FILE): save_json(SETTINGS_FILE, {"smtp":{}, "api_keys":{}})
    app.run(debug=True, port=5000)
