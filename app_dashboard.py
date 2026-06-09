import os
import glob
import yaml
import json
import csv
import sys
import re
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, send_file
from dotenv import load_dotenv, set_key
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash

# Adiciona o diretório src ao path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, 'src'))

from database.models import db, User, Company, Mention, SystemConfig, SystemLog
from utils.sync_crnj import executar_sincronizacao
from flask_session import Session

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "rodou-secret-key-sqlite-2026")

# Configuração do Banco de Dados SQLite
DATABASE_PATH = os.path.join(BASE_DIR, "data", "rodou.db")
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{DATABASE_PATH}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Configuração de Sessão em SERVIDOR (FileSystem)
app.config.update(
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR=os.path.join(BASE_DIR, 'flask_sessions'),
    SESSION_PERMANENT=True,
    SESSION_REFRESH_EACH_REQUEST=False,
    SESSION_USE_SIGNER=True,
    SESSION_COOKIE_NAME='registrale_secure_sid',
    SESSION_COOKIE_SAMESITE='Strict',
    SESSION_COOKIE_HTTPONLY=True,
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=30)
)

db.init_app(app)
Session(app)

@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# Helpers de Segurança e Banco
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def add_log(event, details):
    try:
        new_log = SystemLog(event=event, details=details)
        db.session.add(new_log)
        db.session.commit()
    except: db.session.rollback()

def get_config(category, key, default=None):
    cfg = SystemConfig.query.filter_by(category=category, key=key).first()
    return cfg.value if cfg else default

def set_config(category, key, value):
    cfg = SystemConfig.query.filter_by(category=category, key=key).first()
    if cfg:
        cfg.value = value
    else:
        db.session.add(SystemConfig(category=category, key=key, value=value))
    db.session.commit()

# --- ROTAS DE AUTENTICAÇÃO ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            session.permanent = True
            session['user_id'] = user.id
            session['user_role'] = user.role
            session['username'] = user.username
            session['expires_at'] = (datetime.now() + app.permanent_session_lifetime).timestamp()
            return redirect(url_for('index'))
        return render_template('login.html', error="Usuário ou senha inválidos")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# --- ROTAS PRINCIPAIS ---

@app.route('/')
@login_required
def index():
    # Verifica expiração absoluta
    expires_at = session.get('expires_at')
    if expires_at and datetime.now().timestamp() > expires_at:
        session.clear()
        return redirect(url_for('login'))

    is_master = session.get('user_role') == 'master'
    
    # KPIs do Banco
    total_cnpjs = Company.query.count()
    ativos = Company.query.filter_by(is_active=True).count()
    
    today_str = datetime.now().strftime('%d/%m/%Y')
    month_str = datetime.now().strftime('/%m/%Y')
    
    mencoes_hoje = Mention.query.filter(Mention.date == today_str).count()
    este_mes = Mention.query.filter(Mention.date.like(f"%{month_str}")).count()
    
    # Últimas menções para o feed lateral
    recent_mentions = []
    mentions_db = Mention.query.order_by(Mention.detected_at.desc()).limit(20).all()
    for m in mentions_db:
        recent_mentions.append({
            "id": m.id,
            "empresa": m.company.name,
            "cnpj": m.company.cnpj,
            "secao": m.section,
            "data": m.date,
            "trecho": m.abstract,
            "link": m.link
        })

    # Histórico de Eventos
    logs = SystemLog.query.order_by(SystemLog.timestamp.desc()).limit(50).all()
    history = [{"data": l.timestamp.strftime('%d/%m %H:%M'), "evento": l.event, "detalhes": l.details} for l in logs]

    # Sync Info
    last_sync_log = SystemLog.query.filter_by(event="Sincronização API").order_by(SystemLog.timestamp.desc()).first()
    last_sync = last_sync_log.timestamp.strftime('%d/%m %H:%M') if last_sync_log else "N/A"
    
    last_search_log = SystemLog.query.filter(SystemLog.event.like("%Busca%")).order_by(SystemLog.timestamp.desc()).first()
    last_search = last_search_log.timestamp.strftime('%d/%m %H:%M') if last_search_log else "N/A"

    init_data = {
        "mencoes_recentes": recent_mentions,
        "kpis": {
            "cnpjs": total_cnpjs,
            "ativos": ativos,
            "mencoes_hoje": mencoes_hoje,
            "este_mes": este_mes
        }
    }

    # Configurações (Apenas Admin)
    settings = {"smtp": {}, "api_keys": {}}
    users_list = []
    if is_master:
        configs = SystemConfig.query.all()
        for c in configs:
            settings[c.category][c.key] = c.value
        
        users_db = User.query.all()
        users_list = [{"username": u.username, "role": u.role} for u in users_db]

    return render_template('index.html', 
                           user={"username": session['username'], "role": session['user_role']},
                           init_data=init_data,
                           mencoes=recent_mentions,
                           last_sync=last_sync,
                           last_search=last_search,
                           next_search="05:00 AM", # Estimado
                           time_left=max(0, int(expires_at - datetime.now().timestamp())) if expires_at else 0,
                           settings=settings,
                           users=users_list,
                           historico=history if history else [{"data": "N/A", "evento": "Status", "detalhes": "Aguardando sincronização."}])

# --- APIs DE DADOS ---

@app.route('/api/mentions')
@login_required
def api_mentions():
    mentions_db = Mention.query.order_by(Mention.detected_at.desc()).all()
    results = []
    for m in mentions_db:
        results.append({
            "id": m.id,
            "empresa": m.company.name,
            "cnpj": m.company.cnpj,
            "cnpj_norm": "".join(filter(str.isdigit, m.company.cnpj)),
            "secao": m.section,
            "data": m.date,
            "trecho": m.abstract,
            "link": m.link
        })
    return jsonify(results)

@app.route('/api/companies')
@login_required
def api_companies():
    companies = Company.query.order_by(Company.name).all()
    return jsonify([{
        "nome": c.name, "cnpj": c.cnpj, "uf": c.uf, "cidade": c.city,
        "email": c.email, "telefone": c.phone, "situacao": c.situation, "status": c.is_active
    } for c in companies])

@app.route('/api/company_history/<path:cnpj>')
@login_required
def company_history(cnpj):
    cnpj_clean = "".join(filter(str.isdigit, cnpj))
    company = Company.query.filter(Company.cnpj.like(f"%{cnpj_clean}%")).first()
    if not company: return jsonify([])
    
    mentions = Mention.query.filter_by(company_id=company.id).order_by(Mention.detected_at.desc()).all()
    return jsonify([{
        "id": m.id, "data": m.date, "secao": m.section, "trecho": m.abstract, "link": m.link
    } for m in mentions])

# --- GESTÃO DE ROTINAS ---

@app.route('/api/routines', methods=['GET', 'POST'])
@login_required
def manage_routines():
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    if request.method == 'GET':
        yaml_files = glob.glob(os.path.join(dag_confs_path, "*.yaml"))
        routines = []
        for f_path in yaml_files:
            name = os.path.basename(f_path)
            if "_part_" in name: continue
            try:
                with open(f_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    if not data or 'dag' not in data: continue
                    dag = data.get('dag', {})
                    search = dag.get('search', [{}])[0]
                    report = dag.get('report', {})
                    routines.append({
                        "id": dag.get('id', name),
                        "file": name,
                        "description": dag.get('description', ''),
                        "schedule": dag.get('schedule', '0 5 * * *'),
                        "terms": search.get('terms', []),
                        "organs": search.get('organs', []),
                        "sections": search.get('dou_sections', []),
                        "emails": report.get('emails', []),
                        "subject": report.get('subject', ''),
                        "type": "sync" if name == "Pesquisa_cnpj.yaml" else "custom"
                    })
            except: continue
        return jsonify(routines)
    
    if session.get('user_role') != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    filename = data.get('file') or f"{re.sub(r'\W+', '_', data['name'].lower())}.yaml"
    file_path = os.path.join(dag_confs_path, filename)
    
    # Mantém a estrutura YAML mas agora os termos podem ser dinâmicos
    new_dag = {"dag": {
        "id": re.sub(r'\.[^.]*$', '', filename),
        "description": data.get('description', ''),
        "schedule": data.get('schedule', '0 5 * * *'),
        "tags": ["custom"],
        "owner": ["admin"],
        "search": [{
            "header": data.get('name', 'Busca'),
            "organs": data.get('organs', []),
            "terms": "FROM_SQLITE" if filename == "Pesquisa_cnpj.yaml" else data.get('terms', ["AJUSTE"]),
            "dou_sections": data.get('sections', ["SECAO_1", "SECAO_2", "SECAO_3"]),
            "field": "TUDO", "is_exact_search": True, "full_text": True, "date": "DIA"
        }],
        "report": {
            "title": data.get('name', 'Alerta'),
            "emails": data.get('emails', []),
            "subject": data.get('subject', '')
        }
    }}
    
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(new_dag, f, allow_unicode=True, sort_keys=False)
    return jsonify({"status": "success", "message": "Rotina salva!"})

def trigger_airflow_dag(dag_id):
    import requests
    try:
        airflow_url = os.getenv('AIRFLOW_URL', 'http://airflow-webserver:8080')
        url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        auth = ("airflow", "airflow")
        response = requests.post(url, json={}, auth=auth, timeout=5)
        return response.status_code in [200, 201], response.text
    except Exception as e:
        return False, str(e)

@app.route('/api/routines/trigger/<path:file>', methods=['POST'])
@login_required
def trigger_routine(file):
    if file == "Pesquisa_cnpj.yaml":
        return trigger_sync_logic()
    
    file_path = os.path.join(BASE_DIR, "dag_confs", file)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            dag_id = data['dag']['id']
            ok, msg = trigger_airflow_dag(dag_id)
            if ok:
                add_log("Busca Manual", f"Disparada rotina: {dag_id}")
                return jsonify({"status": "success", "message": f"Busca {dag_id} iniciada!"})
            return jsonify({"status": "error", "message": "Erro no Airflow", "details": msg}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# --- CONFIGURAÇÕES E USUÁRIOS ---

@app.route('/api/save_settings', methods=['POST'])
@login_required
def save_settings():
    if session.get('user_role') != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    try:
        for cat, values in data.items():
            for k, v in values.items():
                set_config(cat, k, str(v))
        add_log("Configurações", "Configurações do sistema atualizadas.")
        return jsonify({"status": "success", "message": "Salvo no banco de dados!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/users', methods=['GET', 'POST', 'DELETE'])
@login_required
def manage_users():
    if session.get('user_role') != 'master': return jsonify({"status": "error"}), 403
    if request.method == 'GET':
        users = User.query.all()
        return jsonify([{"username": u.username, "role": u.role} for u in users])
    
    if request.method == 'POST':
        data = request.json
        if User.query.filter_by(username=data['username']).first():
            return jsonify({"status": "error", "message": "Já existe"}), 400
        new_user = User(username=data['username'], role=data.get('role', 'user'))
        new_user.set_password(data['password'])
        db.session.add(new_user)
        db.session.commit()
        return jsonify({"status": "success"})
    
    if request.method == 'DELETE':
        un = request.args.get('username')
        user = User.query.filter_by(username=un).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 500

@app.route('/api/sync', methods=['POST'])
@login_required
def manual_sync_route():
    return trigger_sync_logic()

def trigger_sync_logic():
    if not executar_sincronizacao:
        return jsonify({"status": "error", "message": "Motor de sync não carregado."}), 500
    try:
        executar_sincronizacao()
        return jsonify({"status": "success", "message": "Sincronização concluída!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/export_report')
@login_required
def export_report():
    # Exporta todas as empresas do banco
    companies = Company.query.all()
    output = os.path.join(BASE_DIR, "data", "relatorio_geral.csv")
    with open(output, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f, delimiter=';')
        w.writerow(["Empresa", "CNPJ", "UF", "Cidade", "Situacao", "Monitorado"])
        for c in companies:
            w.writerow([c.name, c.cnpj, c.uf, c.city, c.situation, "Sim" if c.is_active else "Não"])
    return send_file(output, as_attachment=True)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        # Usuário inicial se vazio
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', role='master')
            admin.set_password('admin')
            db.session.add(admin)
            db.session.commit()
    app.run(host='0.0.0.0', debug=False, port=5000)
