from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(20), default='user') # 'master' or 'user'

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class Company(db.Model):
    __tablename__ = 'companies'
    id = db.Column(db.Integer, primary_key=True)
    cnpj = db.Column(db.String(20), unique=True, nullable=False)
    name = db.Column(db.String(255))
    uf = db.Column(db.String(2))
    city = db.Column(db.String(100))
    email = db.Column(db.String(100))
    phone = db.Column(db.String(50))
    situation = db.Column(db.String(50), default='Ativa')
    is_active = db.Column(db.Boolean, default=True) # If it should be monitored
    last_sync = db.Column(db.DateTime, default=datetime.utcnow)

class Mention(db.Model):
    __tablename__ = 'mentions'
    id = db.Column(db.Integer, primary_key=True)
    external_id = db.Column(db.String(100)) # ID from source if available
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    section = db.Column(db.String(50))
    date = db.Column(db.String(20)) # Published date string
    abstract = db.Column(db.Text)
    link = db.Column(db.String(500))
    detected_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    company = db.relationship('Company', backref=db.backref('mentions', lazy=True))

class SystemConfig(db.Model):
    __tablename__ = 'system_configs'
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(50)) # 'smtp', 'api_keys'
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text)

class SystemLog(db.Model):
    __tablename__ = 'system_logs'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    event = db.Column(db.String(100))
    details = db.Column(db.Text)
