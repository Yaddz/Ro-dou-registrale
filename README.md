# Ro-DOU Registrale

Sistema inteligente de monitoramento e análise de publicações do Diário Oficial da União (DOU).

## 🚀 Arquitetura do Sistema

O projeto é composto por três pilares principais:

1.  **Robôs de Busca (Airflow):** Orquestram as varreduras diárias no DOU, segmentando a base de clientes para alta performance.
2.  **Dashboard de Gestão (Flask):** Interface visual premium para controle de rotinas, relatórios avançados e configurações.
3.  **Sincronizador (GestãoClick):** Integração automática para manter a base de monitoramento sempre atualizada com o seu ERP.

## 🛠️ Como Iniciar (Produção via Docker)

O sistema está totalmente containerizado para facilitar o deploy.

### Pré-requisitos
- Docker e Docker Compose instalados.
- Arquivo `.env` configurado com as chaves necessárias.

### Comandos
1.  **Subir todos os serviços:**
    ```bash
    docker compose up -d
    ```
2.  **Acessar o Dashboard:**
    Abra o navegador em `http://localhost:5000`

3.  **Acessar o Apache Airflow:**
    Abra o navegador em `http://localhost:8080` (User: `airflow` / Pass: `airflow`)

## 📊 Dashboard de Controle

O dashboard oferece as seguintes funcionalidades:

-   **Visão Geral:** KPIs de monitoramento e menções em tempo real.
-   **Rotinas de Busca:** Crie pesquisas personalizadas com agendamento amigável (sem precisar de código).
-   **Relatórios:** Filtros avançados por data, empresa, seção do DOU e exportação inteligente para CSV.
-   **Integrações:** Configure SMTP para e-mails e tokens de API diretamente pela interface.
-   **Gestão de Usuários:** Controle quem tem acesso ao painel (Níveis Master e Consulta).

## 🔐 Credenciais Padrão

-   **Dashboard (Admin):** `admin` / `admin`
-   **Airflow:** `airflow` / `airflow`

## 📁 Estrutura de Pastas

-   `/src`: Core do sistema e lógica dos buscadores.
-   `/dag_confs`: Arquivos YAML de configuração das buscas.
-   `/data`: Base de dados persistente (JSON).
-   `/mnt`: Logs e dados de volume do Postgres.
-   `app_dashboard.py`: Servidor do painel de controle.

---
© 2026 Registrale - Monitoramento Inteligente.
