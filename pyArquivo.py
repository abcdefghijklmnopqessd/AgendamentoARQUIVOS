import streamlit as st
import os
import subprocess
import pickle
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
from datetime import date, datetime, time
import pandas as pd
from openpyxl import load_workbook
import uuid

# Diretórios para armazenar arquivos
if not os.path.exists("uploads"):
    os.makedirs("uploads")
if not os.path.exists("jobs"):
    os.makedirs("jobs")

# Inicializar o estado da sessão
if 'job_list' not in st.session_state:
    st.session_state.job_list = []

# Configuração do scheduler
scheduler = BackgroundScheduler()

# Carregar agendamentos salvos anteriormente
def carregar_agendamentos():
    jobs_file = "jobs/saved_jobs.pkl"
    if os.path.exists(jobs_file):
        try:
            with open(jobs_file, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            st.error(f"Erro ao carregar agendamentos: {e}")
    return []

# Salvar agendamentos
def salvar_agendamentos(jobs):
    jobs_file = "jobs/saved_jobs.pkl"
    try:
        with open(jobs_file, 'wb') as f:
            pickle.dump(jobs, f)
    except Exception as e:
        st.error(f"Erro ao salvar agendamentos: {e}")

# Função para executar arquivos .xlsx
def abrir_excel(file_path):
    try:
        wb = load_workbook(file_path)
        sheet = wb.active
        data = pd.DataFrame(sheet.values)
        # Exibir no log do servidor
        st.write(f"Arquivo Excel aberto: {file_path}")
        st.write(data)
        return True
    except Exception as e:
        st.error(f"Erro ao abrir o arquivo Excel: {e}")
        return False

# Função para executar arquivos .py
def executar_python(file_path):
    try:
        st.write(f"Executando arquivo Python: {file_path}")
        result = subprocess.run(["python", file_path], capture_output=True, text=True, check=True)
        st.write(f"Saída: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        st.error(f"Erro ao executar o arquivo Python: {e}")
        st.error(f"Saída de erro: {e.stderr}")
        return False

# Função para processar o arquivo quando chegar a hora agendada
def processar_arquivo(file_path, file_type, job_id):
    st.write(f"Processando arquivo: {file_path}, tipo: {file_type}")
    resultado = False

    if file_type == 'xlsx':
        resultado = abrir_excel(file_path)
    elif file_type == 'py':
        resultado = executar_python(file_path)
    else:
        st.error(f"Tipo de arquivo não suportado: {file_type}")

    # Atualizar status do job
    for job in st.session_state.job_list:
        if job['id'] == job_id:
            job['status'] = "Executado" if resultado else "Falha"
            job['data_execucao'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            break

    # Salvar agendamentos atualizados
    salvar_agendamentos(st.session_state.job_list)
    return resultado

# Função de agendamento
def agendar_arquivo(file, data_agendada, nome_arquivo):
    # Gerar ID único para o job
    job_id = str(uuid.uuid4())

    # Salvar o arquivo no servidor
    file_extension = nome_arquivo.split('.')[-1].lower()
    file_path = os.path.join("uploads", f"{job_id}.{file_extension}")

    with open(file_path, 'wb') as f:
        f.write(file.getbuffer())

    # Adicionar job ao scheduler
    job = scheduler.add_job(
        processar_arquivo,
        'date',
        run_date=data_agendada,
        args=[file_path, file_extension, job_id],
        id=job_id
    )

    # Adicionar à lista de jobs
    novo_job = {
        'id': job_id,
        'nome': nome_arquivo,
        'caminho': file_path,
        'data_agendada': data_agendada.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'Pendente',
        'data_execucao': None
    }

    st.session_state.job_list.append(novo_job)

    # Salvar agendamentos
    salvar_agendamentos(st.session_state.job_list)

    return job_id

# Carregar agendamentos salvos e iniciá-los
def iniciar_agendamentos_salvos():
    jobs_salvos = carregar_agendamentos()

    if jobs_salvos:
        st.session_state.job_list = jobs_salvos

        # Adicionar jobs pendentes ao scheduler
        for job in jobs_salvos:
            if job['status'] == 'Pendente':
                data_agendada = datetime.strptime(job['data_agendada'], '%Y-%m-%d %H:%M:%S')

                # Verificar se a data ainda está no futuro
                if data_agendada > datetime.now():
                    file_extension = job['nome'].split('.')[-1].lower()
                    scheduler.add_job(
                        processar_arquivo,
                        'date',
                        run_date=data_agendada,
                        args=[job['caminho'], file_extension, job['id']],
                        id=job['id']
                    )
                else:
                    # Marcar como expirado se a data já passou
                    job['status'] = 'Expirado'

        # Salvar as atualizações
        salvar_agendamentos(st.session_state.job_list)

# Iniciar o scheduler e carregar agendamentos
iniciar_agendamentos_salvos()
scheduler.start()

# Interface do Streamlit
st.title("Agendador de Arquivos")
st.write("Escolha um arquivo para agendar a execução:")

uploaded_file = st.file_uploader("Escolha um arquivo (.xlsx ou .py)", type=['xlsx', 'py'])
data = st.date_input("Escolha a data para execução", value=datetime.now())
hora = st.time_input("Escolha a hora para execução", datetime.now())

# Combinando data e hora em um objeto datetime
data_agendada = datetime.combine(data, hora)

if uploaded_file and data_agendada:
    if st.button("Agendar"):
        if data_agendada <= datetime.now():
            st.error("A data de agendamento deve ser no futuro!")
        else:
            job_id = agendar_arquivo(uploaded_file, data_agendada, uploaded_file.name)
            st.success(f"Arquivo '{uploaded_file.name}' agendado para {data_agendada.strftime('%d/%m/%Y %H:%M:%S')}")

# Função para excluir agendamento
def excluir_agendamento(job_id):
    # Remover o job do scheduler
    try:
        scheduler.remove_job(job_id)
    except Exception as e:
        st.error(f"Erro ao remover agendamento do scheduler: {e}")
    
    # Remover da lista de jobs
    for i, job in enumerate(st.session_state.job_list):
        if job['id'] == job_id:
            del st.session_state.job_list[i]
            break
    
    # Remover o arquivo, se existir
    for job in st.session_state.job_list:
        if job['id'] == job_id and os.path.exists(job['caminho']):
            try:
                os.remove(job['caminho'])
            except Exception as e:
                st.error(f"Erro ao remover arquivo: {e}")
    
    # Salvar a lista atualizada
    salvar_agendamentos(st.session_state.job_list)
    st.rerun()  # Atualizar a interface

# Mostrar agendamentos
st.subheader("Agendamentos")
if not st.session_state.job_list:
    st.info("Não há agendamentos.")
else:
    # Criar colunas para exibir os agendamentos com botão de exclusão
    cols = st.columns([3, 2, 1, 2, 1])  # Ajustando colunas para incluir botão
    cols[0].write("**Nome**")
    cols[1].write("**Agendado para**")
    cols[2].write("**Status**")
    cols[3].write("**Executado em**")
    cols[4].write("**Ações**")
    
    for job in st.session_state.job_list:
        col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 2, 1])
        col1.write(job['nome'])
        col2.write(job['data_agendada'])
        col3.write(job['status'])
        col4.write(job['data_execucao'] if job['data_execucao'] else '-')
        
        # Botão de exclusão
        # Só mostrar botão de exclusão para agendamentos pendentes
        if job['status'] == 'Pendente':
            if col5.button("🗑️", key=f"delete_{job['id']}"):
                excluir_agendamento(job['id'])
        else:
            col5.write("")
