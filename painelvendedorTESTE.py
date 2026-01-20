import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import pytz
import altair as alt
import time

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================
st.set_page_config(
    page_title="Painel Dox",
    page_icon="logodox.png",
    layout="wide"
)

FUSO_BR = pytz.timezone('America/Sao_Paulo')

# URLs das Planilhas
URL_SISTEMA = "https://docs.google.com/spreadsheets/d/1jODOp_SJUKWp1UaSmW_xJgkkyqDUexa56_P5QScAv3s/edit"
URL_PINHEIRAL = "https://docs.google.com/spreadsheets/d/1DxTnEEh9VgbFyjqxYafdJ0-puSAIHYhZ6lo5wZTKDeg/edit"
URL_BICAS = "https://docs.google.com/spreadsheets/d/1zKZK0fpYl-UtHcYmFkZJtOO17fTqBWaJ39V2UOukack/edit"

# Abas de Produção
ABAS_PINHEIRAL = ["Fagor", "Esquadros", "Marafon", "Divimec (Slitter)", "Divimec (Rebaixamento)"]
ABAS_BICAS = ["LCT Divimec", "LCT Ungerer", "LCL Divimec", "Divimec (RM)", "Servomaq", "Blanqueadeira", "Recorte", "Osciladora", "Maçarico"]

try:
    st.logo("logodox.png")
except:
    pass 

# ==============================================================================
# CONEXÃO GSPREAD (MOTOR OTIMIZADO)
# ==============================================================================

def conectar_google_sheets():
    """
    Cria uma conexão leve e direta com a API do Google.
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    try:
        # Tenta pegar dos segredos do Streamlit
        creds_dict = dict(st.secrets["gcp_service_account"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    except:
        # Fallback para arquivo local (caso esteja rodando no PC)
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    
    client = gspread.authorize(creds)
    return client

def ler_aba_gspread(client, url_planilha, nome_aba):
    """
    Lê uma aba específica e retorna um DataFrame.
    Usa 'get_all_values' que é mais robusto que 'get_all_records' para colunas vazias.
    """
    try:
        sheet = client.open_by_url(url_planilha)
        worksheet = sheet.worksheet(nome_aba)
        
        # Pega todos os dados como lista de listas
        dados = worksheet.get_all_values()
        
        if not dados:
            return pd.DataFrame()
            
        # A primeira linha é o cabeçalho
        header = dados[0]
        rows = dados[1:]
        
        df = pd.DataFrame(rows, columns=header)
        return df
    except Exception as e:
        # Se der erro (ex: aba não existe), retorna vazio sem quebrar o app
        return pd.DataFrame()

def escrever_aba_gspread(client, url_planilha, nome_aba, df_novo, modo="append"):
    """
    Função genérica para salvar dados.
    modo='append': Adiciona ao final.
    modo='overwrite': Apaga tudo e escreve de novo.
    """
    try:
        sheet = client.open_by_url(url_planilha)
        try:
            worksheet = sheet.worksheet(nome_aba)
        except:
            # Se não existir, cria (apenas se for overwrite, append precisa existir)
            worksheet = sheet.add_worksheet(title=nome_aba, rows=100, cols=20)

        if modo == "overwrite":
            worksheet.clear()
            # Prepara dados (incluindo header)
            dados_lista = [df_novo.columns.values.tolist()] + df_novo.values.tolist()
            worksheet.update(dados_lista, value_input_option="USER_ENTERED")
        else:
            # Append (sem header)
            dados_lista = df_novo.values.tolist()
            worksheet.append_rows(dados_lista, value_input_option="USER_ENTERED")
            
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

# ==============================================================================
# CACHING DE DADOS (CARREGAMENTO)
# ==============================================================================

# O client não pode ser cacheado, mas os DataFrames sim.
# Criamos uma função interna para obter o client fresco a cada execução de cache.

@st.cache_data(ttl="30m", show_spinner=False)
def carregar_usuarios():
    client = conectar_google_sheets()
    df = ler_aba_gspread(client, URL_SISTEMA, "Usuarios")
    return df

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_dados_faturamento_geral():
    # Carrega as 3 abas de faturamento de uma vez para otimizar cache
    client = conectar_google_sheets()
    
    df_direto = ler_aba_gspread(client, URL_SISTEMA, "Dados_Faturamento")
    df_transf = ler_aba_gspread(client, URL_SISTEMA, "Dados_Faturamento_Transf")
    df_vendedores = ler_aba_gspread(client, URL_SISTEMA, "Dados_Fat_Vendedores")
    df_metas = ler_aba_gspread(client, URL_SISTEMA, "Metas_Faturamento")
    
    # Tratamentos
    for df in [df_direto, df_transf, df_vendedores]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.upper()
            if 'TONS' in df.columns:
                df['TONS'] = df['TONS'].astype(str).str.replace(',', '.')
                df['TONS'] = pd.to_numeric(df['TONS'], errors='coerce').fillna(0)
            if 'DATA_EMISSAO' in df.columns:
                df['DATA_DT'] = pd.to_datetime(df['DATA_EMISSAO'], dayfirst=True, errors='coerce')
    
    if not df_metas.empty:
        df_metas.columns = df_metas.columns.str.strip().str.upper()
    
    return df_direto, df_transf, df_vendedores, df_metas

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_dados_producao_completo():
    client = conectar_google_sheets()
    df_prod = ler_aba_gspread(client, URL_SISTEMA, "Dados_Producao")
    df_metas = ler_aba_gspread(client, URL_SISTEMA, "Metas_Producao")
    
    if not df_prod.empty:
        df_prod.columns = df_prod.columns.str.strip().str.upper()
        if 'VOLUME' in df_prod.columns:
            df_prod['VOLUME'] = pd.to_numeric(df_prod['VOLUME'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        if 'DATA' in df_prod.columns:
            df_prod['DATA_DT'] = pd.to_datetime(df_prod['DATA'], dayfirst=True, errors='coerce')

    if not df_metas.empty:
        df_metas.columns = df_metas.columns.str.strip().str.upper()

    return df_prod, df_metas

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_geral():
    client = conectar_google_sheets()
    df_sol = ler_aba_gspread(client, URL_SISTEMA, "Solicitacoes")
    df_fotos = ler_aba_gspread(client, URL_SISTEMA, "Solicitacoes_Fotos")
    df_cert = ler_aba_gspread(client, URL_SISTEMA, "Solicitacoes_Certificados")
    df_notas = ler_aba_gspread(client, URL_SISTEMA, "Solicitacoes_Notas")
    df_logs = ler_aba_gspread(client, URL_SISTEMA, "Acessos")
    
    # Tratamento Logs
    if not df_logs.empty and "Data" in df_logs.columns:
        try:
            df_logs["Data_Dt"] = pd.to_datetime(df_logs["Data"], dayfirst=True, errors='coerce')
            df_logs = df_logs.sort_values(by="Data_Dt", ascending=False).drop(columns=["Data_Dt"])
        except: pass
        
    return df_sol, df_fotos, df_cert, df_notas, df_logs

@st.cache_data(ttl="15m", show_spinner=False)
def carregar_pedidos_multilojas():
    """
    Aqui está o maior gargalo. Lendo com gspread é mais rápido, mas ainda são muitas abas.
    Adicionamos um pequeno sleep para evitar erro 429 (Too Many Requests).
    """
    client = conectar_google_sheets()
    dados_consolidados = []
    
    # Função interna para processar lista de abas
    def processar_abas(url_filial, lista_abas, nome_filial):
        for aba in lista_abas:
            try:
                # Lê a aba
                df = ler_aba_gspread(client, url_filial, aba)
                if not df.empty:
                    df['Máquina/Processo'] = aba
                    df['Filial_Origem'] = nome_filial
                    
                    # Filtra colunas
                    cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
                    cols_existentes = [c for c in cols_necessarias if c in df.columns]
                    
                    if "Vendedor Correto" in cols_existentes:
                        df_limpo = df[cols_existentes + ['Máquina/Processo', 'Filial_Origem']].copy()
                        if "Número do Pedido" in df_limpo.columns:
                            # Limpeza de zeros e pontos
                            df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                        dados_consolidados.append(df_limpo)
                
                # Pausa estratégica para respirar a API
                time.sleep(0.5) 
                
            except Exception as e:
                continue

    processar_abas(URL_PINHEIRAL, ABAS_PINHEIRAL, "PINHEIRAL")
    processar_abas(URL_BICAS, ABAS_BICAS, "SJ BICAS")

    if dados_consolidados:
        return pd.concat(dados_consolidados, ignore_index=True)
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_credito_geral():
    client = conectar_google_sheets()
    df_credito = ler_aba_gspread(client, URL_SISTEMA, "Dados_Credito")
    df_carteira = ler_aba_gspread(client, URL_SISTEMA, "Dados_Carteira")
    df_titulos = ler_aba_gspread(client, URL_SISTEMA, "Dados_Titulos")
    
    for df in [df_credito, df_carteira, df_titulos]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.upper()
            
    return df_credito, df_carteira, df_titulos

# ==============================================================================
# FUNÇÕES DE AÇÃO (SALVAR DADOS)
# ==============================================================================

def registrar_acesso(login, nome):
    client = conectar_google_sheets()
    agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
    novo_log = pd.DataFrame([{"Data": agora_br, "Login": login, "Nome": nome}])
    escrever_aba_gspread(client, URL_SISTEMA, "Acessos", novo_log, modo="append")

def salvar_nova_solicitacao(nome, email, login, senha):
    client = conectar_google_sheets()
    agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
    nova_linha = pd.DataFrame([{"Nome": nome, "Email": email, "Login": login, "Senha": senha, "Data": agora_br, "Status": "Pendente"}])
    if escrever_aba_gspread(client, URL_SISTEMA, "Solicitacoes", nova_linha, modo="append"):
        carregar_solicitacoes_geral.clear()
        return True
    return False

def salvar_generico_solicitacao(aba, dados_dict):
    client = conectar_google_sheets()
    nova_linha = pd.DataFrame([dados_dict])
    if escrever_aba_gspread(client, URL_SISTEMA, aba, nova_linha, modo="append"):
        carregar_solicitacoes_geral.clear()
        return True
    return False

def salvar_metas(aba, df_novo):
    client = conectar_google_sheets()
    # Metas sobrescrevem a aba inteira
    return escrever_aba_gspread(client, URL_SISTEMA, aba, df_novo, modo="overwrite")

# ==============================================================================
# UI - INTERFACE (Adaptada para usar as novas funções)
# ==============================================================================

def formatar_moeda(valor):
    try:
        if isinstance(valor, str): valor = float(valor.replace('.', '').replace(',', '.'))
        if pd.isna(valor): return "R$ 0,00"
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return str(valor)

# SESSÃO
if 'logado' not in st.session_state:
    st.session_state['logado'] = False
    st.session_state['usuario_nome'] = ""
    st.session_state['usuario_filtro'] = ""
    st.session_state['usuario_email'] = "" 
    st.session_state['usuario_tipo'] = ""
if 'fazendo_cadastro' not in st.session_state: st.session_state['fazendo_cadastro'] = False

# --- TELA DE LOGIN ---
if not st.session_state['logado']:
    if st.session_state['fazendo_cadastro']:
        st.title("📝 Solicitação de Acesso")
        with st.form("form_cadastro"):
            nome = st.text_input("Nome Completo")
            email = st.text_input("E-mail")
            login = st.text_input("Crie um Login")
            senha = st.text_input("Crie uma Senha", type="password")
            if st.form_submit_button("Enviar Solicitação"):
                if salvar_nova_solicitacao(nome, email, login, senha):
                    st.success("Solicitação enviada!")
                    st.session_state['fazendo_cadastro'] = False
                    time.sleep(1)
                    st.rerun()
            if st.form_submit_button("Voltar"): st.session_state['fazendo_cadastro'] = False; st.rerun()
    else:
        st.title("🔒 Login - Painel Dox")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            u = st.text_input("Login").strip()
            s = st.text_input("Senha", type="password").strip()
            if st.button("Acessar", type="primary"):
                with st.spinner("Autenticando..."):
                    df = carregar_usuarios()
                    if not df.empty and 'Login' in df.columns and 'Senha' in df.columns:
                        user = df[(df['Login'].str.lower() == u.lower()) & (df['Senha'] == s)]
                        if not user.empty:
                            d = user.iloc[0]
                            st.session_state.update({
                                'logado': True, 
                                'usuario_nome': d['Nome Vendedor'].split()[0], 
                                'usuario_filtro': d['Nome Vendedor'], 
                                'usuario_email': d.get('Email', ''), 
                                'usuario_tipo': d['Tipo']
                            })
                            registrar_acesso(u, d['Nome Vendedor'])
                            st.rerun()
                        else: st.error("Login ou senha incorretos.")
                    else: st.error("Erro de conexão. Tente novamente.")
            if st.button("Solicitar Acesso"): st.session_state['fazendo_cadastro'] = True; st.rerun()

else:
    # --- ÁREA LOGADA ---
    with st.sidebar:
        st.write(f"Olá, **{st.session_state['usuario_nome']}**")
        if st.button("Sair"): 
            st.session_state.update({'logado': False})
            st.rerun()
        st.divider()
        if st.button("🔄 Forçar Atualização"): 
            st.cache_data.clear()
            st.rerun()

    # Carrega dados globais
    df_sol, df_fotos, df_cert, df_notas, df_logs = carregar_solicitacoes_geral()
    df_credito, df_carteira_cli, df_titulos = carregar_dados_credito_geral()
    
    # --- ABAS ---
    # Definição das abas conforme perfil
    if st.session_state['usuario_tipo'].lower() == "admin":
        tabs = st.tabs(["📂 Itens Programados", "💰 Crédito", "📷 Fotos RDQ", "📝 Acessos", "📑 Certificados", "🧾 Notas Fiscais", "🔍 Logs", "📊 Faturamento", "🏭 Produção"])
    elif st.session_state['usuario_tipo'].lower() == "master":
        tabs = st.tabs(["📂 Itens Programados", "💰 Crédito", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais", "📊 Faturamento", "🏭 Produção"])
    else:
        tabs = st.tabs(["📂 Itens Programados", "💰 Crédito", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais"])

    # --- ABA: CARTEIRA DE PEDIDOS ---
    with tabs[0]:
        df_total = carregar_pedidos_multilojas()
        if not df_total.empty:
            texto_busca = st.text_input("🔍 Buscar Pedido (Cliente, Produto...):")
            
            # Filtro por Filial e Vendedor (Lógica mantida do original)
            filtro_filial = st.selectbox("Filial:", ["Todas", "PINHEIRAL", "SJ BICAS"])
            if filtro_filial != "Todas": df_total = df_total[df_total["Filial_Origem"] == filtro_filial]
            
            # Filtro por Usuário
            tipo_user = st.session_state['usuario_tipo'].lower()
            if tipo_user not in ["admin", "master"]:
                 df_total = df_total[df_total["Vendedor Correto"].astype(str).str.contains(st.session_state['usuario_filtro'], case=False, na=False)]

            if texto_busca:
                mask = df_total.astype(str).apply(lambda x: x.str.contains(texto_busca, case=False, na=False)).any(axis=1)
                df_total = df_total[mask]

            st.dataframe(df_total, hide_index=True, use_container_width=True)
        else:
            st.info("Nenhum pedido encontrado ou carregando...")

    # --- ABA: CRÉDITO ---
    with tabs[1]:
        st.subheader("Painel de Crédito")
        if not df_credito.empty:
            busca_cred = st.text_input("🔍 Buscar Cliente:")
            df_show = df_credito.copy()
            if busca_cred:
                mask = df_show.astype(str).apply(lambda x: x.str.contains(busca_cred, case=False, na=False)).any(axis=1)
                df_show = df_show[mask]
            st.dataframe(df_show, hide_index=True, use_container_width=True)
        else: st.info("Dados de crédito indisponíveis.")

    # --- ABA: FOTOS ---
    idx_foto = 2
    with tabs[idx_foto]:
        st.subheader("Solicitação de Fotos")
        with st.form("form_foto"):
            lote = st.text_input("Lote / Bobina")
            email = st.text_input("E-mail", value=st.session_state['usuario_email'])
            if st.form_submit_button("Solicitar"):
                agora = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
                dados = {
                    "Data": agora, 
                    "Vendedor": st.session_state['usuario_nome'], 
                    "Email": email, 
                    "Lote": f"'{lote}", 
                    "Status": "Pendente"
                }
                if salvar_generico_solicitacao("Solicitacoes_Fotos", dados):
                    st.success("Solicitado!")

        if not df_fotos.empty:
            st.markdown("##### Histórico")
            st.dataframe(df_fotos, use_container_width=True)

    # --- ABA: CERTIFICADOS ---
    idx_cert = 3 if st.session_state['usuario_tipo'].lower() != "admin" else 4
    with tabs[idx_cert]:
        st.subheader("Solicitação de Certificados")
        with st.form("form_cert"):
            lote = st.text_input("Lote / Bobina")
            email = st.text_input("E-mail", value=st.session_state['usuario_email'])
            if st.form_submit_button("Solicitar"):
                agora = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
                dados = {
                    "Data": agora, "Vendedor": st.session_state['usuario_nome'], 
                    "Email": email, "Lote": f"'{lote}", "Status": "Pendente"
                }
                if salvar_generico_solicitacao("Solicitacoes_Certificados", dados): st.success("Solicitado!")
        
        if not df_cert.empty:
            st.dataframe(df_cert, use_container_width=True)

    # --- ABA: NOTAS ---
    idx_nota = 4 if st.session_state['usuario_tipo'].lower() != "admin" else 5
    with tabs[idx_nota]:
        st.subheader("Solicitação de NF")
        with st.form("form_nf"):
            nf = st.text_input("Número NF")
            filial = st.selectbox("Filial", ["PINHEIRAL", "SJ BICAS", "SF DO SUL", "SAO PAULO"])
            email = st.text_input("E-mail", value=st.session_state['usuario_email'])
            if st.form_submit_button("Solicitar"):
                agora = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
                dados = {
                    "Data": agora, "Vendedor": st.session_state['usuario_nome'], 
                    "Email": email, "NF": f"'{nf}", "Filial": filial, "Status": "Pendente"
                }
                if salvar_generico_solicitacao("Solicitacoes_Notas", dados): st.success("Solicitado!")
        
        if not df_notas.empty:
            st.dataframe(df_notas, use_container_width=True)

    # --- PERFIL ADMIN / MASTER (Faturamento e Produção) ---
    if st.session_state['usuario_tipo'].lower() in ["admin", "master"]:
        # Carrega dados específicos
        df_fat_dir, df_fat_transf, df_fat_vend, df_meta_fat = carregar_dados_faturamento_geral()
        df_prod, df_meta_prod = carregar_dados_producao_completo()

        # Índices variam se for Admin ou Master
        idx_fat = 7 if st.session_state['usuario_tipo'].lower() == "admin" else 5
        idx_prod = 8 if st.session_state['usuario_tipo'].lower() == "admin" else 6

        # ABA FATURAMENTO
        with tabs[idx_fat]:
            st.subheader("📊 Faturamento")
            if not df_fat_dir.empty:
                # Exemplo de gráfico simples
                st.write("Dados Carregados com Sucesso via gspread.")
                st.bar_chart(df_fat_dir, x="DATA_DT", y="TONS")
            else:
                st.warning("Sem dados de faturamento.")

        # ABA PRODUÇÃO
        with tabs[idx_prod]:
            st.subheader("🏭 Produção")
            if not df_prod.empty:
                st.dataframe(df_prod, use_container_width=True)
            else:
                st.warning("Sem dados de produção.")

    # --- PERFIL ADMIN (Acessos e Logs) ---
    if st.session_state['usuario_tipo'].lower() == "admin":
        with tabs[3]: # Acessos (solicitações pendentes)
            st.dataframe(df_sol, use_container_width=True)
        with tabs[6]: # Logs
            st.dataframe(df_logs, use_container_width=True)