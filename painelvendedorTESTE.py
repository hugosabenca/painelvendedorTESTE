import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import pytz
import altair as alt
import time
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

# ==============================================================================
# CONFIGURAÇÕES GERAIS E URLS
# ==============================================================================
st.set_page_config(
    page_title="Painel Dox",
    page_icon="logodox.png",
    layout="wide"
)

FUSO_BR = pytz.timezone('America/Sao_Paulo')

# --- USANDO URLS COMPLETAS ---
URL_SISTEMA = "https://docs.google.com/spreadsheets/d/1jODOp_SJUKWp1UaSmW_xJgkkyqDUexa56_P5QScAv3s/edit"
URL_PINHEIRAL = "https://docs.google.com/spreadsheets/d/1DxTnEEh9VgbFyjqxYafdJ0-puSAIHYhZ6lo5wZTKDeg/edit"
URL_BICAS = "https://docs.google.com/spreadsheets/d/1zKZK0fpYl-UtHcYmFkZJtOO17fTqBWaJ39V2UOukack/edit"

# --- LISTAS DE MÁQUINAS (ABAS) ---
ABAS_PINHEIRAL = ["Fagor", "Esquadros", "Marafon", "Divimec (Slitter)", "Divimec (Rebaixamento)"]
ABAS_BICAS = ["LCT Divimec", "LCT Ungerer", "LCL Divimec", "Divimec (RM)", "Servomaq", "Blanqueadeira", "Recorte", "Osciladora", "Maçarico"]

try:
    st.logo("logodox.png")
except Exception:
    pass 

# ==============================================================================
# CONEXÃO GSPREAD OTIMIZADA (SESSÃO ÚNICA)
# ==============================================================================

def get_gspread_client_cached():
    """
    Substitui a conectar_google_sheets antiga.
    Mantém a conexão viva no session_state para não reconectar a cada clique.
    """
    if 'gspread_client' not in st.session_state:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        try:
            # Tenta via Secrets (Cloud)
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        except:
            # Tenta via arquivo local
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        
        client = gspread.authorize(creds)
        st.session_state['gspread_client'] = client
        return client
    
    # Se já existe, verifica se precisa renovar o token
    client = st.session_state['gspread_client']
    if client.auth.expired:
        client.login()
    return client

# ==============================================================================
# LEITURA E ESCRITA (COM LÓGICA DE RETRY)
# ==============================================================================

def ler_com_retry(url, aba, tentativas=5, espera=1):
    """
    Lê dados com gspread usando o cliente cacheado.
    Retorna None se der erro crítico de conexão (para ativar a persistência).
    """
    client = get_gspread_client_cached()
    for i in range(tentativas):
        try:
            sheet = client.open_by_url(url)
            worksheet = sheet.worksheet(aba)
            data = worksheet.get_all_values()
            if data and len(data) > 0:
                return pd.DataFrame(data[1:], columns=data[0])
            else:
                return pd.DataFrame()
        except Exception as e:
            # Se for erro de cota (429), espera mais tempo
            if "429" in str(e) or "Quota exceeded" in str(e):
                time.sleep(espera * 2)
            else:
                time.sleep(espera)
            
            # Se falhou na última tentativa, retorna None para avisar que deu erro de conexão
            if i == tentativas - 1:
                return None
    return None

def escrever_no_sheets(url, aba, df_novo, modo="append"):
    try:
        client = get_gspread_client_cached()
        sheet = client.open_by_url(url)
        worksheet = sheet.worksheet(aba)
        if modo == "overwrite":
            worksheet.clear()
            dados = [df_novo.columns.values.tolist()] + df_novo.values.tolist()
            worksheet.update(dados, value_input_option="USER_ENTERED")
        else:
            dados = df_novo.values.tolist()
            worksheet.append_rows(dados, value_input_option="USER_ENTERED")
        return True
    except:
        return False

# ==============================================================================
# LÓGICA DE PERSISTÊNCIA SILENCIOSA (MODO TEIMOSO)
# ==============================================================================

def obter_dados_persistentes(chave_sessao, funcao_carregamento):
    """
    Tenta carregar dados novos.
    Se der erro de conexão (None), retorna SILENCIOSAMENTE os dados antigos da memória.
    Assim o usuário nunca vê tela em branco ou erro.
    """
    # 1. Garante que existe algo na memória (nem que seja vazio)
    if chave_sessao not in st.session_state:
        st.session_state[chave_sessao] = pd.DataFrame()
    
    # 2. Tenta buscar dados novos
    dados_novos = funcao_carregamento()
    
    # 3. Se a busca funcionou (não é None), atualiza a memória
    if dados_novos is not None:
        st.session_state[chave_sessao] = dados_novos
    
    # 4. Retorna o que tem na memória (seja novo ou velho)
    return st.session_state[chave_sessao]

# ==============================================================================
# FUNÇÕES DE FEEDBACK
# ==============================================================================

def ja_enviou_feedback(login):
    # Usa retry padrão, mas se falhar retorna False para não bloquear
    df = ler_com_retry(URL_SISTEMA, "Feedback_Vendedores", tentativas=3)
    if df is None or df.empty:
        return False
    
    if 'Login' in df.columns:
        logins_existentes = df['Login'].astype(str).str.strip().str.lower().tolist()
        return str(login).strip().lower() in logins_existentes
    return False

def salvar_feedback(login, nome, satisfacao, dispositivo, menos_usada, remover, sugestao):
    try:
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        df_novo = pd.DataFrame([{
            "Data": agora_br,
            "Login": login,
            "Nome": nome,
            "Satisfacao": satisfacao,
            "Dispositivo": dispositivo,
            "Aba_Menos_Usada": menos_usada,
            "Abas_Remover": remover,
            "Sugestao": sugestao
        }])
        return escrever_no_sheets(URL_SISTEMA, "Feedback_Vendedores", df_novo, modo="append")
    except:
        return False

# ==============================================================================
# FUNÇÕES DE FORMATAÇÃO E CORREÇÃO
# ==============================================================================

def converte_numero_seguro(valor):
    s = str(valor).strip()
    if not s or s.lower() == 'nan' or s.lower() == 'none': return 0.0
    if ',' in s:
        s = s.replace('.', '').replace(',', '.') 
    try:
        return float(s)
    except:
        return 0.0

def formatar_br_decimal(valor, casas=3):
    try:
        v = float(valor)
        s = "{:,.{}f}".format(v, casas)
        return s.replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return str(valor)

# ==============================================================================
# CARREGAMENTO DE DADOS (COM CACHE DO STREAMLIT)
# ==============================================================================

@st.cache_data(ttl="30m", show_spinner=False)
def carregar_usuarios():
    # Cache longo para login rápido
    df_users = ler_com_retry(URL_SISTEMA, "Usuarios", tentativas=10, espera=2)
    if df_users is not None and not df_users.empty: return df_users.astype(str)
    return pd.DataFrame()

@st.cache_data(ttl="10m", show_spinner=False)
def ler_dados_nuvem_generico(aba, url_planilha):
    df = ler_com_retry(url_planilha, aba)
    if df is None: return None # Retorna None para ativar a persistência
    if not df.empty:
        df.columns = df.columns.str.strip().str.upper()
        if 'TONS' in df.columns:
            df['TONS'] = df['TONS'].apply(converte_numero_seguro)
        if 'DATA_EMISSAO' in df.columns:
            df['DATA_DT'] = pd.to_datetime(df['DATA_EMISSAO'], dayfirst=True, errors='coerce')
        return df
    return pd.DataFrame()

def carregar_dados_faturamento_direto(): return ler_dados_nuvem_generico("Dados_Faturamento", URL_SISTEMA)
def carregar_dados_faturamento_transf(): return ler_dados_nuvem_generico("Dados_Faturamento_Transf", URL_SISTEMA)

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_faturamento_vendedores():
    df = ler_com_retry(URL_SISTEMA, "Dados_Fat_Vendedores")
    if df is None: return None
    if not df.empty:
        df.columns = df.columns.str.strip().str.upper()
        if 'TONS' in df.columns:
            df['TONS'] = df['TONS'].apply(converte_numero_seguro)
        if 'DATA_EMISSAO' in df.columns:
            df['DATA_DT'] = pd.to_datetime(df['DATA_EMISSAO'], dayfirst=True, errors='coerce')
        return df
    return pd.DataFrame()

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_estoque():
    df = ler_com_retry(URL_SISTEMA, "Dados_Estoque")
    if df is None: return None # Sinaliza erro para manter dados antigos
    if not df.empty:
        df.columns = df.columns.str.strip().str.upper()
        if 'DIAS.ESTOQUE' in df.columns:
            try:
                df['DATA_ENTRADA'] = pd.to_datetime(df['DIAS.ESTOQUE'], dayfirst=True, errors='coerce')
                agora = datetime.now()
                df['DIAS'] = (agora - df['DATA_ENTRADA']).dt.days
                df['DIAS'] = df['DIAS'].fillna(0).astype(int)
            except:
                df['DIAS'] = 0
        else:
            df['DIAS'] = 0
        cols_float = ['QTDE', 'EMPENHADO', 'DISPONIVEL', 'ESPES', 'LARGURA', 'COMPRIMENTO']
        for col in cols_float:
            if col in df.columns:
                df[col] = df[col].apply(converte_numero_seguro)
        if 'ESPES' in df.columns:
            df['ESPES'] = df['ESPES'] / 100.0
        return df
    return pd.DataFrame()

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_metas_faturamento():
    df = ler_com_retry(URL_SISTEMA, "Metas_Faturamento")
    if df is None: return pd.DataFrame(columns=['FILIAL', 'META'])
    if df.empty: return pd.DataFrame(columns=['FILIAL', 'META'])
    df.columns = df.columns.str.strip().str.upper()
    if 'META' in df.columns:
        df['META'] = df['META'].apply(converte_numero_seguro)
    return df

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_dados_producao_nuvem():
    df = ler_com_retry(URL_SISTEMA, "Dados_Producao")
    if df is None: return None
    if not df.empty:
        df.columns = df.columns.str.strip().str.upper()
        if 'VOLUME' in df.columns:
            df['VOLUME'] = df['VOLUME'].apply(converte_numero_seguro)
        if 'DATA' in df.columns:
            df['DATA_DT'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
        return df
    return pd.DataFrame()

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_metas_producao():
    df = ler_com_retry(URL_SISTEMA, "Metas_Producao")
    if df is None or df.empty: return pd.DataFrame(columns=['MAQUINA', 'META'])
    df.columns = df.columns.str.strip().str.upper()
    if 'META' in df.columns:
        df['META'] = df['META'].apply(converte_numero_seguro)
    return df

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes():
    df = ler_com_retry(URL_SISTEMA, "Solicitacoes")
    if df is None or df.empty: return pd.DataFrame(columns=["Nome", "Email", "Login", "Senha", "Data", "Status"])
    return df

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_fotos():
    df = ler_com_retry(URL_SISTEMA, "Solicitacoes_Fotos")
    if df is None: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
    if not df.empty:
        cols_map = {c: c.strip() for c in df.columns}
        df = df.rename(columns=cols_map)
        if "Lote" in df.columns: df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_certificados():
    df = ler_com_retry(URL_SISTEMA, "Solicitacoes_Certificados")
    if df is None: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
    if not df.empty:
        cols_map = {c: c.strip() for c in df.columns}
        df = df.rename(columns=cols_map)
        if "Lote" in df.columns: df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_notas():
    df = ler_com_retry(URL_SISTEMA, "Solicitacoes_Notas")
    if df is None: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])
    if not df.empty:
        cols_map = {c: c.strip() for c in df.columns}
        df = df.rename(columns=cols_map)
        if "NF" in df.columns: df["NF"] = df["NF"].astype(str).str.replace("'", "") 
        return df
    return pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_logs_acessos():
    df = ler_com_retry(URL_SISTEMA, "Acessos")
    if df is None: return pd.DataFrame(columns=["Data", "Login", "Nome"])
    if not df.empty:
        df.columns = df.columns.str.strip()
        if "Data" in df.columns:
            try:
                df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors='coerce')
                df = df.sort_values(by="Data_Dt", ascending=False).drop(columns=["Data_Dt"])
            except: pass
        return df
    return pd.DataFrame(columns=["Data", "Login", "Nome"])

@st.cache_data(ttl="15m", show_spinner=False)
def carregar_dados_pedidos():
    dados_consolidados = []
    # Pinheiral
    for aba in ABAS_PINHEIRAL:
        df = ler_com_retry(URL_PINHEIRAL, aba, tentativas=2)
        if df is not None and not df.empty:
            df = df.astype(str)
            df['Máquina/Processo'] = aba
            df['Filial_Origem'] = "PINHEIRAL"
            cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
            cols_existentes = [c for c in cols_necessarias if c in df.columns]
            if "Vendedor Correto" in cols_existentes:
                df_limpo = df[cols_existentes + ['Máquina/Processo', 'Filial_Origem']].copy()
                if "Número do Pedido" in df_limpo.columns:
                    df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                dados_consolidados.append(df_limpo)
        time.sleep(0.5)
    # Bicas
    for aba in ABAS_BICAS:
        df = ler_com_retry(URL_BICAS, aba, tentativas=2)
        if df is not None and not df.empty:
            df = df.astype(str)
            df['Máquina/Processo'] = aba
            df['Filial_Origem'] = "SJ BICAS"
            cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
            cols_existentes = [c for c in cols_necessarias if c in df.columns]
            if "Vendedor Correto" in cols_existentes:
                df_limpo = df[cols_existentes + ['Máquina/Processo', 'Filial_Origem']].copy()
                if "Número do Pedido" in df_limpo.columns:
                    df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                dados_consolidados.append(df_limpo)
        time.sleep(0.5)

    if dados_consolidados: return pd.concat(dados_consolidados, ignore_index=True)
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_credito():
    df = ler_com_retry(URL_SISTEMA, "Dados_Credito")
    if df is None: return None
    if not df.empty:
        df = df.astype(str)
        df.columns = df.columns.str.strip().str.upper()
        return df
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_carteira():
    df = ler_com_retry(URL_SISTEMA, "Dados_Carteira")
    if df is None: return None
    if not df.empty:
        df = df.astype(str)
        df.columns = df.columns.str.strip().str.upper()
        return df
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_titulos():
    df = ler_com_retry(URL_SISTEMA, "Dados_Titulos")
    if df is None: return None
    if not df.empty:
        df = df.astype(str)
        df.columns = df.columns.str.strip().str.upper()
        return df
    return pd.DataFrame()

# ==============================================================================
# FUNÇÕES DE ESCRITA
# ==============================================================================

def salvar_metas_faturamento(dicionario_metas):
    try:
        df_novo = pd.DataFrame(list(dicionario_metas.items()), columns=['FILIAL', 'META'])
        return escrever_no_sheets(URL_SISTEMA, "Metas_Faturamento", df_novo, modo="overwrite")
    except: return False

def salvar_metas_producao(dicionario_metas):
    try:
        df_novo = pd.DataFrame(list(dicionario_metas.items()), columns=['MAQUINA', 'META'])
        return escrever_no_sheets(URL_SISTEMA, "Metas_Producao", df_novo, modo="overwrite")
    except: return False

def registrar_acesso(login, nome):
    try:
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        novo_log = pd.DataFrame([{"Data": agora_br, "Login": login, "Nome": nome}])
        escrever_no_sheets(URL_SISTEMA, "Acessos", novo_log, modo="append")
    except: pass

def salvar_nova_solicitacao(nome, email, login, senha):
    try:
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Nome": nome, "Email": email, "Login": login, "Senha": senha, "Data": agora_br, "Status": "Pendente"}])
        if escrever_no_sheets(URL_SISTEMA, "Solicitacoes", nova_linha, modo="append"):
            carregar_solicitacoes.clear()
            return True
        return False
    except: return False

def salvar_solicitacao_foto(vendedor_nome, vendedor_email, lote):
    try:
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        if escrever_no_sheets(URL_SISTEMA, "Solicitacoes_Fotos", nova_linha, modo="append"):
            carregar_solicitacoes_fotos.clear()
            return True
        return False
    except: return False

def salvar_solicitacao_certificado(vendedor_nome, vendedor_email, lote):
    try:
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        if escrever_no_sheets(URL_SISTEMA, "Solicitacoes_Certificados", nova_linha, modo="append"):
            carregar_solicitacoes_certificados.clear()
            return True
        return False
    except: return False

def salvar_solicitacao_nota(vendedor_nome, vendedor_email, nf_numero, filial):
    try:
        nf_str = f"'{nf_numero}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "NF": nf_str, "Filial": filial, "Status": "Pendente"}])
        if escrever_no_sheets(URL_SISTEMA, "Solicitacoes_Notas", nova_linha, modo="append"):
            carregar_solicitacoes_notas.clear()
            return True
        return False
    except: return False

def formatar_peso_brasileiro(valor):
    try:
        if pd.isna(valor) or valor == "": return "0"
        texto = f"{float(valor):.3f}"
        texto = texto.replace('.', ',').rstrip('0').rstrip(',')
        return texto
    except: return str(valor)

def formatar_moeda(valor):
    try:
        if isinstance(valor, str): valor = float(valor.replace('.', '').replace(',', '.'))
        if pd.isna(valor): return "R$ 0,00"
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return str(valor)

# ==============================================================================
# UI
# ==============================================================================

def plotar_grafico_faturamento(df_filtrado, titulo_grafico, meta_valor=None):
    if df_filtrado.empty:
        st.warning(f"Sem dados para {titulo_grafico} neste período.")
        return
    def fmt_br(val): return f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    hoje_normalizado = datetime.now(FUSO_BR).replace(hour=0, minute=0, second=0, microsecond=0)
    df_hoje = df_filtrado[df_filtrado['DATA_DT'].dt.date == hoje_normalizado.date()]
    val_hoje = df_hoje['TONS'].sum()
    txt_hoje = f"**Hoje ({hoje_normalizado.strftime('%d/%m')}):** {fmt_br(val_hoje)} Ton"
    df_last = df_filtrado[(df_filtrado['TONS'] > 0) & (df_filtrado['DATA_DT'].dt.date < hoje_normalizado.date())].sort_values('DATA_DT', ascending=False)
    if not df_last.empty:
        last_date = df_last['DATA_DT'].max()
        last_val = df_last[df_last['DATA_DT'] == last_date]['TONS'].sum()
        txt_last = f"**Último Faturamento ({last_date.strftime('%d/%m')}):** {fmt_br(last_val)} Ton"
    else: txt_last = "**Último Faturamento:** -"
    st.markdown(f"### {titulo_grafico}")
    st.markdown(f"{txt_hoje} | {txt_last}")
    df_chart = df_filtrado.copy()
    df_chart['DATA_STR'] = df_chart['DATA_DT'].dt.strftime('%d/%m/%Y')
    df_chart['TONS_TXT'] = df_chart['TONS'].apply(lambda x: f"{x:.1f}".replace('.', ','))
    base = alt.Chart(df_chart).encode(x=alt.X('DATA_STR', title=None, sort=None, axis=alt.Axis(labelAngle=0)))
    barras = base.mark_bar(color='#0078D4', size=40).encode(y=alt.Y('TONS', title='Toneladas'), tooltip=['DATA_STR', 'TONS'])
    rotulos = base.mark_text(dy=-10, color='black').encode(y=alt.Y('TONS'), text=alt.Text('TONS_TXT'))
    grafico = (barras + rotulos)
    if meta_valor is not None and meta_valor > 0:
        regra_meta = alt.Chart(pd.DataFrame({'y': [meta_valor]})).mark_rule(color='red', strokeDash=[5, 5]).encode(y='y', size=alt.value(2))
        texto_meta = alt.Chart(pd.DataFrame({'y': [meta_valor]})).mark_text(align='left', baseline='bottom', color='red', dx=5).encode(y='y', text=alt.value(f"Meta: {meta_valor}"))
        grafico = (grafico + regra_meta + texto_meta)
    grafico = grafico.properties(height=400)
    st.altair_chart(grafico, use_container_width=True)
    st.divider()

def exibir_aba_faturamento():
    st.subheader("📊 Painel de Faturamento")
    if st.button("🔄 Atualizar Gráfico"):
        with st.spinner("Buscando dados sincronizados..."):
            carregar_dados_faturamento_direto.clear()
            carregar_dados_faturamento_transf.clear()
            st.session_state['dados_faturamento'] = carregar_dados_faturamento_direto()
            st.session_state['dados_faturamento_transf'] = carregar_dados_faturamento_transf()
            st.session_state['metas_faturamento'] = carregar_metas_faturamento()
            
    # USO DA FUNÇÃO BLINDADA (PERSISTÊNCIA)
    df_direto = obter_dados_persistentes("cache_fat_direto", carregar_dados_faturamento_direto)
    df_transf = obter_dados_persistentes("cache_fat_transf", carregar_dados_faturamento_transf)
    df_meta = obter_dados_persistentes("cache_fat_meta", carregar_metas_faturamento)
    
    with st.expander("⚙️ Definir Meta (tons)"):
        with st.form("form_metas_fat"):
            st.caption("Defina a meta diária de faturamento para PINHEIRAL (Direto).")
            novas_metas = {}
            valor_atual = 0.0
            if not df_meta.empty:
                filtro = df_meta[df_meta['FILIAL'] == 'PINHEIRAL']
                if not filtro.empty: valor_atual = float(filtro.iloc[0]['META'])
            novas_metas['PINHEIRAL'] = st.number_input("PINHEIRAL", value=valor_atual, step=1.0, min_value=0.0)
            if st.form_submit_button("💾 Salvar Metas"):
                if salvar_metas_faturamento(novas_metas):
                    st.success("Meta atualizada!")
                    carregar_metas_faturamento.clear()
                    st.rerun()
    st.divider()
    periodo = st.radio("Selecione o Período:", ["Últimos 7 Dias", "Acumulado Mês Corrente"], horizontal=True, key="fat_periodo")
    hoje_normalizado = datetime.now(FUSO_BR).replace(hour=0, minute=0, second=0, microsecond=0)
    if periodo == "Últimos 7 Dias": data_limite = hoje_normalizado - timedelta(days=6)
    else: data_limite = hoje_normalizado.replace(day=1)
    
    if not df_direto.empty:
        df_filtro_direto = df_direto[df_direto['DATA_DT'].dt.date >= data_limite.date()]
        meta_direto = 0
        if not df_meta.empty:
            fmeta = df_meta[df_meta['FILIAL'] == 'PINHEIRAL']
            if not fmeta.empty: meta_direto = float(fmeta.iloc[0]['META'])
        plotar_grafico_faturamento(df_filtro_direto, "Faturamento Direto: Pinheiral", meta_direto)
    else: st.info("Sem dados de Faturamento Direto carregados.")
    
    if not df_transf.empty:
        df_filtro_transf = df_transf[df_transf['DATA_DT'].dt.date >= data_limite.date()]
        plotar_grafico_faturamento(df_filtro_transf, "Faturamento Transferência: Pinheiral", meta_valor=None) 
    else: st.info("Sem dados de Transferência carregados.")

def exibir_aba_producao():
    st.subheader("🏭 Painel de Produção (Pinheiral)")
    if st.button("🔄 Atualizar Produção"):
        with st.spinner("Carregando indicadores..."):
            carregar_dados_producao_nuvem.clear() 
            st.rerun()
            
    # USO DA FUNÇÃO BLINDADA (PERSISTÊNCIA)
    df = obter_dados_persistentes("cache_producao_dados", carregar_dados_producao_nuvem)
    df_metas = obter_dados_persistentes("cache_producao_metas", carregar_metas_producao)

    with st.expander("⚙️ Definir Metas Diárias (Tons)"):
        if not df.empty:
            lista_maquinas = sorted(df['MAQUINA'].unique())
        else: lista_maquinas = ["Divimec 1", "Divimec 2", "Endireitadeira", "Esquadros", "Fagor", "Marafon"]
        with st.form("form_metas"):
            st.caption("Defina a meta diária (Tons) para cada máquina.")
            novas_metas = {}
            cols = st.columns(3)
            for i, mq in enumerate(lista_maquinas):
                valor_atual = 0.0
                if not df_metas.empty:
                    filtro = df_metas[df_metas['MAQUINA'] == mq]
                    if not filtro.empty: valor_atual = float(filtro.iloc[0]['META'])
                with cols[i % 3]: novas_metas[mq] = st.number_input(f"{mq}", value=valor_atual, step=1.0, min_value=0.0)
            if st.form_submit_button("💾 Salvar Metas"):
                if salvar_metas_producao(novas_metas): 
                    st.success("Metas atualizadas!")
                    carregar_metas_producao.clear()
                    st.rerun()
    st.divider()
    if not df.empty:
        periodo = st.radio("Selecione o Período:", ["Últimos 7 Dias", "Acumulado Mês Corrente"], horizontal=True, key="prod_periodo")
        hoje_normalizado = datetime.now(FUSO_BR).replace(hour=0, minute=0, second=0, microsecond=0)
        if periodo == "Últimos 7 Dias": data_limite = hoje_normalizado - timedelta(days=6) 
        else: data_limite = hoje_normalizado.replace(day=1)
        df_filtro = df[df['DATA_DT'].dt.date >= data_limite.date()]
        if df_filtro.empty: 
            st.warning("Nenhum dado encontrado para este período.")
            return
        total_prod = df_filtro['VOLUME'].sum()
        dias_unicos = df_filtro['DATA_DT'].nunique()
        media_diaria = total_prod / dias_unicos if dias_unicos > 0 else 0
        k1, k2 = st.columns(2)
        k1.metric("Total Produzido", f"{total_prod:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " Ton")
        k2.metric("Média Diária", f"{media_diaria:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " Ton")
        st.divider()
        maquinas = sorted(df_filtro['MAQUINA'].unique())
        def fmt_br(val): return f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        for mq in maquinas:
            df_mq = df_filtro[df_filtro['MAQUINA'] == mq].copy()
            st.markdown(f"### Produção: {mq}")
            df_hoje = df_mq[df_mq['DATA_DT'].dt.date == hoje_normalizado.date()]
            hoje_a = df_hoje[df_hoje['TURNO'] == 'Turno A']['VOLUME'].sum()
            hoje_c = df_hoje[df_hoje['TURNO'] == 'Turno C']['VOLUME'].sum()
            hoje_total = hoje_a + hoje_c
            texto_hoje = f"**Hoje ({hoje_normalizado.strftime('%d/%m')}):** Turno A: {fmt_br(hoje_a)} | Turno C: {fmt_br(hoje_c)} | **Total: {fmt_br(hoje_total)}**"
            df_hist = df_mq[(df_mq['VOLUME'] > 0) & (df_mq['DATA_DT'].dt.date < hoje_normalizado.date())]
            if not df_hist.empty:
                last_date = df_hist['DATA_DT'].max()
                df_last = df_hist[df_hist['DATA_DT'] == last_date]
                last_a = df_last[df_last['TURNO'] == 'Turno A']['VOLUME'].sum()
                last_c = df_last[df_last['TURNO'] == 'Turno C']['VOLUME'].sum()
                last_total = last_a + last_c
                texto_last = f"**Última Produção ({last_date.strftime('%d/%m')}):** Turno A: {fmt_br(last_a)} | Turno C: {fmt_br(last_c)} | **Total: {fmt_br(last_total)}**"
            else: texto_last = "**Última Produção:** -"
            st.markdown(texto_hoje); st.markdown(texto_last)
            df_mq['VOLUME_TXT'] = df_mq['VOLUME'].apply(lambda x: f"{x:.1f}".replace('.', ','))
            meta_valor = 0
            if not df_metas.empty:
                filtro_meta = df_metas[df_metas['MAQUINA'] == mq]
                if not filtro_meta.empty: meta_valor = float(filtro_meta.iloc[0]['META'])
            base = alt.Chart(df_mq).encode(x=alt.X('DATA', title=None, axis=alt.Axis(labelAngle=0)))
            barras = base.mark_bar().encode(xOffset='TURNO', y=alt.Y('VOLUME', title='Tons'), color=alt.Color('TURNO', legend=alt.Legend(title="Turno", orient='top')), tooltip=['DATA', 'TURNO', 'VOLUME'])
            rotulos = base.mark_text(dy=-10, color='black').encode(xOffset='TURNO', y=alt.Y('VOLUME'), text=alt.Text('VOLUME_TXT'))
            regra_meta = alt.Chart(pd.DataFrame({'y': [meta_valor]})).mark_rule(color='red', strokeDash=[5, 5]).encode(y='y', size=alt.value(2))
            texto_meta = alt.Chart(pd.DataFrame({'y': [meta_valor]})).mark_text(align='left', baseline='bottom', color='red', dx=5).encode(y='y', text=alt.value(f"Meta: {meta_valor}"))
            grafico_final = (barras + rotulos + regra_meta + texto_meta).properties(height=350)
            st.altair_chart(grafico_final, use_container_width=True)
            st.markdown("---")
    elif 'dados_producao' in st.session_state and st.session_state['dados_producao'].empty:
        st.warning("Nenhum dado na planilha de produção.")
    else: st.info("Clique no botão para carregar.")

def exibir_aba_estoque():
    st.subheader("📦 Consulta de Estoque Disponível")
    
    col_btn, _ = st.columns([1, 4])
    with col_btn:
        if st.button("🔄 Atualizar Estoque"):
            carregar_estoque.clear()
            st.rerun()
            
    # USO DA FUNÇÃO BLINDADA (PERSISTÊNCIA)
    df_estoque = obter_dados_persistentes("cache_estoque", carregar_estoque)
    
    if df_estoque.empty:
        st.info("Nenhum dado de estoque carregado.")
        return

    # FILTROS
    lista_filiais = ["Todas"] + sorted(df_estoque['FILIAL'].unique().tolist())
    c1, c2 = st.columns(2)
    with c1: filial_sel = st.selectbox("Filtrar por Filial:", lista_filiais)
    with c2: busca = st.text_input("Buscar (aperte enter após digitar):")

    somente_disp = st.checkbox("Somente Disponível")
    st.caption("(marque para mostrar somente itens que possuem saldo disponível maior que zero)")

    df_filtrado = df_estoque.copy()
    if somente_disp:
        if 'DISPONIVEL' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['DISPONIVEL'] > 0.001]

    if filial_sel != "Todas":
        df_filtrado = df_filtrado[df_filtrado['FILIAL'] == filial_sel]
        
    if busca:
        mask = df_filtrado.astype(str).apply(lambda x: x.str.contains(busca, case=False, na=False)).any(axis=1)
        df_filtrado = df_filtrado[mask]

    st.markdown(f"**Itens encontrados:** {len(df_filtrado)}")
    
    df_show = df_filtrado.copy()
    mapa_renomeacao = {'PRODUTO': 'DESCRIÇÃO DO PRODUTO', 'ARMAZEM': 'ARM', 'LARGURA': 'LARG', 'COMPRIMENTO': 'COMP', 'EMPENHADO': 'EMP', 'DISPONIVEL': 'DISP'}
    df_show.rename(columns=mapa_renomeacao, inplace=True)

    if 'ESPES' in df_show.columns: df_show['ESPES'] = df_show['ESPES'].apply(lambda x: formatar_br_decimal(x, 2))
    cols_qtd = ['QTDE', 'EMP', 'DISP']
    for col in cols_qtd:
        if col in df_show.columns: df_show[col] = df_show[col].apply(lambda x: formatar_br_decimal(x, 3))
    if 'COMP' in df_show.columns: df_show['COMP'] = df_show['COMP'].apply(lambda x: str(int(x)) if x > 0 else "0")

    colunas_desejadas = ["FILIAL", "ARM", "DESCRIÇÃO DO PRODUTO", "LOTE", "ESPES", "LARG", "COMP", "QTDE", "EMP", "DISP"]
    cols_finais = [c for c in colunas_desejadas if c in df_show.columns]
    
    gb = GridOptionsBuilder.from_dataframe(df_show[cols_finais])
    gb.configure_default_column(resizable=True, filterable=True, sortable=True, cellStyle={'textAlign': 'center'}, suppressSizeToFit=False)
    gb.configure_grid_options(floatingFilter=True) 
    gb.configure_column("DESCRIÇÃO DO PRODUTO", minWidth=380, flex=1, cellStyle={'textAlign': 'left'}) 
    gb.configure_column("FILIAL", width=110, minWidth=90)
    gb.configure_column("ARM", width=60, minWidth=50, maxWidth=70)
    gb.configure_column("LOTE", width=110, minWidth=90)
    gb.configure_column("ESPES", width=70, minWidth=60, maxWidth=80)
    gb.configure_column("LARG", width=70, minWidth=60, maxWidth=80)
    gb.configure_column("COMP", width=70, minWidth=60, maxWidth=80)
    gb.configure_column("QTDE", width=80, minWidth=70, maxWidth=100)
    gb.configure_column("EMP", width=80, minWidth=70, maxWidth=100)
    gb.configure_column("DISP", width=90, minWidth=80, maxWidth=110, cellStyle={'fontWeight': 'bold', 'textAlign': 'center', 'color': '#000080'})
    gb.configure_selection('single', use_checkbox=False)
    gridOptions = gb.build()
    
    AgGrid(df_show[cols_finais], gridOptions=gridOptions, height=500, width='100%', fit_columns_on_grid_load=True, theme='streamlit', update_mode=GridUpdateMode.SELECTION_CHANGED, allow_unsafe_jscode=True)

def exibir_carteira_pedidos():
    tipo_usuario = st.session_state['usuario_tipo'].lower()
    
    # USO DA FUNÇÃO BLINDADA (PERSISTÊNCIA)
    df_total = obter_dados_persistentes("cache_pedidos", carregar_dados_pedidos)

    if not df_total.empty:
        df_total = df_total.dropna(subset=["Número do Pedido"])
        df_total = df_total[~df_total["Número do Pedido"].isin(["000nan", "00None", "000000"])]
        filtro_filial = st.selectbox("Selecione a Filial:", ["Todas", "PINHEIRAL", "SJ BICAS"])
        if filtro_filial != "Todas":
            df_total = df_total[df_total["Filial_Origem"] == filtro_filial]
        nome_filtro = st.session_state['usuario_filtro']
        if tipo_usuario in ["admin", "gerente", "master"]:
            vendedores_unicos = sorted(df_total["Vendedor Correto"].dropna().unique())
            filtro_vendedor = st.selectbox(f"Filtrar Vendedor ({tipo_usuario.capitalize()})", ["Todos"] + vendedores_unicos)
            if filtro_vendedor != "Todos": df_filtrado = df_total[df_total["Vendedor Correto"] == filtro_vendedor].copy()
            else: df_filtrado = df_total.copy()
        elif tipo_usuario == "gerente comercial":
            if "Gerente Correto" in df_total.columns: df_filtrado = df_total[df_total["Gerente Correto"].str.lower() == nome_filtro.lower()].copy()
            else: df_filtrado = pd.DataFrame()
        else: 
            df_filtrado = df_total[df_total["Vendedor Correto"].str.lower().str.contains(nome_filtro.lower(), regex=False, na=False)].copy()
        if df_filtrado.empty: st.info(f"Nenhum pedido pendente encontrado para a filial selecionada.")
        else:
            df_filtrado['Quantidade_Num'] = df_filtrado['Quantidade'].apply(converte_numero_seguro)
            df_filtrado['Peso (ton)'] = df_filtrado['Quantidade_Num'].apply(formatar_peso_brasileiro)
            try:
                df_filtrado['Prazo_dt'] = pd.to_datetime(df_filtrado['Prazo'], dayfirst=True, errors='coerce')
                df_filtrado['Prazo'] = df_filtrado['Prazo_dt'].dt.strftime('%d/%m/%Y').fillna("-")
            except: pass
            colunas_visiveis = ["Número do Pedido", "Filial_Origem", "Cliente Correto", "Produto", "Peso (ton)", "Prazo", "Máquina/Processo"]
            if tipo_usuario in ["admin", "gerente", "gerente comercial", "master"]: 
                colunas_visiveis.insert(6, "Vendedor Correto")
                if "Gerente Correto" in df_total.columns:
                    colunas_visiveis.insert(7, "Gerente Correto")
            colunas_finais = [c for c in colunas_visiveis if c in df_filtrado.columns]
            df_final = df_filtrado[colunas_finais]
            total_pedidos = len(df_filtrado)
            total_peso = df_filtrado['Quantidade_Num'].sum()
            total_peso_str = f"{total_peso:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            kpi1, kpi2 = st.columns(2)
            kpi1.metric("Itens Programados:", total_pedidos)
            kpi2.metric("Volume Total (Tons):", total_peso_str)
            st.divider()
            texto_busca = st.text_input("🔍 Filtro (Cliente, Pedido, Produto...):")
            if texto_busca:
                mask = df_final.astype(str).apply(lambda x: x.str.contains(texto_busca, case=False, na=False)).any(axis=1)
                df_exibicao = df_final[mask]
            else: df_exibicao = df_final
            st.dataframe(df_exibicao, hide_index=True, use_container_width=True, column_config={"Prazo": st.column_config.TextColumn("Previsão"), "Filial_Origem": st.column_config.TextColumn("Filial")})
            if texto_busca and df_exibicao.empty: st.warning(f"Nenhum resultado encontrado para '{texto_busca}'")
    else: st.error("Não foi possível carregar a planilha de pedidos. Tente atualizar a página.")

@st.dialog("Detalhes Financeiros", width="large")
def mostrar_detalhes_titulos(cliente_nome, df_titulos):
    st.markdown(f"### 🏢 {cliente_nome}")
    st.caption("Abaixo a lista de títulos em aberto (vencidos e a vencer) para este cliente.")
    if df_titulos.empty:
        st.warning("Não há títulos pendentes registrados para este CNPJ.")
    else:
        df_show = df_titulos.copy()
        if "VALOR" in df_show.columns: df_show["VALOR"] = df_show["VALOR"].apply(formatar_moeda)
        if "SALDO" in df_show.columns: df_show["SALDO"] = df_show["SALDO"].apply(formatar_moeda)
        cols_visual = ["DATA_EMISSAO", "NOTA_FISCAL", "PARCELA", "VALOR", "SALDO", "VENCIMENTO", "STATUS_RESUMO", "STATUS_DETALHADO", "TIPO_DE_FATURAMENTO"]
        cols_finais = [c for c in cols_visual if c in df_show.columns]
        st.dataframe(df_show[cols_finais], hide_index=True, use_container_width=True, column_config={"NOTA_FISCAL": st.column_config.TextColumn("NF"), "DATA_EMISSAO": st.column_config.TextColumn("Emissão"), "STATUS_RESUMO": st.column_config.TextColumn("Status"), "STATUS_DETALHADO": st.column_config.TextColumn("Detalhe Vencimento"), "TIPO_DE_FATURAMENTO": st.column_config.TextColumn("Tipo Fat.")})

def exibir_aba_credito():
    st.markdown("### 💰 Painel de Crédito <small style='font-weight: normal; font-size: 14px; color: gray;'>(Aba em teste. Qualquer divergência, por favor reporte.)</small>", unsafe_allow_html=True)
    with st.expander("ℹ️ Legenda: Entenda o significado de cada coluna (Clique para expandir)"):
        st.markdown("""
        **CLIENTE**: Nome do cliente cadastrado na empresa.
        **RISCO_DE_BLOQUEIO**: Indica o nível de risco (ALTO/MÉDIO/BAIXO).
        **ACAO_SUGERIDA**: Orientação clara (cobrar, aguardar, etc.).
        **OPCAO_DE_FATURAMENTO**: Modalidade (LC DOX / BV).
        **SITUACAO_LC**: Status do convênio (Liberado, Bloqueado, etc.).
        """)

    # USO DA FUNÇÃO BLINDADA (PERSISTÊNCIA)
    df_credito = obter_dados_persistentes("cache_credito", carregar_dados_credito)
    df_carteira = obter_dados_persistentes("cache_carteira_cred", carregar_dados_carteira)
    df_titulos_geral = obter_dados_persistentes("cache_titulos", carregar_dados_titulos) 
    
    if df_credito.empty:
        st.info("Nenhuma informação de crédito disponível no momento (Aguardando sincronização do Robô).")
        return

    cols_order = ["CNPJ", "CLIENTE", "VENDEDOR", "GERENTE", "RISCO_DE_BLOQUEIO", "ACAO_SUGERIDA", "MOTIVO_PROVAVEL_DO_BLOQUEIO", "OPCAO_DE_FATURAMENTO", "RECEBIVEIS", "DIAS_EM_ATRASO_RECEBIVEIS", "SALDO_VENCIDO", "VENCIMENTO LC", "DIAS_PARA_VENCER_LC", "DATA_VENC_LC", "DISPONIVEL VIA LC2", "DISPONIVEL BV", "DISPONIVEL VIA RA", "SALDO_A_VENCER", "DIAS_PARA_VENCER_TITULO", "DATA_VENCIMENTO_MAIS_ANTIGA", "LC DOX", "LC BV", "LC TOTAL", "RA", "EM_ABERTO", "EM ABERTO BV", "LC SUPPLIER", "SUPPLIER DISP", "SITUACAO LC"]
    cols_financeiras = ["SALDO_VENCIDO", "SALDO_A_VENCER", "LC TOTAL", "LC DOX", "RA", "EM_ABERTO", "DISPONIVEL VIA RA", "DISPONIVEL VIA LC2", "LC BV", "EM ABERTO BV", "DISPONIVEL BV", "LC SUPPLIER", "SUPPLIER DISP"]

    tipo_usuario = st.session_state['usuario_tipo'].lower()
    nome_usuario = st.session_state['usuario_filtro']
    nome_usuario_limpo = nome_usuario.strip().lower()

    if tipo_usuario in ["admin", "master", "gerente"]: df_base = df_credito.copy()
    elif tipo_usuario == "gerente comercial":
        if "GERENTE" in df_credito.columns:
            df_credito["GERENTE_CLEAN"] = df_credito["GERENTE"].astype(str).str.strip().str.lower()
            df_base = df_credito[df_credito["GERENTE_CLEAN"].str.contains(nome_usuario_limpo, na=False)].copy()
        else: df_base = pd.DataFrame()
    else:
        if "VENDEDOR" in df_credito.columns:
            df_credito["VENDEDOR_CLEAN"] = df_credito["VENDEDOR"].astype(str).str.strip().str.lower()
            df_base = df_credito[df_credito["VENDEDOR_CLEAN"].str.contains(nome_usuario_limpo, na=False)].copy()
        else: df_base = pd.DataFrame()

    if df_base.empty:
        st.info(f"Nenhum cliente encontrado para o perfil: {nome_usuario}")
        return

    cols_existentes = [c for c in cols_order if c in df_base.columns]
    df_base = df_base[cols_existentes].copy()
    df_base.insert(0, "DETALHES", "👈 VER TÍTULOS")

    if tipo_usuario == "gerente comercial":
        if "GERENTE" in df_base.columns: df_base = df_base.drop(columns=["GERENTE"])
    elif tipo_usuario not in ["admin", "master", "gerente"]: 
        if "VENDEDOR" in df_base.columns: df_base = df_base.drop(columns=["VENDEDOR"])
        if "GERENTE" in df_base.columns: df_base = df_base.drop(columns=["GERENTE"])

    cols_dias = ["DIAS_PARA_VENCER_LC", "DIAS_PARA_VENCER_TITULO", "DIAS_EM_ATRASO_RECEBIVEIS"]
    for col in cols_dias:
        if col in df_base.columns: df_base[col] = pd.to_numeric(df_base[col], errors='coerce').apply(lambda x: f"{int(x)}" if pd.notnull(x) else "")

    for col in cols_financeiras:
        if col in df_base.columns: df_base[col] = df_base[col].apply(formatar_moeda)

    df_base = df_base.astype(str).replace(['None', 'nan', 'NaT', '<NA>', 'nan.0'], '')
    
    texto_busca_credito = st.text_input("🔍 Filtrar Clientes (CNPJ, Nome...):")
    if texto_busca_credito:
        mask = df_base.astype(str).apply(lambda x: x.str.contains(texto_busca_credito, case=False, na=False)).any(axis=1)
        df_base = df_base[mask]

    lista_clientes_com_pedido = []
    if not df_carteira.empty and "CLIENTE" in df_carteira.columns:
        lista_clientes_com_pedido = df_carteira["CLIENTE"].unique().tolist()

    if "CLIENTE" in df_base.columns:
        df_prioridade = df_base[df_base["CLIENTE"].isin(lista_clientes_com_pedido)].copy()
    else:
        df_prioridade = pd.DataFrame()

    config_colunas = {
        "DETALHES": st.column_config.TextColumn("", help="Clique na caixa de seleção à esquerda para ver os títulos.", width=130),
        "CLIENTE": st.column_config.TextColumn("Cliente", help="Nome do cliente."),
        "RISCO_DE_BLOQUEIO": st.column_config.TextColumn("RISCO", help="Nível de risco."),
    }

    def exibir_tabela_com_selecao(df_input, titulo):
        st.markdown(titulo)
        event = st.dataframe(df_input, hide_index=True, use_container_width=True, column_config=config_colunas, on_select="rerun", selection_mode="single-row")
        if event.selection.rows:
            idx = event.selection.rows[0]
            cnpj_selecionado = df_input.iloc[idx]["CNPJ"]
            cliente_selecionado = df_input.iloc[idx]["CLIENTE"]
            if not df_titulos_geral.empty:
                df_titulos_filtrado = df_titulos_geral[df_titulos_geral["CNPJ"] == cnpj_selecionado]
                mostrar_detalhes_titulos(cliente_selecionado, df_titulos_filtrado)
            else: mostrar_detalhes_titulos(cliente_selecionado, pd.DataFrame())

    if not df_prioridade.empty:
        exibir_tabela_com_selecao(df_prioridade, "#### Clientes com Pedidos Abertos")
        st.divider()
    exibir_tabela_com_selecao(df_base, "#### Todos os Clientes")

def exibir_aba_fotos(is_admin=False):
    st.info("ℹ️ Somente materiais da filial de Pinheiral.") 
    st.subheader("📷 Solicitação de Fotos (Material em RDQ)")
    st.markdown("Digite o número do Lote/Bobina abaixo para solicitar fotos de materiais no armazém 20/24.")
    with st.form("form_foto"):
        col_f1, col_f2 = st.columns([1, 2])
        with col_f1: lote_input = st.text_input("Lote / Bobina:")
        with col_f2: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''))
        if st.form_submit_button("Solicitar Fotos", type="primary"):
            if not lote_input: st.warning("Digite o lote.")
            elif not email_input: st.warning("Preencha o e-mail.")
            else:
                lote_limpo = lote_input.strip()
                if salvar_solicitacao_foto(st.session_state['usuario_nome'], email_input, lote_limpo): 
                    st.success(f"Solicitação do lote **{lote_limpo}** enviada!")

    if is_admin:
        st.divider()
        st.markdown("### 🛠️ Gestão de Pedidos de Fotos (Visão Admin)")
        df_fotos = carregar_solicitacoes_fotos()
        if not df_fotos.empty:
            st.dataframe(df_fotos, use_container_width=True, column_config={"Lote": st.column_config.TextColumn("Lote")})
            if st.button("Atualizar Lista de Fotos"): 
                carregar_solicitacoes_fotos.clear()
                st.rerun()
        else: st.info("Nenhum pedido de foto registrado.")

def exibir_aba_certificados(is_admin=False):
    st.info("ℹ️ Somente bobinas nacionas. Materiais de SFS solicitar diretamente com o Faturamento/Logística da unidade.") 
    st.subheader("📑 Solicitação de Certificados de Qualidade")
    with st.form("form_certificado"):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1: lote_cert = st.text_input("Lote / Bobina (Certificado):")
        with col_c2: email_cert = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''), key="email_cert_input")
        if st.form_submit_button("Solicitar Certificado", type="primary"):
            if not lote_cert: st.warning("Digite o lote.")
            elif not email_cert: st.warning("Preencha o e-mail.")
            elif salvar_solicitacao_certificado(st.session_state['usuario_nome'], email_cert, lote_cert): st.success(f"Solicitação enviada!")
    st.divider()
    df_cert = carregar_solicitacoes_certificados()
    if not df_cert.empty and not is_admin:
        user_email = st.session_state.get('usuario_email', '')
        if 'Email' in df_cert.columns: df_cert = df_cert[df_cert['Email'].str.lower() == user_email.lower()]
    if not df_cert.empty:
        st.dataframe(df_cert, use_container_width=True)
        if st.button("Atualizar Lista"): carregar_solicitacoes_certificados.clear(); st.rerun()

def exibir_aba_notas(is_admin=False):
    st.subheader("🧾 Solicitação de Nota Fiscal (PDF)")
    with st.form("form_notas"):
        col_n1, col_n2, col_n3 = st.columns([1, 1, 1])
        with col_n1: filial_input = st.selectbox("Selecione a Filial:", ["PINHEIRAL", "SJ BICAS", "SF DO SUL", "SAO PAULO"])
        with col_n2: nf_input = st.text_input("Número da NF:")
        with col_n3: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''), key="email_nf")
        if st.form_submit_button("Solicitar NF", type="primary"):
            if not nf_input: st.warning("Digite o número da nota.")
            elif not email_input: st.warning("Preencha o e-mail.")
            else:
                nf_limpa = nf_input.strip().lstrip('0')
                if salvar_solicitacao_nota(st.session_state['usuario_nome'], email_input, nf_limpa, filial_input): 
                    st.success(f"Solicitação enviada!")
    st.divider()
    df_notas = carregar_solicitacoes_notas()
    if not df_notas.empty and not is_admin:
        user_email = st.session_state.get('usuario_email', '')
        if 'Email' in df_notas.columns: df_notas = df_notas[df_notas['Email'].str.lower() == user_email.lower()]
    if not df_notas.empty:
        st.dataframe(df_notas, use_container_width=True)
        if st.button("Atualizar Lista"): carregar_solicitacoes_notas.clear(); st.rerun()

# --- SESSÃO ---
if 'logado' not in st.session_state:
    st.session_state['logado'] = False
    st.session_state['usuario_nome'] = ""
    st.session_state['usuario_filtro'] = ""
    st.session_state['usuario_email'] = "" 
    st.session_state['usuario_tipo'] = ""
if 'fazendo_cadastro' not in st.session_state: st.session_state['fazendo_cadastro'] = False

# --- LOGIN (V4 - BASEADO NO BACKUP 10) ---
if not st.session_state['logado']:
    if st.session_state['fazendo_cadastro']:
        st.title("📝 Solicitação de Acesso")
        with st.form("form_cadastro"):
            nome = st.text_input("Nome Completo")
            email = st.text_input("E-mail")
            login = st.text_input("Crie um Login")
            senha = st.text_input("Crie uma Senha", type="password")
            c1, c2 = st.columns(2)
            if c1.form_submit_button("Enviar Solicitação", type="primary", use_container_width=True):
                if nome and email and login and senha:
                    if salvar_nova_solicitacao(nome, email, login, senha): st.success("Solicitação enviada!")
                else: st.warning("Preencha tudo.")
            if c2.form_submit_button("Voltar", use_container_width=True): st.session_state['fazendo_cadastro'] = False; st.rerun()
    else:
        st.title("🔒 Login - Painel Dox")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            u = st.text_input("Login").strip()
            s = st.text_input("Senha", type="password").strip()
            if st.button("Acessar", type="primary"):
                # --- AQUI ESTAVA A MUDANÇA ---
                # Usando carregar_usuarios() que agora é otimizada por dentro (ler_com_retry)
                # mas mantendo a lógica de validação original do Backup 10
                df = carregar_usuarios()
                
                if df.empty:
                    st.error("Erro ao carregar base de usuários. Tente novamente.")
                elif 'Login' not in df.columns or 'Senha' not in df.columns:
                    st.error("Erro técnico na estrutura da tabela.")
                else:
                    try:
                        user = df[(df['Login'].str.lower() == u.lower()) & (df['Senha'] == s)]
                        if not user.empty:
                            d = user.iloc[0]
                            st.session_state.update({
                                'logado': True, 
                                'usuario_nome': d['Nome Vendedor'].split()[0], 
                                'usuario_filtro': d['Nome Vendedor'], 
                                'usuario_email': d.get('Email', ''), 
                                'usuario_tipo': d['Tipo'],
                                'usuario_login': d['Login'] # Salva o login para o feedback
                            })
                            registrar_acesso(u, d['Nome Vendedor'])
                            st.rerun()
                        else: st.error("Dados incorretos.")
                    except: st.error("Erro ao processar login.")
            st.markdown("---")
            if st.button("Solicitar Acesso"): st.session_state['fazendo_cadastro'] = True; st.rerun()
else:
    precisa_votar = False
    if st.session_state['usuario_tipo'].lower() == "vendedor":
        if 'feedback_enviado' not in st.session_state:
             login_atual = st.session_state.get('usuario_login', st.session_state['usuario_filtro'])
             st.session_state['feedback_enviado'] = ja_enviou_feedback(login_atual)
        
        if not st.session_state['feedback_enviado']:
            st.markdown("### 👋 Olá! Antes de prosseguir...")
            st.info("Para continuarmos evoluindo o Painel Dox, precisamos da sua opinião rápida.")
            with st.form("form_feedback"):
                q1 = st.radio("O que tem achado do Painel?", ["Excelente", "Bom", "Regular", "Ruim"], horizontal=True)
                q2 = st.radio("Você acessa o painel preferencialmente por onde?", ["Computador", "Celular", "Tablet"], horizontal=True)
                q3 = st.radio("Qual aba você menos utiliza?", ["Itens Programados", "Crédito", "Estoque", "Fotos RDQ", "Certificados", "Notas Fiscais", "Uso todas"])
                q4 = st.multiselect("Qual/Quais aba(s) você acha que poderia(m) ser removida(s)?", ["Itens Programados", "Crédito", "Estoque", "Fotos RDQ", "Certificados", "Notas Fiscais", "Nenhuma"])
                q5 = st.text_area("Alguma sugestão de melhoria? (Opcional)")
                if st.form_submit_button("Enviar Respostas", type="primary"):
                    login_save = st.session_state.get('usuario_login', st.session_state['usuario_filtro'])
                    nome_save = st.session_state['usuario_filtro']
                    remocao_str = ", ".join(q4)
                    if salvar_feedback(login_save, nome_save, q1, q2, q3, remocao_str, q5):
                        st.session_state['feedback_enviado'] = True
                        st.success("Obrigado!"); time.sleep(1.5); st.rerun()
                    else: st.error("Erro ao salvar.")
            st.stop()

    with st.sidebar:
        st.write(f"Bem-vindo, **{st.session_state['usuario_nome'].upper()}**")
        agora = datetime.now(FUSO_BR)
        st.caption(f"{agora.strftime('%d/%m/%Y')}")
        if st.button("Sair"): st.session_state.update({'logado': False, 'usuario_nome': ""}); st.rerun()
        st.divider()
        if st.button("🔄 Atualizar Dados"): st.cache_data.clear(); st.rerun()
        
        if st.session_state['usuario_tipo'].lower() == "vendedor":
            st.divider()
            df_fat_vend = carregar_faturamento_vendedores()
            if df_fat_vend is not None and not df_fat_vend.empty and 'VENDEDOR' in df_fat_vend.columns:
                df_mes = df_fat_vend[(df_fat_vend['DATA_DT'].dt.month == agora.month) & (df_fat_vend['DATA_DT'].dt.year == agora.year)]
                user_clean = str(st.session_state['usuario_filtro']).upper().strip()
                df_user = df_mes[df_mes['VENDEDOR'].astype(str).str.upper().str.strip().str.contains(user_clean, regex=False, na=False)]
                st.markdown(f"### 🎯 Seu Desempenho"); st.metric("Total (Tons)", f"{df_user['TONS'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    if st.session_state['usuario_tipo'].lower() == "admin":
        a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📝 Acessos", "📑 Certificados", "🧾 Notas Fiscais", "🔍 Logs", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_estoque()
        with a4: exibir_aba_fotos(True)
        with a5: st.dataframe(carregar_solicitacoes(), use_container_width=True)
        with a6: exibir_aba_certificados(True)
        with a7: exibir_aba_notas(True) 
        with a8: st.dataframe(carregar_logs_acessos(), use_container_width=True)
        with a9: exibir_aba_faturamento()
        with a10: exibir_aba_producao()
        
    elif st.session_state['usuario_tipo'].lower() == "master":
        a1, a2, a3, a4, a5, a6, a7, a8 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_estoque()
        with a4: exibir_aba_fotos(False)
        with a5: exibir_aba_certificados(False) 
        with a6: exibir_aba_notas(False)        
        with a7: exibir_aba_faturamento()
        with a8: exibir_aba_producao()
        
    else:
        a1, a2, a3, a4, a5, a6 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_estoque()
        with a4: exibir_aba_fotos(False)
        with a5: exibir_aba_certificados(False)
        with a6: exibir_aba_notas(False)