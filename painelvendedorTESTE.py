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
# 1. CONEXÃO GSPREAD OTIMIZADA ("Cofre Aberto")
# ==============================================================================

def get_gspread_client_cached():
    """
    Substitui a conexão antiga. Mantém o cliente na memória da sessão
    para evitar re-autenticar a cada clique (Economia de Cota e Tempo).
    """
    if 'gspread_client' not in st.session_state:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        except:
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        
        client = gspread.authorize(creds)
        st.session_state['gspread_client'] = client
        return client
    
    # Verifica se o token expirou e renova se necessário
    client = st.session_state['gspread_client']
    if client.auth.expired:
        client.login()
    return client

# ==============================================================================
# LEITURA E ESCRITA (COM TRATAMENTO DE ERRO "SINALIZADO")
# ==============================================================================

def ler_com_retry(url, aba, tentativas=5, espera=1):
    """
    Tenta ler os dados.
    - Se sucesso: Retorna DataFrame.
    - Se erro de conexão (429/Timeout): Retorna None (Sinal para usar cache).
    - Se vazio: Retorna DataFrame vazio.
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
            # Se for erro de cota, espera mais tempo
            msg = str(e).lower()
            if "429" in msg or "quota exceeded" in msg:
                time.sleep(espera * 2)
            else:
                time.sleep(espera)
            
            # Se falhar na última tentativa, retorna None (Erro Crítico de Conexão)
            if i == tentativas - 1:
                return None
    return None

def escrever_no_sheets(url, aba, df_novo, modo="append"):
    try:
        client = get_gspread_client_cached() # Usa a conexão rápida
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
# 2. FUNÇÃO DE BLINDAGEM DE DADOS ("Memória Persistente")
# ==============================================================================

def obter_dados_persistentes(chave_sessao, funcao_carregamento):
    """
    Tenta buscar dados novos.
    Se der erro de conexão (None), retorna SILENCIOSAMENTE os dados antigos 
    que já estavam na memória, sem mostrar erro para o usuário.
    """
    # 1. Garante que a memória existe (mesmo que vazia no início)
    if chave_sessao not in st.session_state:
        st.session_state[chave_sessao] = pd.DataFrame()
    
    # 2. Tenta carregar dados novos
    dados_novos = funcao_carregamento()
    
    # 3. Se veio dado válido (mesmo que tabela vazia, mas conexão OK), atualiza a memória
    if dados_novos is not None:
        st.session_state[chave_sessao] = dados_novos
    
    # 4. Se dados_novos for None (Erro Conexão), ignora e retorna o antigo (Memória)
    return st.session_state[chave_sessao]

# ==============================================================================
# FUNÇÕES DE FEEDBACK
# ==============================================================================


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
# CARREGAMENTO DE DADOS (USANDO CACHE DO STREAMLIT + SINAL DE ERRO)
# ==============================================================================

@st.cache_data(ttl="30m", show_spinner=False)
def carregar_usuarios():
    # Login precisa ser confiável, então tenta mais vezes
    df_users = ler_com_retry(URL_SISTEMA, "Usuarios", tentativas=10, espera=2)
    if df_users is not None and not df_users.empty: return df_users.astype(str)
    return pd.DataFrame()


@st.cache_data(ttl="10m", show_spinner=False)
def ler_dados_nuvem_generico(aba, url_planilha):
    df = ler_com_retry(url_planilha, aba)
    if df is None: return None # Retorna None para ativar persistência
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
    if df is None: return None # Erro de conexão = None
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
    if df is None: return pd.DataFrame(columns=['FILIAL', 'META']) # Metas podem falhar sem quebrar
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

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_feedbacks_avisos():
    df = ler_com_retry(URL_SISTEMA, "Feedback_Vendedores")
    if df is None: return pd.DataFrame()
    return df

def registrar_ciencia_aviso(login, nome):
    try:
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        # Estrutura com as 10 colunas exatas. As antigas de feedback ficam em branco ("").
        nova_linha = pd.DataFrame([{
            "Data": agora_br, 
            "Login": login, 
            "Nome": nome, 
            "Satisfacao": "", 
            "Dispositivo": "", 
            "Aba_Menos_Usada": "", 
            "Abas_Remover": "", 
            "Sugestao": "", 
            "Tipo_Aviso": "Status_Servidor", 
            "Mensagem": "Ciente"
        }])
        
        if escrever_no_sheets(URL_SISTEMA, "Feedback_Vendedores", nova_linha, modo="append"):
            carregar_feedbacks_avisos.clear()
            return True
        return False
    except:
        return False

@st.cache_data(ttl="2m", show_spinner=False)
def carregar_status_robo():
    df = ler_com_retry(URL_SISTEMA, "Status_Robo", tentativas=2, espera=1)
    if df is None: return None
    return df

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

@st.cache_data(ttl="1m", show_spinner=False)
def carregar_dados_manutencao():
    # Tenta ler a aba Dados_Manutencao
    df = ler_com_retry(URL_SISTEMA, "Dados_Manutencao")
    if df is None: return None
    if not df.empty:
        # Renomeia as colunas que vêm do Google Forms para facilitar
        # O Forms geralmente cria "Carimbo de data/hora", "Qual a máquina?", etc.
        # Vamos normalizar para não dar erro no código.
        cols_atuais = df.columns.tolist()
        mapa_colunas = {
            cols_atuais[0]: "Data_Abertura", # A 1ª coluna é sempre o timestamp
            cols_atuais[1]: "Maquina",
            cols_atuais[2]: "Operador",
            cols_atuais[3]: "Tipo_Problema",
            cols_atuais[4]: "Descricao"
        }
        df = df.rename(columns=mapa_colunas)
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

def atualizar_chamado_manutencao(row_index, status, prioridade, mecanico, inicio, fim, solucao):
    try:
        client = get_gspread_client_cached()
        sheet = client.open_by_url(URL_SISTEMA)
        worksheet = sheet.worksheet("Dados_Manutencao")
        
        # O índice da linha no Google Sheets é = index do dataframe + 2 
        # (+1 pelo cabeçalho, +1 pq o google começa no 1 e o python no 0)
        linha_sheet = row_index + 2
        
        # Atualiza as colunas de Gestão (F, G, H, I, J, K...)
        # Ajuste as letras/números das colunas conforme sua planilha real
        # Supondo: F=Status, G=Prioridade, H=Mecanico, I=Inicio, J=Fim, K=Solucao
        
        # Atualiza Status (Coluna 6 se for a F, ou conte na sua planilha)
        # DICA: Se "Descricao" é a Coluna 5 (E), então Status é 6 (F), Prioridade 7 (G)...
        
        # Vamos enviar uma lista para atualizar a linha de uma vez nas colunas certas
        # Ajuste o range conforme onde você escreveu os cabeçalhos na planilha
        # Exemplo: Atualizando da coluna 6 (Status) até a 11 (Solucao)
        
        worksheet.update_cell(linha_sheet, 6, status)      # Coluna F
        worksheet.update_cell(linha_sheet, 7, prioridade)  # Coluna G
        worksheet.update_cell(linha_sheet, 8, mecanico)    # Coluna H
        worksheet.update_cell(linha_sheet, 9, inicio)      # Coluna I
        worksheet.update_cell(linha_sheet, 10, fim)        # Coluna J
        worksheet.update_cell(linha_sheet, 11, solucao)    # Coluna K
        
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False    

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
            st.rerun() # Persistencia ativa
            
    # USO DA FUNÇÃO BLINDADA
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
            
    # USO DA FUNÇÃO BLINDADA
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

# --- NOVA ABA DE ESTOQUE (AG-GRID + FILTROS SIMPLIFICADOS) ---
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
    with c1:
        filial_sel = st.selectbox("Filtrar por Filial:", lista_filiais)
    with c2:
        busca = st.text_input("Buscar (aperte enter após digitar):")

    # CHECKBOX DE FILTRO DE DISPONIBILIDADE
    # Título principal + Caption abaixo (para fonte menor)
    somente_disp = st.checkbox("Somente Disponível")
    st.caption("(marque para mostrar somente itens que possuem saldo disponível maior que zero)")

    # APLICAÇÃO DOS FILTROS
    df_filtrado = df_estoque.copy()
    
    # 1. Filtro de Saldo
    if somente_disp:
        if 'DISPONIVEL' in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado['DISPONIVEL'] > 0.001]

    # 2. Filtro de Filial
    if filial_sel != "Todas":
        df_filtrado = df_filtrado[df_filtrado['FILIAL'] == filial_sel]
        
    # 3. Filtro de Busca
    if busca:
        mask = df_filtrado.astype(str).apply(lambda x: x.str.contains(busca, case=False, na=False)).any(axis=1)
        df_filtrado = df_filtrado[mask]

    st.markdown(f"**Itens encontrados:** {len(df_filtrado)}")
    
    # ------------------------------------------------------------------
    # PREPARAÇÃO PARA EXIBIÇÃO VISUAL (TEXTO FORMATADO)
    # ------------------------------------------------------------------
    
    df_show = df_filtrado.copy()
    
    # 1. Renomeia Colunas
    mapa_renomeacao = {
        'PRODUTO': 'DESCRIÇÃO DO PRODUTO',
        'ARMAZEM': 'ARM',
        'LARGURA': 'LARG',
        'COMPRIMENTO': 'COMP',
        'EMPENHADO': 'EMP',
        'DISPONIVEL': 'DISP'
    }
    df_show.rename(columns=mapa_renomeacao, inplace=True)

    # 2. Formata Espessura
    if 'ESPES' in df_show.columns:
        df_show['ESPES'] = df_show['ESPES'].apply(lambda x: formatar_br_decimal(x, 2))

    # 3. Formata Quantidades
    cols_qtd = ['QTDE', 'EMP', 'DISP']
    for col in cols_qtd:
        if col in df_show.columns:
            df_show[col] = df_show[col].apply(lambda x: formatar_br_decimal(x, 3))

    # 4. Formata Comprimento
    if 'COMP' in df_show.columns:
        df_show['COMP'] = df_show['COMP'].apply(lambda x: str(int(x)) if x > 0 else "0")

    # Seleção e Ordem das colunas
    colunas_desejadas = [
        "FILIAL", 
        "ARM", 
        "DESCRIÇÃO DO PRODUTO", 
        "LOTE", 
        "ESPES", 
        "LARG", 
        "COMP", 
        "QTDE", 
        "EMP",
        "DISP"
    ]
    cols_finais = [c for c in colunas_desejadas if c in df_show.columns]
    
    # ------------------------------------------------------------------
    # CONFIGURAÇÃO DA AG-GRID
    # ------------------------------------------------------------------
    
    gb = GridOptionsBuilder.from_dataframe(df_show[cols_finais])
    
    # Configurações Globais (Floating Filter ATIVADO)
    gb.configure_default_column(
        resizable=True, 
        filterable=True, 
        sortable=True,
        cellStyle={'textAlign': 'center'},
        suppressSizeToFit=False # Garante que tente ajustar
    )
    
    gb.configure_grid_options(floatingFilter=True) # <--- BUSCA INSTANTÂNEA EM CADA COLUNA

    # Configurações Específicas de Coluna (Larguras AGRESSIVAS e FIXAS)
    
    # Descrição do Produto: A única com flex=1 para ocupar o espaço que sobrar
    gb.configure_column("DESCRIÇÃO DO PRODUTO", minWidth=380, flex=1, cellStyle={'textAlign': 'left'}) 
    
    # Colunas pequenas (Números e códigos) - Definindo width fixo pequeno
    gb.configure_column("FILIAL", width=110, minWidth=90)
    gb.configure_column("ARM", width=60, minWidth=50, maxWidth=70)
    gb.configure_column("LOTE", width=110, minWidth=90)
    
    # Medidas (Bem compactas)
    gb.configure_column("ESPES", width=70, minWidth=60, maxWidth=80)
    gb.configure_column("LARG", width=70, minWidth=60, maxWidth=80)
    gb.configure_column("COMP", width=70, minWidth=60, maxWidth=80)
    
    # Quantidades (Médias)
    gb.configure_column("QTDE", width=80, minWidth=70, maxWidth=100)
    gb.configure_column("EMP", width=80, minWidth=70, maxWidth=100)
    
    # Disponível (Destaque, um pouco maior para não cortar o negrito)
    gb.configure_column("DISP", width=90, minWidth=80, maxWidth=110, cellStyle={'fontWeight': 'bold', 'textAlign': 'center', 'color': '#000080'})
    
    gb.configure_selection('single', use_checkbox=False)
    gridOptions = gb.build()
    
    AgGrid(
        df_show[cols_finais],
        gridOptions=gridOptions,
        height=500,
        width='100%',
        fit_columns_on_grid_load=True, 
        theme='streamlit',
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True
    )


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
        if tipo_usuario in ["admin", "gerente", "master", "logística", "logistica", "pcp"]:
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
            # --- APLICANDO A FUNÇÃO SEGURA TAMBÉM NOS PEDIDOS ---
            df_filtrado['Quantidade_Num'] = df_filtrado['Quantidade'].apply(converte_numero_seguro)
            
            df_filtrado['Peso (ton)'] = df_filtrado['Quantidade_Num'].apply(formatar_peso_brasileiro)
            try:
                df_filtrado['Prazo_dt'] = pd.to_datetime(df_filtrado['Prazo'], dayfirst=True, errors='coerce')
                df_filtrado['Prazo'] = df_filtrado['Prazo_dt'].dt.strftime('%d/%m/%Y').fillna("-")
            except: pass
            colunas_visiveis = ["Número do Pedido", "Filial_Origem", "Cliente Correto", "Produto", "Peso (ton)", "Prazo", "Máquina/Processo"]
            if tipo_usuario in ["admin", "gerente", "gerente comercial", "master", "logística", "logistica", "pcp"]: 
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

@st.dialog("📡 Novo Recurso: Status do Servidor", width="large")
def popup_aviso_servidor():
    # Esse truque em HTML/CSS esconde o botão "X" (Close) no topo do pop-up
    st.markdown(
        """
        <style>
            button[aria-label="Close"] {
                display: none !important;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.markdown(f"Olá, **{st.session_state['usuario_nome']}**!")
    st.markdown("Adicionamos um novo indicador no seu menu lateral para mostrar a 'saúde' da nossa conexão com a fábrica em tempo real.")
    
    st.markdown("🟢 **Servidor Online:** Tudo normal! Dados atualizados.")
    st.markdown("🔴 **Servidor Offline:** Houve uma perda temporária de comunicação com o servidor da Dox.")
    
    st.info("**O que muda quando o servidor estiver Offline?**\n\n"
            "• Os dados do painel podem estar com alguns minutos de atraso, e será normalizado até a conexão ser restabelecida.\n\n"
            "• Suas solicitações automáticas (**Certificados, Notas Fiscais e Fotos**) ficarão 'na fila'.\n\n"
            "• **Não precisa pedir de novo!** Assim que a conexão voltar, o sistema processará a fila e enviará tudo para o seu e-mail automaticamente.")
    
    if st.button("👍 Entendi e estou ciente", type="primary", use_container_width=True):
        # Registra na planilha
        registrar_ciencia_aviso(st.session_state['usuario_login'], st.session_state['usuario_nome'])
        # Marca na sessão para não abrir de novo hoje
        st.session_state['viu_aviso_servidor'] = True
        st.rerun()

# --- DIALOG PARA EXIBIR TÍTULOS ---
@st.dialog("Detalhes Financeiros", width="large")
def mostrar_detalhes_titulos(cliente_nome, df_titulos):
    st.markdown(f"### 🏢 {cliente_nome}")
    st.caption("Abaixo a lista de títulos em aberto (vencidos e a vencer) para este cliente.")
    
    if df_titulos.empty:
        st.warning("Não há títulos pendentes registrados para este CNPJ.")
    else:
        # Tratamento visual da tabela de títulos
        df_show = df_titulos.copy()
        
        # Formatando valor como moeda para exibição
        if "VALOR" in df_show.columns:
            df_show["VALOR"] = df_show["VALOR"].apply(formatar_moeda)
        if "SALDO" in df_show.columns:
            df_show["SALDO"] = df_show["SALDO"].apply(formatar_moeda)

        # Selecionar colunas relevantes para o vendedor
        cols_visual = [
            "DATA_EMISSAO", "NOTA_FISCAL", "PARCELA", "VALOR", "SALDO",
            "VENCIMENTO", "STATUS_RESUMO", "STATUS_DETALHADO", "TIPO_DE_FATURAMENTO"
        ]
        
        # Filtra colunas que realmente existem
        cols_finais = [c for c in cols_visual if c in df_show.columns]
        
        st.dataframe(
            df_show[cols_finais], 
            hide_index=True,
            use_container_width=True,
            column_config={
                "NOTA_FISCAL": st.column_config.TextColumn("NF"),
                "DATA_EMISSAO": st.column_config.TextColumn("Emissão"),
                "STATUS_RESUMO": st.column_config.TextColumn("Status"),
                "STATUS_DETALHADO": st.column_config.TextColumn("Detalhe Vencimento"),
                "TIPO_DE_FATURAMENTO": st.column_config.TextColumn("Tipo Fat.")
            }
        )

def exibir_aba_credito():
    st.markdown("### 💰 Painel de Crédito <small style='font-weight: normal; font-size: 14px; color: gray;'>(Aba em teste. Qualquer divergência, por favor reporte.)</small>", unsafe_allow_html=True)
    
    # --- LEGENDA RETRÁTIL (NO TOPO) ---
    with st.expander("ℹ️ Legenda: Entenda o significado de cada coluna (Clique para expandir)"):
        st.markdown("""
        **CLIENTE**: Nome do cliente cadastrado na empresa.
        
        **CNPJ**: CNPJ do cliente.
        
        **VENDEDOR**: Vendedor responsável pelo atendimento desse cliente.
        
        **GERENTE**: Gerente responsável pelo vendedor.
        
        **RISCO_DE_BLOQUEIO**: Indica o nível de risco de o cliente ter o faturamento bloqueado no momento.
        * **ALTO**: Faturamento pode ser bloqueado. Atenção imediata.
        * **MÉDIO**: Atenção, pode virar bloqueio em breve.
        * **BAIXO**: Situação normal no momento.
        
        **ACAO_SUGERIDA**: Orientação clara do que o vendedor deve fazer agora com esse cliente (cobrar, aguardar, falar com Financeiro ou faturar normalmente).
        
        **MOTIVO_PROVAVEL_DO_BLOQUEIO**: Explica o principal motivo que pode causar bloqueio de faturamento (atraso, limite vencido, limite baixo ou outro risco identificado).
        
        **OPCAO_DE_FATURAMENTO**: Mostra por qual tipo de crédito o cliente pode faturar no momento.
        * Crédito disponível via LC DOX e BV: Pode faturar normalmente.
        * Somente LC DOX disponível: Faturar apenas dentro do limite DOX.
        * Somente BV disponível: Faturar usando BV.
        * Sem crédito disponível: Necessário falar com o Financeiro antes de faturar.
        
        **RECEBÍVEIS**: Indica se o cliente possui títulos vencidos em aberto.
        * **Em Atraso**: Existe valor vencido não pago.
        * **Em Dia**: Nenhum título vencido.
        
        **DIAS_EM_ATRASO_RECEBIVEIS**: Quantidade de dias que o título mais antigo está em atraso. Quanto maior, maior o risco de bloqueio.
        
        **SALDO_VENCIDO**: Valor total em aberto de títulos que já venceram e ainda não foram pagos pelo cliente.
        
        **VENCIMENTO LC**: Situação do vencimento do limite de crédito do cliente.
        * **LC OK**: Limite válido.
        * **LC Vencido**: Limite expirado.
        * **Sem data de vencimento**: Cadastro precisa ser verificado com o Financeiro.
        
        **DIAS_PARA_VENCER_LC**: Quantos dias faltam para o limite de crédito vencer. Valores baixos indicam atenção.
        
        **DATA_VENC_LC**: Data em que o limite de crédito do cliente vence.
        
        **DISPONIVEL VIA LC2**: Valor disponível para faturar usando o limite de crédito DOX, já considerando títulos em aberto.
        
        **DISPONIVEL BV**: Valor disponível para faturar usando a modalidade BV (Banco/Vendor).
        
        **DISPONIVEL VIA RA**: Valor disponível para faturar via RA (recebimento antecipado), desde que não existam atrasos.
        
        **SALDO_A_VENCER**: Valor total de títulos que ainda vão vencer no futuro (não estão atrasados).
        
        **DIAS_PARA_VENCER_TITULO**: Quantidade de dias para o próximo título vencer. Ajuda a prever risco de atraso.
        
        **DATA_VENCIMENTO_MAIS_ANTIGA**: Data do título vencido mais antigo do cliente. Indica há quanto tempo existe inadimplência.
        
        **LC_DOX**: Limite de crédito DOX ainda disponível após considerar os títulos em aberto.
        
        **LC_BV**: Limite total disponível para faturamento via BV.
        
        **LC_TOTAL**: Valor total do limite de crédito concedido ao cliente.
        
        **RA**: Valor total de títulos do tipo RA (recebimento antecipado) ainda em aberto.
        
        **EM ABERTO**: Soma de todos os títulos em aberto do cliente, independentemente do vencimento.
        
        **EM ABERTO BV**: Valor total de títulos em aberto vinculados à modalidade BV.
        
        **LC_SUPPLIER**: Limite total de crédito aprovado pelo banco para o cliente operar via Supplier.
        
        **SUPPLIER_DISP**: Valor do limite Supplier que ainda está disponível para uso em novas vendas.
        
        **SITUACAO_LC**: Indica se o convênio de crédito do cliente com o banco (Supplier/BV) está liberado, bloqueado ou em análise no momento.
        * **ATIVO / LIBERADO**: Cliente pode faturar normalmente via Supplier/BV.
        * **BLOQUEADO**: Faturamento via Supplier/BV não é permitido até regularização.
        * **SUSPENSO**: Convênio temporariamente suspenso pelo banco, sem liberação para faturar.
        * **EM ANÁLISE**: Crédito em avaliação pelo banco, geralmente sem liberação até conclusão.
        * **CANCELADO**: Convênio de crédito encerrado, não sendo mais possível operar via Supplier/BV.
        """)

    # 1. Carrega Dados (Com Retry Logic) - BLINDADO
    df_credito = obter_dados_persistentes("cache_credito", carregar_dados_credito)
    df_carteira = obter_dados_persistentes("cache_carteira_cred", carregar_dados_carteira)
    df_titulos_geral = obter_dados_persistentes("cache_titulos", carregar_dados_titulos) 
    
    if df_credito.empty:
        st.info("Nenhuma informação de crédito disponível no momento (Aguardando sincronização do Robô).")
        return

    # 2. Definição das Colunas
    cols_order = [
        "CNPJ", "CLIENTE", "VENDEDOR", "GERENTE", "RISCO_DE_BLOQUEIO", "ACAO_SUGERIDA", "MOTIVO_PROVAVEL_DO_BLOQUEIO",
        "OPCAO_DE_FATURAMENTO", "RECEBIVEIS", "DIAS_EM_ATRASO_RECEBIVEIS", "SALDO_VENCIDO", "VENCIMENTO LC",
        "DIAS_PARA_VENCER_LC", "DATA_VENC_LC", "DISPONIVEL VIA LC2", "DISPONIVEL BV", "DISPONIVEL VIA RA",
        "SALDO_A_VENCER", "DIAS_PARA_VENCER_TITULO", "DATA_VENCIMENTO_MAIS_ANTIGA", "LC DOX", "LC BV", "LC TOTAL",
        "RA", "EM_ABERTO", "EM ABERTO BV", "LC SUPPLIER", "SUPPLIER DISP", "SITUACAO LC"
    ]
    cols_financeiras = [
        "SALDO_VENCIDO", "SALDO_A_VENCER", "LC TOTAL", "LC DOX", "RA", 
        "EM_ABERTO", "DISPONIVEL VIA RA", "DISPONIVEL VIA LC2", "LC BV", 
        "EM ABERTO BV", "DISPONIVEL BV", "LC SUPPLIER", "SUPPLIER DISP"
    ]

    # 3. Filtragem Global (Vendedor Logado)
    tipo_usuario = st.session_state['usuario_tipo'].lower()
    nome_usuario = st.session_state['usuario_filtro']
    nome_usuario_limpo = nome_usuario.strip().lower()

    if tipo_usuario in ["admin", "master", "gerente"]:
        df_base = df_credito.copy()
        
    elif tipo_usuario == "gerente comercial":
        if "GERENTE" in df_credito.columns:
            df_credito["GERENTE_CLEAN"] = df_credito["GERENTE"].astype(str).str.strip().str.lower()
            df_base = df_credito[df_credito["GERENTE_CLEAN"].str.contains(nome_usuario_limpo, na=False)].copy()
        else:
            df_base = pd.DataFrame()
            
    else:
        if "VENDEDOR" in df_credito.columns:
            df_credito["VENDEDOR_CLEAN"] = df_credito["VENDEDOR"].astype(str).str.strip().str.lower()
            df_base = df_credito[df_credito["VENDEDOR_CLEAN"].str.contains(nome_usuario_limpo, na=False)].copy()
        else:
            df_base = pd.DataFrame()

    if df_base.empty:
        st.info(f"Nenhum cliente encontrado para o perfil: {nome_usuario}")
        return

    # 4. Tratamento Prévio
    cols_existentes = [c for c in cols_order if c in df_base.columns]
    df_base = df_base[cols_existentes].copy()

    # --- INSERÇÃO DA COLUNA ISCA (AJUSTADA V63) ---
    # Inserimos a coluna "DETALHES" na posição 0 com a seta e ajuste de largura
    df_base.insert(0, "DETALHES", "👈 VER TÍTULOS")

    # --- CONTROLE DE VISIBILIDADE DAS COLUNAS ---
    if tipo_usuario == "gerente comercial":
        if "GERENTE" in df_base.columns: df_base = df_base.drop(columns=["GERENTE"])
    elif tipo_usuario not in ["admin", "master", "gerente"]: 
        if "VENDEDOR" in df_base.columns: df_base = df_base.drop(columns=["VENDEDOR"])
        if "GERENTE" in df_base.columns: df_base = df_base.drop(columns=["GERENTE"])

    # Tratamento de Dias e Moeda
    cols_dias = ["DIAS_PARA_VENCER_LC", "DIAS_PARA_VENCER_TITULO", "DIAS_EM_ATRASO_RECEBIVEIS"]
    for col in cols_dias:
        if col in df_base.columns:
            df_base[col] = pd.to_numeric(df_base[col], errors='coerce').apply(lambda x: f"{int(x)}" if pd.notnull(x) else "")

    for col in cols_financeiras:
        if col in df_base.columns:
            df_base[col] = df_base[col].apply(formatar_moeda)

    df_base = df_base.astype(str).replace(['None', 'nan', 'NaT', '<NA>', 'nan.0'], '')

    # 5. Filtro de Busca
    texto_busca_credito = st.text_input("🔍 Filtrar Clientes (CNPJ, Nome...):")
    if texto_busca_credito:
        mask = df_base.astype(str).apply(lambda x: x.str.contains(texto_busca_credito, case=False, na=False)).any(axis=1)
        df_base = df_base[mask]

    # 6. Separação: Com Pedido vs Sem Pedido
    lista_clientes_com_pedido = []
    if not df_carteira.empty and "CLIENTE" in df_carteira.columns:
        lista_clientes_com_pedido = df_carteira["CLIENTE"].unique().tolist()

    if "CLIENTE" in df_base.columns:
        df_prioridade = df_base[df_base["CLIENTE"].isin(lista_clientes_com_pedido)].copy()
    else:
        df_prioridade = pd.DataFrame()

    # Configuração das Colunas (V67 - SUPPLIER ADICIONADO)
    config_colunas = {
        "DETALHES": st.column_config.TextColumn("", help="Clique na caixa de seleção à esquerda para ver os títulos.", width=130),
        "CLIENTE": st.column_config.TextColumn("Cliente", help="Nome do cliente."),
        "CNPJ": st.column_config.TextColumn("CNPJ", help="CNPJ."),
        "VENDEDOR": st.column_config.TextColumn("Vendedor", help="Vendedor."),
        "GERENTE": st.column_config.TextColumn("Gerente", help="Gerente."),
        "RISCO_DE_BLOQUEIO": st.column_config.TextColumn("RISCO_DE_BLOQUEIO", help="Nível de risco de bloqueio (ALTO/MÉDIO/BAIXO)."),
        "ACAO_SUGERIDA": st.column_config.TextColumn("ACAO_SUGERIDA", help="Orientação do que fazer."),
        "MOTIVO_PROVAVEL_DO_BLOQUEIO": st.column_config.TextColumn("MOTIVO_PROVAVEL_DO_BLOQUEIO", help="Motivo do risco."),
        "OPCAO_DE_FATURAMENTO": st.column_config.TextColumn("OPCAO_DE_FATURAMENTO", help="Opção de faturamento disponível."),
        "RECEBIVEIS": st.column_config.TextColumn("RECEBÍVEIS", help="Status dos pagamentos (Em Dia / Em Atraso)."),
        "DIAS_EM_ATRASO_RECEBIVEIS": st.column_config.TextColumn("DIAS_EM_ATRASO_RECEBIVEIS", help="Dias de atraso do título mais antigo."),
        "SALDO_VENCIDO": st.column_config.TextColumn("SALDO_VENCIDO", help="Valor vencido em aberto."),
        "VENCIMENTO LC": st.column_config.TextColumn("VENCIMENTO LC", help="Status do limite (OK / Vencido)."),
        "DIAS_PARA_VENCER_LC": st.column_config.TextColumn("DIAS_PARA_VENCER_LC", help="Dias para vencer o limite."),
        "DATA_VENC_LC": st.column_config.TextColumn("DATA_VENC_LC", help="Data de vencimento do limite."),
        "DISPONIVEL VIA LC2": st.column_config.TextColumn("DISPONIVEL VIA LC2", help="Valor livre no Limite DOX."),
        "DISPONIVEL BV": st.column_config.TextColumn("DISPONIVEL BV", help="Valor livre no Limite BV."),
        "DISPONIVEL VIA RA": st.column_config.TextColumn("DISPONÍVEL VIA RA", help="Valor livre via RA."),
        "SALDO_A_VENCER": st.column_config.TextColumn("SALDO_A_VENCER", help="Valor a vencer."),
        "DIAS_PARA_VENCER_TITULO": st.column_config.TextColumn("DIAS_PARA_VENCER_TITULO", help="Dias para o próximo título vencer."),
        "DATA_VENCIMENTO_MAIS_ANTIGA": st.column_config.TextColumn("DATA_VENCIMENTO_MAIS_ANTIGA", help="Data do título vencido mais antigo."),
        "LC DOX": st.column_config.TextColumn("LC_DOX", help="Limite DOX disponível."),
        "LC BV": st.column_config.TextColumn("LC_BV", help="Limite BV total."),
        "LC TOTAL": st.column_config.TextColumn("LC_TOTAL", help="Limite total."),
        "RA": st.column_config.TextColumn("RA", help="Valor em RA."),
        "EM_ABERTO": st.column_config.TextColumn("EM ABERTO", help="Total em aberto."),
        "EM ABERTO BV": st.column_config.TextColumn("EM ABERTO BV", help="Total em aberto BV."),
        "LC SUPPLIER": st.column_config.TextColumn("LC SUPPLIER", help="Limite total de crédito aprovado pelo banco para o cliente operar via Supplier."),
        "SUPPLIER DISP": st.column_config.TextColumn("SUPPLIER DISP", help="Valor do limite Supplier que ainda está disponível para uso em novas vendas."),
        "SITUACAO LC": st.column_config.TextColumn("SITUACAO LC", help="Indica se o convênio de crédito do cliente com o banco (Supplier/BV) está liberado, bloqueado ou em análise.")
    }

    # 7. Renderização das Tabelas com SELEÇÃO
    
    # Função auxiliar para exibir e processar a seleção
    def exibir_tabela_com_selecao(df_input, titulo):
        st.markdown(titulo)
        # Evento de seleção de linha
        event = st.dataframe(
            df_input, 
            hide_index=True, 
            use_container_width=True, 
            column_config=config_colunas,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # Lógica: Se clicou
        if event.selection.rows:
            idx = event.selection.rows[0]
            # Pega o CNPJ da linha selecionada
            cnpj_selecionado = df_input.iloc[idx]["CNPJ"]
            cliente_selecionado = df_input.iloc[idx]["CLIENTE"]
            
            # Filtra os títulos desse CNPJ
            if not df_titulos_geral.empty:
                df_titulos_filtrado = df_titulos_geral[df_titulos_geral["CNPJ"] == cnpj_selecionado]
                mostrar_detalhes_titulos(cliente_selecionado, df_titulos_filtrado)
            else:
                mostrar_detalhes_titulos(cliente_selecionado, pd.DataFrame())

    if not df_prioridade.empty:
        exibir_tabela_com_selecao(df_prioridade, "#### Clientes com Pedidos Abertos")
        st.divider()
    
    exibir_tabela_com_selecao(df_base, "#### Todos os Clientes")


def exibir_aba_fotos(is_admin=False):
    st.info("ℹ️ Somente materiais da filial de Pinheiral.") 
    st.subheader("📷 Solicitação de Fotos (Material em RDQ)")
    st.markdown("Digite o número do Lote exato abaixo para solicitar fotos de materiais no armazém 20/24.")
    with st.form("form_foto"):
        col_f1, col_f2 = st.columns([1, 2])
        with col_f1: lote_input = st.text_input("Lote:")
        with col_f2: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''))
        if st.form_submit_button("Solicitar Fotos", type="primary"):
            if not lote_input: 
                st.warning("Digite o lote.")
            elif not email_input: 
                st.warning("Preencha o e-mail.")
            else:
                # --- LIMPEZA DE ESPAÇOS AUTOMÁTICA ---
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
    st.markdown("Digite o número do Lote exato para receber o certificado de qualidade.")
    with st.form("form_certificado"):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1: 
            lote_cert = st.text_input("Lote:")
            st.caption("ℹ️ Lotes que só alteram o sequencial final são provenientes da mesma matéria prima. Exemplo: 06818601001, 06818601002, 06818601003 representam a mesma bobina pai.")
        with col_c2: email_cert = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''), key="email_cert_input")
        if st.form_submit_button("Solicitar Certificado", type="primary"):
            if not lote_cert: st.warning("Digite o lote.")
            elif not email_cert: st.warning("Preencha o e-mail.")
            elif salvar_solicitacao_certificado(st.session_state['usuario_nome'], email_cert, lote_cert): st.success(f"Solicitação de certificado do lote **{lote_cert}** enviada!")
    st.divider()
    if is_admin: st.markdown("### 🛠️ Histórico de Solicitações (Visão Admin)")
    else: st.markdown("### 📜 Meus Pedidos de Certificados")
    df_cert = carregar_solicitacoes_certificados()
    if not df_cert.empty and not is_admin:
        user_email = st.session_state.get('usuario_email', '')
        if 'Email' in df_cert.columns: df_cert = df_cert[df_cert['Email'].str.lower() == user_email.lower()]
    if not df_cert.empty:
        st.dataframe(df_cert, use_container_width=True, column_config={"Lote": st.column_config.TextColumn("Lote")})
        if st.button("Atualizar Lista de Certificados"): 
            carregar_solicitacoes_certificados.clear()
            st.rerun()
    else: st.info("Nenhum pedido encontrado.")

def exibir_aba_notas(is_admin=False):
    st.subheader("🧾 Solicitação de Nota Fiscal (PDF)")
    st.markdown("Digite o número da Nota Fiscal para receber o PDF por e-mail. **Atenção:** Por segurança, o sistema só enviará notas que pertençam à sua carteira de clientes.")
    with st.form("form_notas"):
        col_n1, col_n2, col_n3 = st.columns([1, 1, 1])
        with col_n1: 
            # MUDANÇA: Adicionado SAO PAULO
            filial_input = st.selectbox("Selecione a Filial:", ["PINHEIRAL", "SJ BICAS", "SF DO SUL", "SAO PAULO"])
        with col_n2: nf_input = st.text_input("Número da NF (Ex: 71591):")
        with col_n3: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''), key="email_nf")
        if st.form_submit_button("Solicitar NF", type="primary"):
            if not nf_input: st.warning("Digite o número da nota.")
            elif not email_input: st.warning("Preencha o e-mail.")
            else:
                # MUDANÇA: Lógica de limpeza (strip e remove zeros a esquerda)
                nf_limpa = nf_input.strip().lstrip('0')
                if salvar_solicitacao_nota(st.session_state['usuario_nome'], email_input, nf_limpa, filial_input): 
                    st.success(f"Solicitação da NF **{nf_limpa}** ({filial_input}) enviada!")
    st.divider()
    if is_admin: st.markdown("### 🛠️ Histórico de Solicitações (Visão Admin)")
    else: st.markdown("### 📜 Meus Pedidos de Notas")
    df_notas = carregar_solicitacoes_notas()
    if not df_notas.empty and not is_admin:
        user_email = st.session_state.get('usuario_email', '')
        if 'Email' in df_notas.columns: df_notas = df_notas[df_notas['Email'].str.lower() == user_email.lower()]
    if not df_notas.empty:
        st.dataframe(df_notas, use_container_width=True, column_config={"NF": st.column_config.TextColumn("NF")})
        if st.button("Atualizar Lista de Notas"): 
            carregar_solicitacoes_notas.clear()
            st.rerun()
    else: st.info("Nenhum pedido encontrado.")

def exibir_aba_manutencao():
    st.subheader("🔧 Gestão de Manutenção (Chão de Fábrica)")
    
    # Botão de atualizar geral
    if st.button("🔄 Atualizar Dados Manutenção"):
        carregar_dados_manutencao.clear()
        st.rerun()
        
    df = obter_dados_persistentes("cache_manutencao", carregar_dados_manutencao)
    
    if df.empty:
        st.info("Nenhum chamado de manutenção registrado ainda.")
        return

    # CRIAÇÃO DAS SUB-ABAS
    tab_gestao, tab_indicadores = st.tabs(["🛠️ Controle de Chamados", "📊 Indicadores & Gráficos"])

    # =========================================================
    # ABA 1: CONTROLE (OPERACIONAL)
    # =========================================================
    with tab_gestao:
        st.markdown("### 📋 Fila de Chamados Pendentes")
        
        # Filtro visual da tabela
        filtro_status = st.radio("Filtrar Tabela:", ["Pendentes (Abertos/Andamento)", "Histórico Completo"], horizontal=True)
        
        if filtro_status == "Pendentes (Abertos/Andamento)":
            df_show = df[df['Status'].str.strip().str.lower() != 'concluido'].copy()
        else:
            df_show = df.copy()
        
        st.dataframe(
            df_show, 
            use_container_width=True,
            column_config={
                "Link_Foto": st.column_config.LinkColumn("Foto"),
                "Status": st.column_config.Column("Status", help="Situação Atual"),
                "Data_Abertura": st.column_config.DatetimeColumn("Abertura", format="D/M/Y H:m"),
            }
        )
        
        st.divider()
        
        # --- ÁREA DE EDIÇÃO (BAIXA DE CHAMADO) ---
        st.markdown("### ✍️ Editar / Dar Baixa")
        
        # Lista apenas não concluídos para facilitar a vida do gestor
        df_pendentes = df[df['Status'].str.strip().str.lower() != 'concluido'].reset_index()
        
        if df_pendentes.empty:
            st.success("🎉 Tudo limpo! Nenhuma manutenção pendente.")
        else:
            # Cria lista legível para o selectbox
            lista_opcoes = df_pendentes.apply(lambda x: f"ID {x['index']} | {x['Data_Abertura']} | {x['Maquina']} | {x['Descricao']}", axis=1).tolist()
            escolha = st.selectbox("Selecione o chamado para atuar:", lista_opcoes)
            
            if escolha:
                # Pega o ID real
                id_real = int(escolha.split("|")[0].replace("ID", "").strip())
                linha_atual = df.loc[id_real]
                
                with st.form("form_manutencao_baixa"):
                    st.info(f"Editando: **{linha_atual['Maquina']}** (Operador: {linha_atual['Operador']})")
                    st.caption(f"Problema: {linha_atual['Descricao']}")
                    
                    # 1. Status e Prioridade e Mecânico
                    c_m1, c_m2, c_m3 = st.columns(3)
                    with c_m1:
                        # Tenta manter o status atual se já existir, senão padrão é Aberto
                        status_atual = linha_atual.get('Status', 'Aberto')
                        lista_status = ["Aberto", "Em Andamento", "Concluido"]
                        idx_status = lista_status.index(status_atual) if status_atual in lista_status else 0
                        novo_status = st.selectbox("Status", lista_status, index=idx_status)
                        
                    with c_m2:
                        prioridade_atual = linha_atual.get('Prioridade', 'Media')
                        lista_prio = ["Baixa", "Media", "Alta"]
                        idx_prio = lista_prio.index(prioridade_atual) if prioridade_atual in lista_prio else 1
                        nova_prioridade = st.selectbox("Prioridade", lista_prio, index=idx_prio)
                        
                    with c_m3:
                        novo_mecanico = st.text_input("Mecânico Responsável", value=str(linha_atual.get('Mecanico', '')))
                    
                    st.markdown("---")
                    st.markdown("**⏱️ Apontamento de Horas**")

                    # 2. SEPARAÇÃO DE DATA E HORA (INÍCIO)
                    col_d_ini, col_h_ini = st.columns(2)
                    with col_d_ini:
                        d_ini_input = st.date_input("Data Início", value=datetime.now(FUSO_BR), format="DD/MM/YYYY")
                    with col_h_ini:
                        h_ini_input = st.time_input("Hora Início", value=datetime.now(FUSO_BR))

                    # 3. SEPARAÇÃO DE DATA E HORA (FIM)
                    col_d_fim, col_h_fim = st.columns(2)
                    with col_d_fim:
                        d_fim_input = st.date_input("Data Fim (Conclusão)", value=datetime.now(FUSO_BR), format="DD/MM/YYYY")
                    with col_h_fim:
                        h_fim_input = st.time_input("Hora Fim (Conclusão)", value=datetime.now(FUSO_BR))
                    
                    st.markdown("---")
                    nova_solucao = st.text_area("Solução Aplicada / Peças Trocadas", value=str(linha_atual.get('Solucao', '')))
                    
                    if st.form_submit_button("💾 Salvar Apontamento", type="primary"):
                        # --- CONCATENAÇÃO DOS DADOS ANTES DE SALVAR ---
                        # Formata para string: "DD/MM/AAAA HH:MM"
                        str_inicio = f"{d_ini_input.strftime('%d/%m/%Y')} {h_ini_input.strftime('%H:%M')}"
                        str_fim = f"{d_fim_input.strftime('%d/%m/%Y')} {h_fim_input.strftime('%H:%M')}"
                        
                        if atualizar_chamado_manutencao(id_real, novo_status, nova_prioridade, novo_mecanico, str_inicio, str_fim, nova_solucao):
                            st.success("✅ Chamado atualizado com sucesso!")
                            carregar_dados_manutencao.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Erro ao conectar com a planilha.")

    # =========================================================
    # ABA 2: INDICADORES (DASHBOARD)
    # =========================================================
    with tab_indicadores:
        st.markdown("### 📊 Dashboard da Manutenção")
        
        if df.empty:
            st.warning("Sem dados para gerar indicadores.")
        else:
            # =========================================================
            # 1. PROCESSAMENTO DOS DADOS (DATAS E HORAS)
            # =========================================================
            # CORREÇÃO 1: Pegamos agora SEM fuso horário para bater com a planilha
            agora_sem_fuso = datetime.now(FUSO_BR).replace(tzinfo=None)

            # Converte textos para data real
            df['Inicio_Dt'] = pd.to_datetime(df['Data_Inicio'], format='%d/%m/%Y %H:%M', errors='coerce')
            df['Fim_Dt'] = pd.to_datetime(df['Data_Fim'], format='%d/%m/%Y %H:%M', errors='coerce')
            
            # Para o Gráfico de Gantt: Se não tem data fim, usamos "Agora" (sem fuso)
            df['Fim_Visual'] = df['Fim_Dt'].fillna(agora_sem_fuso)
            
            # Calcula duração em Horas (para o Pareto de Tempo e MTTR)
            df['Duracao_Horas'] = (df['Fim_Dt'] - df['Inicio_Dt']).dt.total_seconds() / 3600
            
            # =========================================================
            # 2. INDICADORES KPI (MTTR / MTBF)
            # =========================================================
            df_concluido = df.dropna(subset=['Inicio_Dt', 'Fim_Dt'])
            
            # Cálculo MTTR (Média de horas por reparo)
            mttr_val = df_concluido['Duracao_Horas'].mean() if not df_concluido.empty else 0
            
            # Cálculo MTBF (Estimativa: Horas totais do período / Qtd Quebras)
            if len(df) > 1:
                inicio_periodo = df['Inicio_Dt'].min()
                # CORREÇÃO 2: Usamos a variável sem fuso para subtrair
                fim_periodo = agora_sem_fuso
                horas_totais_calendario = (fim_periodo - inicio_periodo).total_seconds() / 3600
                mtbf_val = horas_totais_calendario / len(df)
            else:
                mtbf_val = 0

            # Exibe os Cartões
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Chamados Totais", len(df))
            k2.metric("Concluídos", len(df_concluido))
            k3.metric("MTTR (Tempo Médio)", f"{mttr_val:.1f} h", help="Média de horas que a máquina fica parada consertando.")
            k4.metric("MTBF (Tempo Entre Falhas)", f"{mtbf_val:.1f} h", help="Em média, a cada quantas horas ocorre uma nova quebra.")
            
            st.divider()

            # =========================================================
            # 3. NOVOS GRÁFICOS
            # =========================================================
            
            # --- A. GRÁFICO DE GANTT (LINHA DO TEMPO) ---
            st.markdown("#### ⏳ Linha do Tempo de Paradas (Gantt)")
            st.caption("Visualize quando cada máquina parou e quanto tempo ficou parada.")
            
            df_gantt = df.dropna(subset=['Inicio_Dt']).copy()
            
            if not df_gantt.empty:
                gantt = alt.Chart(df_gantt).mark_bar().encode(
                    x=alt.X('Inicio_Dt', title='Início'),
                    x2='Fim_Visual', 
                    y=alt.Y('Maquina', title=None),
                    color=alt.Color('Tipo_Problema', legend=alt.Legend(title="Tipo")),
                    tooltip=['Maquina', 'Tipo_Problema', 'Operador', 'Status']
                ).properties(height=300)
                st.altair_chart(gantt, use_container_width=True)
            else:
                st.info("Sem datas de início válidas para gerar o Gantt.")

            st.divider()

            col_g1, col_g2 = st.columns(2)

            # --- B. PARETO DE TEMPO (HORAS PARADAS) ---
            with col_g1:
                st.markdown("#### 🕒 Horas Totais Paradas (Gargalo)")
                st.caption("Quais máquinas ficaram mais tempo sem produzir?")
                
                # Agrupa e soma as horas
                if not df_concluido.empty:
                    df_horas = df_concluido.groupby('Maquina')['Duracao_Horas'].sum().reset_index()
                    df_horas.columns = ['Maquina', 'Horas_Totais']
                    
                    graf_horas = alt.Chart(df_horas).mark_bar().encode(
                        x=alt.X('Horas_Totais', title='Horas Paradas'),
                        y=alt.Y('Maquina', sort='-x', title=None),
                        color=alt.value('#d62728'), # Vermelho
                        tooltip=['Maquina', 'Horas_Totais']
                    ).properties(height=300)
                    st.altair_chart(graf_horas, use_container_width=True)
                else:
                    st.info("Sem manutenções concluídas para calcular horas.")

            # --- C. PARETO DE QUANTIDADE ---
            with col_g2:
                st.markdown("#### 🔢 Quantidade de Quebras")
                st.caption("Quais máquinas quebram mais vezes?")
                
                df_qtd = df['Maquina'].value_counts().reset_index()
                df_qtd.columns = ['Maquina', 'Qtd']
                
                graf_qtd = alt.Chart(df_qtd).mark_bar().encode(
                    x=alt.X('Qtd', title='Nº de Chamados'),
                    y=alt.Y('Maquina', sort='-x', title=None),
                    color=alt.value('#0078D4'), # Azul
                    tooltip=['Maquina', 'Qtd']
                ).properties(height=300)
                st.altair_chart(graf_qtd, use_container_width=True)

# --- SESSÃO ---
if 'logado' not in st.session_state:
    st.session_state['logado'] = False
    st.session_state['usuario_nome'] = ""
    st.session_state['usuario_filtro'] = ""
    st.session_state['usuario_email'] = "" 
    st.session_state['usuario_tipo'] = ""
if 'fazendo_cadastro' not in st.session_state: st.session_state['fazendo_cadastro'] = False

# --- LOGIN ---
# --- LOGIN ---
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
        # =================================================================
        # TELA DE LOGIN: ALINHADA À ESQUERDA E COMPACTA
        # =================================================================
        
        # Cria duas colunas: A primeira estreita para o login, a segunda vazia para preencher o resto
        col_login, col_vazia = st.columns([1, 2]) 

        with col_login:
            st.markdown("<br>", unsafe_allow_html=True) 
            st.title("🔒 Login - Painel Dox")
            st.markdown("---")
            
            # Inputs
            u = st.text_input("Login", placeholder="Digite seu usuário").strip()
            s = st.text_input("Senha", type="password", placeholder="Digite sua senha").strip()
            
            st.markdown("<br>", unsafe_allow_html=True)

            # Botões
            c_btn1, c_btn2 = st.columns(2)
            with c_btn1:
                if st.button("Acessar", type="primary", use_container_width=True):
                    # Validação
                    df = carregar_usuarios()
                    if df.empty: st.error("Erro de conexão.")
                    elif 'Login' not in df.columns or 'Senha' not in df.columns: st.error("Erro técnico.")
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
                                    'usuario_login': d['Login']
                                })
                                registrar_acesso(u, d['Nome Vendedor'])
                                st.rerun()
                            else: st.error("Dados incorretos.")
                        except Exception as e:
                            st.error(f"Erro no login: {e}")
            
            with c_btn2:
                if st.button("Solicitar Acesso", use_container_width=True): 
                    st.session_state['fazendo_cadastro'] = True
                    st.rerun()
else:
    # =========================================================
    # VERIFICAÇÃO DO POP-UP DE AVISO (PASSO 3)
    # =========================================================
    if 'viu_aviso_servidor' not in st.session_state:
        df_avisos = obter_dados_persistentes("cache_avisos", carregar_feedbacks_avisos)
        ja_viu = False
        
        if isinstance(df_avisos, pd.DataFrame) and not df_avisos.empty:
            # Verifica se as colunas necessárias existem para não dar erro
            if 'Login' in df_avisos.columns and 'Tipo_Aviso' in df_avisos.columns:
                # Procura se já tem uma linha com o Login dele e o Tipo de Aviso "Status_Servidor"
                filtro = df_avisos[(df_avisos['Login'].str.lower() == st.session_state['usuario_login'].lower()) & 
                                   (df_avisos['Tipo_Aviso'] == 'Status_Servidor')]
                if not filtro.empty:
                    ja_viu = True
        
        if not ja_viu:
            popup_aviso_servidor()
        else:
            st.session_state['viu_aviso_servidor'] = True
    # =========================================================

    with st.sidebar:
        st.write(f"Bem-vindo, **{st.session_state['usuario_nome'].upper()}**")
        agora = datetime.now(FUSO_BR)
        dias_semana = {0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'}
        meses = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
        texto_data = f"{dias_semana[agora.weekday()]}, {agora.day} de {meses[agora.month]} de {agora.year}"
        
        # Juntamos a data e o perfil no mesmo bloco para economizar espaço
        st.markdown(f"<small><i>{texto_data}</i><br><span style='color: gray;'>Perfil: {st.session_state['usuario_tipo']}</span></small>", unsafe_allow_html=True)
        
        # =========================================================
        # STATUS DO SERVIDOR (ROBÔ) - COMPACTO
        # =========================================================
        df_status = obter_dados_persistentes("cache_status_robo", carregar_status_robo)
        status_texto = "🔴 Servidor Offline" 
        
        if isinstance(df_status, pd.DataFrame) and not df_status.empty and 'Ultima_Atualizacao' in df_status.columns:
            try:
                ultima_att_str = str(df_status.iloc[0]['Ultima_Atualizacao'])
                ultima_att_dt = datetime.strptime(ultima_att_str, '%d/%m/%Y %H:%M:%S')
                ultima_att_dt = FUSO_BR.localize(ultima_att_dt) 
                
                diferenca_minutos = (agora - ultima_att_dt).total_seconds() / 60
                
                if diferenca_minutos <= 25:
                    status_texto = "🟢 Servidor Online"
            except:
                pass
        
        # Exibe o status com uma margem pequena usando HTML (sem a linha gigante)
        st.markdown(f"<div style='margin-top: 15px; margin-bottom: 15px;'><b>{status_texto}</b></div>", unsafe_allow_html=True)
        # =========================================================

        # Botões lado a lado para economizar espaço
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            if st.button("Sair", use_container_width=True): 
                st.session_state.update({'logado': False, 'usuario_nome': ""})
                st.rerun()
        with col_btn2:
            if st.button("Atualizar", use_container_width=True): 
                st.cache_data.clear()
                st.rerun()
        
        st.divider() # Deixamos apenas UMA linha divisória antes do desempenho
        
        # --- BLOCO: FATURAMENTO DO VENDEDOR (VISÍVEL APENAS PARA VENDEDOR) ---
        if st.session_state['usuario_tipo'].lower() == "vendedor":
            df_fat_vend = carregar_faturamento_vendedores()
            
            if not df_fat_vend.empty and 'VENDEDOR' in df_fat_vend.columns and 'DATA_DT' in df_fat_vend.columns:
                usuario_atual = st.session_state['usuario_filtro']
                
                # Filtro Mês/Ano Corrente
                df_mes = df_fat_vend[
                    (df_fat_vend['DATA_DT'].dt.month == agora.month) & 
                    (df_fat_vend['DATA_DT'].dt.year == agora.year)
                ]
                
                # Filtro Usuário (Lógica de "Contém")
                df_mes['VENDEDOR_CLEAN'] = df_mes['VENDEDOR'].astype(str).str.upper().str.strip()
                user_clean = str(usuario_atual).upper().strip()
                
                df_user = df_mes[df_mes['VENDEDOR_CLEAN'].str.contains(user_clean, regex=False, na=False)]
                
                total_tons = df_user['TONS'].sum()
                
                st.markdown(f"### 🎯 Seu Desempenho")
                st.caption(f"Faturado em {meses[agora.month]}:")
                st.metric("Total (Tons)", f"{total_tons:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    if st.session_state['usuario_tipo'].lower() == "admin":
        # Adicionei "🔧 Manutenção" na lista
        a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📝 Acessos", "📑 Certificados", "🧾 Notas Fiscais", "🔍 Logs", "📊 Faturamento", "🏭 Produção", "🔧 Manutenção"])
        
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
        with a11: exibir_aba_manutencao() # <--- NOVA ABA AQUI
        
    elif st.session_state['usuario_tipo'].lower() == "master":
        a1, a2, a3, a4, a5, a6, a7, a8 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_estoque() # <--- NOVA ABA
        with a4: exibir_aba_fotos(False) # VISÃO NORMAL
        with a5: exibir_aba_certificados(False) 
        with a6: exibir_aba_notas(False)        
        with a7: exibir_aba_faturamento()
        with a8: exibir_aba_producao()

    elif st.session_state['usuario_tipo'].lower() in ["logística", "logistica", "pcp"]:
        a1, a2, a3, a4, a5 = st.tabs(["📂 Itens Programados", "📦 Estoque", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_estoque()
        with a3: exibir_aba_fotos(True) # Visão de gestão (vê tudo)
        with a4: exibir_aba_certificados(True) # Visão de gestão (vê tudo)
        with a5: exibir_aba_notas(True) # Visão de gestão (vê tudo)

    elif st.session_state['usuario_tipo'].lower() in ["manutenção", "manutencao"]:
        tabs_manu = st.tabs(["🔧 Manutenção"])
        with tabs_manu[0]: exibir_aba_manutencao()

    elif st.session_state['usuario_tipo'].lower() == "qualidade":
        a1, a2, a3 = st.tabs(["📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_aba_fotos(True) # Visão de gestão
        with a2: exibir_aba_certificados(True) # Visão de gestão
        with a3: exibir_aba_notas(True) # Visão de gestão    
        
    else:
        # Vendedores e Gerentes Padrão - ABA ESTOQUE ADICIONADA
        a1, a2, a3, a4, a5, a6 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📦 Estoque", "📷 Fotos RDQ", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_estoque() # <--- NOVA ABA
        with a4: exibir_aba_fotos(False) # VISÃO NORMAL
        with a5: exibir_aba_certificados(False)
        with a6: exibir_aba_notas(False)