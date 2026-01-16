import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import pytz
import altair as alt
import time

# ==============================================================================
# CONFIGURAÇÕES GERAIS E URLS
# ==============================================================================
st.set_page_config(
    page_title="Painel Dox",
    page_icon="logodox.png",
    layout="wide"
)

FUSO_BR = pytz.timezone('America/Sao_Paulo')

# --- USANDO URLS COMPLETAS PARA EVITAR ERRO DE LEITURA ---
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

conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# FUNÇÕES DE LEITURA BLINDADAS (COM RETRY LOGIC)
# ==============================================================================

@st.cache_data(ttl="10m", show_spinner=False)
def ler_dados_nuvem_generico(aba, url_planilha):
    for tentativa in range(3):
        try:
            df = conn.read(spreadsheet=url_planilha, worksheet=aba, ttl=0)
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                if 'TONS' in df.columns:
                    df['TONS'] = df['TONS'].astype(str).str.replace(',', '.')
                    df['TONS'] = pd.to_numeric(df['TONS'], errors='coerce').fillna(0)
                if 'DATA_EMISSAO' in df.columns:
                    df['DATA_DT'] = pd.to_datetime(df['DATA_EMISSAO'], dayfirst=True, errors='coerce')
                return df
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()

def carregar_dados_faturamento_direto():
    return ler_dados_nuvem_generico("Dados_Faturamento", URL_SISTEMA)

def carregar_dados_faturamento_transf():
    return ler_dados_nuvem_generico("Dados_Faturamento_Transf", URL_SISTEMA)

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_metas_faturamento():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Metas_Faturamento", ttl=0)
        if df.empty: return pd.DataFrame(columns=['FILIAL', 'META'])
        df.columns = df.columns.str.strip().str.upper()
        if 'META' in df.columns:
             df['META'] = pd.to_numeric(df['META'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        return df
    except:
        return pd.DataFrame(columns=['FILIAL', 'META'])

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_dados_producao_nuvem():
    for tentativa in range(3):
        try:
            df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Dados_Producao", ttl=0)
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                if 'VOLUME' in df.columns:
                    df['VOLUME'] = pd.to_numeric(df['VOLUME'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
                if 'DATA' in df.columns:
                    df['DATA_DT'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
                return df
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()

@st.cache_data(ttl="10m", show_spinner=False)
def carregar_metas_producao():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Metas_Producao", ttl=0)
        if df.empty: return pd.DataFrame(columns=['MAQUINA', 'META'])
        df.columns = df.columns.str.strip().str.upper()
        if 'META' in df.columns:
             df['META'] = pd.to_numeric(df['META'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        return df
    except:
        return pd.DataFrame(columns=['MAQUINA', 'META'])

def carregar_usuarios():
    try:
        df_users = conn.read(spreadsheet=URL_SISTEMA, worksheet="Usuarios", ttl=0)
        return df_users.astype(str)
    except Exception as e:
        print(f"Erro conexao usuarios: {e}")
        return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes():
    try: 
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes", ttl=0)
        return df
    except Exception as e:
        return pd.DataFrame(columns=["Nome", "Email", "Login", "Senha", "Data", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_fotos():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Fotos", ttl=0)
        if not df.empty:
            cols_map = {c: c.strip() for c in df.columns}
            df = df.rename(columns=cols_map)
            if "Lote" in df.columns: 
                df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except Exception as e: 
        return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_certificados():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Certificados", ttl=0)
        if not df.empty:
            cols_map = {c: c.strip() for c in df.columns}
            df = df.rename(columns=cols_map)
            if "Lote" in df.columns: 
                df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except Exception as e: 
        return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_solicitacoes_notas():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Notas", ttl=0)
        if not df.empty:
            cols_map = {c: c.strip() for c in df.columns}
            df = df.rename(columns=cols_map)
            if "NF" in df.columns: 
                df["NF"] = df["NF"].astype(str).str.replace("'", "") 
        return df
    except Exception as e: 
        return pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_logs_acessos():
    try:
        df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Acessos", ttl=0)
        if not df.empty:
             df.columns = df.columns.str.strip()
             if "Data" in df.columns:
                 try:
                     df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors='coerce')
                     df = df.sort_values(by="Data_Dt", ascending=False).drop(columns=["Data_Dt"])
                 except: pass
        return df
    except Exception as e: 
        return pd.DataFrame(columns=["Data", "Login", "Nome"])

@st.cache_data(ttl="15m", show_spinner=False)
def carregar_dados_pedidos():
    dados_consolidados = []
    for aba in ABAS_PINHEIRAL:
        try:
            df = conn.read(spreadsheet=URL_PINHEIRAL, worksheet=aba, ttl=0, dtype=str)
            if not df.empty:
                df['Máquina/Processo'] = aba
                df['Filial_Origem'] = "PINHEIRAL"
                cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
                cols_existentes = [c for c in cols_necessarias if c in df.columns]
                if "Vendedor Correto" in cols_existentes:
                    df_limpo = df[cols_existentes + ['Máquina/Processo', 'Filial_Origem']].copy()
                    if "Número do Pedido" in df_limpo.columns:
                        df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                    dados_consolidados.append(df_limpo)
        except: continue
    for aba in ABAS_BICAS:
        try:
            df = conn.read(spreadsheet=URL_BICAS, worksheet=aba, ttl=0, dtype=str)
            if not df.empty:
                df['Máquina/Processo'] = aba
                df['Filial_Origem'] = "SJ BICAS"
                cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
                cols_existentes = [c for c in cols_necessarias if c in df.columns]
                if "Vendedor Correto" in cols_existentes:
                    df_limpo = df[cols_existentes + ['Máquina/Processo', 'Filial_Origem']].copy()
                    if "Número do Pedido" in df_limpo.columns:
                        df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                    dados_consolidados.append(df_limpo)
        except: continue
    if dados_consolidados: return pd.concat(dados_consolidados, ignore_index=True)
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_credito():
    for tentativa in range(3):
        try:
            df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Dados_Credito", ttl=0, dtype=str)
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                return df
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_carteira():
    for tentativa in range(3):
        try:
            df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Dados_Carteira", ttl=0, dtype=str)
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                return df
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()

@st.cache_data(ttl="5m", show_spinner=False)
def carregar_dados_titulos():
    for tentativa in range(3):
        try:
            df = conn.read(spreadsheet=URL_SISTEMA, worksheet="Dados_Titulos", ttl=0, dtype=str)
            if not df.empty:
                df.columns = df.columns.str.strip().str.upper()
                return df
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(2)
    return pd.DataFrame()

# ==============================================================================
# FUNÇÕES DE ESCRITA
# ==============================================================================

def salvar_metas_faturamento(dicionario_metas):
    try:
        df_novo = pd.DataFrame(list(dicionario_metas.items()), columns=['FILIAL', 'META'])
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Metas_Faturamento", data=df_novo)
        return True
    except Exception as e: return False

def salvar_metas_producao(dicionario_metas):
    try:
        df_novo = pd.DataFrame(list(dicionario_metas.items()), columns=['MAQUINA', 'META'])
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Metas_Producao", data=df_novo)
        return True
    except Exception as e: return False

def registrar_acesso(login, nome):
    try:
        try: df_logs = conn.read(spreadsheet=URL_SISTEMA, worksheet="Acessos", ttl=0)
        except: df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        if df_logs.empty and "Data" not in df_logs.columns: df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        novo_log = pd.DataFrame([{"Data": agora_br, "Login": login, "Nome": nome}])
        df_final = pd.concat([df_logs, novo_log], ignore_index=True)
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Acessos", data=df_final)
    except: pass

def salvar_nova_solicitacao(nome, email, login, senha):
    try:
        df_existente = carregar_solicitacoes()
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Nome": nome, "Email": email, "Login": login, "Senha": senha, "Data": agora_br, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes", data=df_final)
        return True
    except Exception as e: return False

def salvar_solicitacao_foto(vendedor_nome, vendedor_email, lote):
    try:
        try: df_existente = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Fotos", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Fotos", data=df_final)
        return True
    except Exception as e: return False

def salvar_solicitacao_certificado(vendedor_nome, vendedor_email, lote):
    try:
        try: df_existente = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Certificados", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Certificados", data=df_final)
        return True
    except Exception as e: return False

def salvar_solicitacao_nota(vendedor_nome, vendedor_email, nf_numero, filial):
    try:
        try: df_existente = conn.read(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Notas", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])
        nf_str = f"'{nf_numero}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "NF": nf_str, "Filial": filial, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(spreadsheet=URL_SISTEMA, worksheet="Solicitacoes_Notas", data=df_final)
        return True
    except Exception as e: return False

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
            st.session_state['dados_faturamento'] = carregar_dados_faturamento_direto()
            st.session_state['dados_faturamento_transf'] = carregar_dados_faturamento_transf()
            st.session_state['metas_faturamento'] = carregar_metas_faturamento()
    if 'metas_faturamento' not in st.session_state: st.session_state['metas_faturamento'] = carregar_metas_faturamento()
    if 'dados_faturamento' not in st.session_state: st.session_state['dados_faturamento'] = carregar_dados_faturamento_direto()
    if 'dados_faturamento_transf' not in st.session_state: st.session_state['dados_faturamento_transf'] = carregar_dados_faturamento_transf()
    with st.expander("⚙️ Definir Meta (tons)"):
        with st.form("form_metas_fat"):
            st.caption("Defina a meta diária de faturamento para PINHEIRAL (Direto).")
            novas_metas = {}
            valor_atual = 0.0
            df_m = st.session_state['metas_faturamento']
            if not df_m.empty:
                filtro = df_m[df_m['FILIAL'] == 'PINHEIRAL']
                if not filtro.empty: valor_atual = float(filtro.iloc[0]['META'])
            novas_metas['PINHEIRAL'] = st.number_input("PINHEIRAL", value=valor_atual, step=1.0, min_value=0.0)
            if st.form_submit_button("💾 Salvar Metas"):
                if salvar_metas_faturamento(novas_metas):
                    st.success("Meta atualizada!")
                    st.session_state['metas_faturamento'] = carregar_metas_faturamento()
                    st.rerun()
    st.divider()
    periodo = st.radio("Selecione o Período:", ["Últimos 7 Dias", "Acumulado Mês Corrente"], horizontal=True, key="fat_periodo")
    hoje_normalizado = datetime.now(FUSO_BR).replace(hour=0, minute=0, second=0, microsecond=0)
    if periodo == "Últimos 7 Dias": data_limite = hoje_normalizado - timedelta(days=6)
    else: data_limite = hoje_normalizado.replace(day=1)
    df_direto = st.session_state['dados_faturamento']
    if not df_direto.empty:
        df_filtro_direto = df_direto[df_direto['DATA_DT'].dt.date >= data_limite.date()]
        meta_direto = 0
        df_m = st.session_state['metas_faturamento']
        if not df_m.empty:
            fmeta = df_m[df_m['FILIAL'] == 'PINHEIRAL']
            if not fmeta.empty: meta_direto = float(fmeta.iloc[0]['META'])
        plotar_grafico_faturamento(df_filtro_direto, "Faturamento Direto: Pinheiral", meta_direto)
    else: st.info("Sem dados de Faturamento Direto carregados.")
    df_transf = st.session_state['dados_faturamento_transf']
    if not df_transf.empty:
        df_filtro_transf = df_transf[df_transf['DATA_DT'].dt.date >= data_limite.date()]
        plotar_grafico_faturamento(df_filtro_transf, "Faturamento Transferência: Pinheiral", meta_valor=None) 
    else: st.info("Sem dados de Transferência carregados.")

def exibir_aba_producao():
    st.subheader("🏭 Painel de Produção (Pinheiral)")
    if st.button("🔄 Atualizar Produção"):
        with st.spinner("Carregando indicadores..."):
            st.session_state['dados_producao'] = carregar_dados_producao_nuvem()
            st.session_state['metas_producao'] = carregar_metas_producao()
    if 'metas_producao' not in st.session_state: st.session_state['metas_producao'] = carregar_metas_producao()
    with st.expander("⚙️ Definir Metas Diárias (Tons)"):
        if 'dados_producao' in st.session_state and not st.session_state['dados_producao'].empty:
            lista_maquinas = sorted(st.session_state['dados_producao']['MAQUINA'].unique())
        else: lista_maquinas = ["Divimec 1", "Divimec 2", "Endireitadeira", "Esquadros", "Fagor", "Marafon"]
        with st.form("form_metas"):
            st.caption("Defina a meta diária (Tons) para cada máquina.")
            novas_metas = {}
            cols = st.columns(3)
            df_metas_atual = st.session_state['metas_producao']
            for i, mq in enumerate(lista_maquinas):
                valor_atual = 0.0
                if not df_metas_atual.empty:
                    filtro = df_metas_atual[df_metas_atual['MAQUINA'] == mq]
                    if not filtro.empty: valor_atual = float(filtro.iloc[0]['META'])
                with cols[i % 3]: novas_metas[mq] = st.number_input(f"{mq}", value=valor_atual, step=1.0, min_value=0.0)
            if st.form_submit_button("💾 Salvar Metas"):
                if salvar_metas_producao(novas_metas): st.success("Metas atualizadas!"); st.session_state['metas_producao'] = carregar_metas_producao(); st.rerun()
    st.divider()
    if 'dados_producao' in st.session_state and not st.session_state['dados_producao'].empty:
        df = st.session_state['dados_producao']
        df_metas = st.session_state['metas_producao']
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

def exibir_carteira_pedidos():
    tipo_usuario = st.session_state['usuario_tipo'].lower()
    df_total = carregar_dados_pedidos()
    if df_total is not None and not df_total.empty:
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
            df_filtrado['Quantidade_Num'] = pd.to_numeric(df_filtrado['Quantidade'], errors='coerce').fillna(0)
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
    else: st.error("Não foi possível carregar a planilha de pedidos.")

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
        
        **DISPONÍVEL VIA LC2**: Valor disponível para faturar usando o limite de crédito DOX, já considerando títulos em aberto.
        
        **DISPONÍVEL BV**: Valor disponível para faturar usando a modalidade BV (Banco/Vendor).
        
        **DISPONÍVEL VIA RA**: Valor disponível para faturar via RA (recebimento antecipado), desde que não existam atrasos.
        
        **SALDO_A_VENCER**: Valor total de títulos que ainda vão vencer no futuro (não estão atrasados).
        
        **DIAS_PARA_VENCER_TITULO**: Quantidade de dias para o próximo título vencer. Ajuda a prever risco de atraso.
        
        **DATA_VENCIMENTO_MAIS_ANTIGA**: Data do título vencido mais antigo do cliente. Indica há quanto tempo existe inadimplência.
        
        **LC_DOX**: Limite de crédito DOX ainda disponível após considerar os títulos em aberto.
        
        **LC_BV**: Limite total disponível para faturamento via BV.
        
        **LC_TOTAL**: Valor total do limite de crédito concedido ao cliente.
        
        **RA**: Valor total de títulos do tipo RA (recebimento antecipado) ainda em aberto.
        
        **EM ABERTO**: Soma de todos os títulos em aberto do cliente, independentemente do vencimento.
        
        **EM ABERTO BV**: Valor total de títulos em aberto vinculados à modalidade BV.
        """)

    # 1. Carrega Dados (Com Retry Logic)
    df_credito = carregar_dados_credito()
    df_carteira = carregar_dados_carteira()
    df_titulos_geral = carregar_dados_titulos() # Carrega base de títulos
    
    if df_credito.empty:
        st.info("Nenhuma informação de crédito disponível no momento (Aguardando sincronização do Robô).")
        return

    # 2. Definição das Colunas
    cols_order = [
        "CNPJ", "CLIENTE", "VENDEDOR", "GERENTE", "RISCO_DE_BLOQUEIO", "ACAO_SUGERIDA", "MOTIVO_PROVAVEL_DO_BLOQUEIO",
        "OPCAO_DE_FATURAMENTO", "RECEBIVEIS", "DIAS_EM_ATRASO_RECEBIVEIS", "SALDO_VENCIDO", "VENCIMENTO LC",
        "DIAS_PARA_VENCER_LC", "DATA_VENC_LC", "DISPONIVEL VIA LC2", "DISPONIVEL BV", "DISPONIVEL VIA RA",
        "SALDO_A_VENCER", "DIAS_PARA_VENCER_TITULO", "DATA_VENCIMENTO_MAIS_ANTIGA", "LC DOX", "LC BV", "LC TOTAL",
        "RA", "EM_ABERTO", "EM ABERTO BV"
    ]
    cols_financeiras = [
        "SALDO_VENCIDO", "SALDO_A_VENCER", "LC TOTAL", "LC DOX", "RA", 
        "EM_ABERTO", "DISPONIVEL VIA RA", "DISPONIVEL VIA LC2", "LC BV", 
        "EM ABERTO BV", "DISPONIVEL BV"
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

    # --- INSERÇÃO DA COLUNA ISCA (AJUSTADA V62) ---
    # Inserimos a coluna "DETALHES" na posição 0 com a seta
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

    # Configuração das Colunas (V62 - DETALHES AJUSTADO)
    config_colunas = {
        "DETALHES": st.column_config.TextColumn("", help="Clique na caixa de seleção à esquerda para ver os títulos.", width="medium"),
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
        "EM ABERTO BV": st.column_config.TextColumn("EM ABERTO BV", help="Total em aberto BV.")
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
    st.subheader("📷 Solicitação de Fotos (Material em RDQ)")
    st.markdown("Digite o número do Lote/Bobina abaixo para solicitar fotos de materiais defeituosos.")
    with st.form("form_foto"):
        col_f1, col_f2 = st.columns([1, 2])
        with col_f1: lote_input = st.text_input("Lote / Bobina:")
        with col_f2: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''))
        if st.form_submit_button("Solicitar Fotos", type="primary"):
            if not lote_input: st.warning("Digite o lote.")
            elif not email_input: st.warning("Preencha o e-mail.")
            elif salvar_solicitacao_foto(st.session_state['usuario_nome'], email_input, lote_input): st.success(f"Solicitação do lote **{lote_input}** enviada!")
    if is_admin:
        st.divider()
        st.markdown("### 🛠️ Gestão de Pedidos de Fotos (Visão Admin)")
        df_fotos = carregar_solicitacoes_fotos()
        if not df_fotos.empty:
            st.dataframe(df_fotos, use_container_width=True, column_config={"Lote": st.column_config.TextColumn("Lote")})
            if st.button("Atualizar Lista de Fotos"): st.cache_data.clear(); st.rerun()
        else: st.info("Nenhum pedido de foto registrado.")

def exibir_aba_certificados(is_admin=False):
    st.subheader("📑 Solicitação de Certificados de Qualidade")
    st.markdown("Digite o número do Lote/Bobina para receber o certificado de qualidade.")
    with st.form("form_certificado"):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1: 
            lote_cert = st.text_input("Lote / Bobina (Certificado):")
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
        if st.button("Atualizar Lista de Certificados"): st.cache_data.clear(); st.rerun()
    else: st.info("Nenhum pedido encontrado.")

def exibir_aba_notas(is_admin=False):
    st.subheader("🧾 Solicitação de Nota Fiscal (PDF)")
    st.markdown("Digite o número da Nota Fiscal para receber o PDF por e-mail. **Atenção:** Por segurança, o sistema só enviará notas que pertençam à sua carteira de clientes.")
    with st.form("form_notas"):
        col_n1, col_n2, col_n3 = st.columns([1, 1, 1])
        with col_n1: filial_input = st.selectbox("Selecione a Filial:", ["PINHEIRAL", "SJ BICAS", "SF DO SUL"])
        with col_n2: nf_input = st.text_input("Número da NF (Ex: 71591):")
        with col_n3: email_input = st.text_input("Enviar para o e-mail:", value=st.session_state.get('usuario_email', ''), key="email_nf")
        if st.form_submit_button("Solicitar NF", type="primary"):
            if not nf_input: st.warning("Digite o número da nota.")
            elif not email_input: st.warning("Preencha o e-mail.")
            elif salvar_solicitacao_nota(st.session_state['usuario_nome'], email_input, nf_input, filial_input): st.success(f"Solicitação da NF **{nf_input}** ({filial_input}) enviada!")
    st.divider()
    if is_admin: st.markdown("### 🛠️ Histórico de Solicitações (Visão Admin)")
    else: st.markdown("### 📜 Meus Pedidos de Notas")
    df_notas = carregar_solicitacoes_notas()
    if not df_notas.empty and not is_admin:
        user_email = st.session_state.get('usuario_email', '')
        if 'Email' in df_notas.columns: df_notas = df_notas[df_notas['Email'].str.lower() == user_email.lower()]
    if not df_notas.empty:
        st.dataframe(df_notas, use_container_width=True, column_config={"NF": st.column_config.TextColumn("NF")})
        if st.button("Atualizar Lista de Notas"): st.cache_data.clear(); st.rerun()
    else: st.info("Nenhum pedido encontrado.")

# --- SESSÃO ---
if 'logado' not in st.session_state:
    st.session_state['logado'] = False
    st.session_state['usuario_nome'] = ""
    st.session_state['usuario_filtro'] = ""
    st.session_state['usuario_email'] = "" 
    st.session_state['usuario_tipo'] = ""
if 'fazendo_cadastro' not in st.session_state: st.session_state['fazendo_cadastro'] = False

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
        st.title("🔒 Login - Painel Dox")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            u = st.text_input("Login").strip()
            s = st.text_input("Senha", type="password").strip()
            if st.button("Acessar", type="primary"):
                df = carregar_usuarios()
                if df.empty:
                    st.error("Falha temporária de conexão. Por favor, tente novamente.")
                elif 'Login' not in df.columns or 'Senha' not in df.columns:
                    st.error("Erro técnico na validação do login. Contate o suporte.")
                else:
                    try:
                        user = df[(df['Login'].str.lower() == u.lower()) & (df['Senha'] == s)]
                        if not user.empty:
                            d = user.iloc[0]
                            st.session_state.update({'logado': True, 'usuario_nome': d['Nome Vendedor'].split()[0], 'usuario_filtro': d['Nome Vendedor'], 'usuario_email': d.get('Email', ''), 'usuario_tipo': d['Tipo']})
                            registrar_acesso(u, d['Nome Vendedor'])
                            st.rerun()
                        else: st.error("Dados incorretos.")
                    except Exception as e:
                        st.error("Erro ao processar login. Tente novamente.")
            st.markdown("---")
            if st.button("Solicitar Acesso"): st.session_state['fazendo_cadastro'] = True; st.rerun()
else:
    with st.sidebar:
        st.write(f"Bem-vindo, **{st.session_state['usuario_nome'].upper()}**")
        agora = datetime.now(FUSO_BR)
        dias_semana = {0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'}
        meses = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
        texto_data = f"{dias_semana[agora.weekday()]}, {agora.day} de {meses[agora.month]} de {agora.year}"
        st.markdown(f"<small><i>{texto_data}</i></small>", unsafe_allow_html=True)
        st.caption(f"Perfil: {st.session_state['usuario_tipo']}")
        if st.button("Sair"): st.session_state.update({'logado': False, 'usuario_nome': ""}); st.rerun()
        st.divider()
        if st.button("🔄 Atualizar Dados"): st.cache_data.clear(); st.rerun()

    if st.session_state['usuario_tipo'].lower() == "admin":
        a1, a2, a3, a4, a5, a6, a7, a8 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📝 Acessos", "📑 Certificados", "🧾 Notas Fiscais", "🔍 Logs", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: st.dataframe(carregar_solicitacoes(), use_container_width=True)
        with a4: exibir_aba_certificados(True)
        with a5: exibir_aba_notas(True) 
        with a6: st.dataframe(carregar_logs_acessos(), use_container_width=True)
        with a7: exibir_aba_faturamento()
        with a8: exibir_aba_producao()
        
    elif st.session_state['usuario_tipo'].lower() == "master":
        a1, a2, a3, a4, a5, a6 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📑 Certificados", "🧾 Notas Fiscais", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_certificados(False) 
        with a4: exibir_aba_notas(False)        
        with a5: exibir_aba_faturamento()
        with a6: exibir_aba_producao()
        
    else:
        # Vendedores e Gerentes Padrão
        a1, a2, a3, a4 = st.tabs(["📂 Itens Programados", "💰 Crédito", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_credito()
        with a3: exibir_aba_certificados(False)
        with a4: exibir_aba_notas(False)