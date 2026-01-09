import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import pytz
import altair as alt

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================
st.set_page_config(
    page_title="Painel do Vendedor Dox",
    page_icon="logodox.png",
    layout="wide"
)

FUSO_BR = pytz.timezone('America/Sao_Paulo')

try:
    st.logo("logodox.png")
except Exception:
    pass 

conn = st.connection("gsheets", type=GSheetsConnection)

# ==============================================================================
# FUNÇÕES DE LEITURA
# ==============================================================================

def carregar_dados_faturamento_nuvem():
    try:
        df = conn.read(worksheet="Dados_Faturamento", ttl=0)
        if df.empty: return pd.DataFrame()
        if df['TONS'].dtype == object: df['TONS'] = df['TONS'].astype(str).str.replace(',', '.')
        df['TONS'] = pd.to_numeric(df['TONS'], errors='coerce').fillna(0)
        df['DATA_EMISSAO'] = pd.to_datetime(df['DATA_EMISSAO'], format='%d/%m/%Y', errors='coerce')
        hoje = datetime.now()
        datas_fixas = [(hoje - timedelta(days=i)).strftime('%d/%m/%Y') for i in range(6, -1, -1)]
        df_base = pd.DataFrame({'Data_Str': datas_fixas})
        df['Data_Str'] = df['DATA_EMISSAO'].dt.strftime('%d/%m/%Y')
        df_agrupado = df.groupby('Data_Str')[['TONS']].sum().reset_index()
        df_final = pd.merge(df_base, df_agrupado, on='Data_Str', how='left')
        df_final['TONS'] = df_final['TONS'].fillna(0)
        def formatar_rotulo(row):
            valor_fmt = f"{row['TONS']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{row['Data_Str']}\n{valor_fmt}"
        df_final['Label_X'] = df_final.apply(formatar_rotulo, axis=1)
        return df_final
    except Exception as e:
        st.error(f"Erro ao ler faturamento da nuvem: {e}")
        return pd.DataFrame()

def carregar_dados_producao_nuvem():
    try:
        df = conn.read(worksheet="Dados_Producao", ttl=0)
        
        if df.empty: 
            return pd.DataFrame()

        # 1. Normaliza nomes das colunas (Maiusculo e sem espaços extras)
        df.columns = df.columns.str.strip().str.upper()

        # 2. Tratamento de Volume (Garante Float)
        if 'VOLUME' in df.columns:
            if df['VOLUME'].dtype == object: 
                df['VOLUME'] = pd.to_numeric(df['VOLUME'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        
        # 3. Tratamento de Data (Mais flexível)
        if 'DATA' in df.columns:
            df['DATA_DT'] = pd.to_datetime(df['DATA'], dayfirst=True, errors='coerce')
        
        return df

    except Exception as e:
        # AGORA VAI MOSTRAR O ERRO REAL NA TELA SE HOUVER
        st.error(f"Erro Técnico ao ler Produção: {e}") 
        return pd.DataFrame()

def carregar_usuarios():
    try:
        df_users = conn.read(worksheet="Usuarios", ttl=0)
        return df_users.astype(str)
    except: return pd.DataFrame()

def carregar_solicitacoes():
    try: return conn.read(worksheet="Solicitacoes", ttl=0)
    except: return pd.DataFrame(columns=["Nome", "Email", "Login", "Senha", "Data", "Status"])

def carregar_solicitacoes_fotos():
    try:
        df = conn.read(worksheet="Solicitacoes_Fotos", ttl=0)
        if not df.empty and "Lote" in df.columns: df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

def carregar_solicitacoes_certificados():
    try:
        df = conn.read(worksheet="Solicitacoes_Certificados", ttl=0)
        if not df.empty and "Lote" in df.columns: df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

def carregar_solicitacoes_notas():
    try:
        df = conn.read(worksheet="Solicitacoes_Notas", ttl=0)
        if not df.empty and "NF" in df.columns: df["NF"] = df["NF"].astype(str).str.replace("'", "") 
        return df
    except: return pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])

def carregar_logs_acessos():
    try:
        df = conn.read(worksheet="Acessos", ttl=0)
        if not df.empty and "Data" in df.columns:
             try:
                 df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors='coerce')
                 df = df.sort_values(by="Data_Dt", ascending=False).drop(columns=["Data_Dt"])
             except: pass
        return df
    except: return pd.DataFrame(columns=["Data", "Login", "Nome"])

def carregar_dados_pedidos():
    ABAS_MAQUINAS = ["Fagor", "Esquadros", "Marafon", "Divimec (Slitter)", "Divimec (Rebaixamento)"]
    dados_consolidados = []
    for aba in ABAS_MAQUINAS:
        try:
            df = conn.read(worksheet=aba, ttl=0, dtype=str)
            df['Máquina/Processo'] = aba
            cols_necessarias = ["Número do Pedido", "Cliente Correto", "Produto", "Quantidade", "Prazo", "Vendedor Correto", "Gerente Correto"]
            cols_existentes = [c for c in cols_necessarias if c in df.columns]
            if "Vendedor Correto" in cols_existentes:
                df_limpo = df[cols_existentes + ['Máquina/Processo']].copy()
                if "Número do Pedido" in df_limpo.columns:
                    df_limpo["Número do Pedido"] = df_limpo["Número do Pedido"].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)
                dados_consolidados.append(df_limpo)
        except: continue
    if dados_consolidados: return pd.concat(dados_consolidados, ignore_index=True)
    return pd.DataFrame()

# ==============================================================================
# FUNÇÕES DE ESCRITA
# ==============================================================================

def registrar_acesso(login, nome):
    try:
        try: df_logs = conn.read(worksheet="Acessos", ttl=0)
        except: df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        if df_logs.empty and "Data" not in df_logs.columns: df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        novo_log = pd.DataFrame([{"Data": agora_br, "Login": login, "Nome": nome}])
        df_final = pd.concat([df_logs, novo_log], ignore_index=True)
        conn.update(worksheet="Acessos", data=df_final)
    except: pass

def salvar_nova_solicitacao(nome, email, login, senha):
    try:
        df_existente = carregar_solicitacoes()
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Nome": nome, "Email": email, "Login": login, "Senha": senha, "Data": agora_br, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes", data=df_final)
        return True
    except Exception as e: st.error(f"Erro: {e}"); return False

def salvar_solicitacao_foto(vendedor_nome, vendedor_email, lote):
    try:
        try: df_existente = conn.read(worksheet="Solicitacoes_Fotos", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes_Fotos", data=df_final)
        return True
    except Exception as e: st.error(f"Erro: {e}"); return False

def salvar_solicitacao_certificado(vendedor_nome, vendedor_email, lote):
    try:
        try: df_existente = conn.read(worksheet="Solicitacoes_Certificados", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes_Certificados", data=df_final)
        return True
    except Exception as e: st.error(f"Erro: {e}"); return False

def salvar_solicitacao_nota(vendedor_nome, vendedor_email, nf_numero, filial):
    try:
        try: df_existente = conn.read(worksheet="Solicitacoes_Notas", ttl=0)
        except: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns: df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "NF", "Filial", "Status"])
        nf_str = f"'{nf_numero}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "NF": nf_str, "Filial": filial, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes_Notas", data=df_final)
        return True
    except Exception as e: st.error(f"Erro: {e}"); return False

def formatar_peso_brasileiro(valor):
    try:
        if pd.isna(valor) or valor == "": return "0"
        texto = f"{float(valor):.3f}"
        texto = texto.replace('.', ',').rstrip('0').rstrip(',')
        return texto
    except: return str(valor)

# ==============================================================================
# UI
# ==============================================================================

def exibir_aba_faturamento():
    st.subheader("📊 Ritmo de Faturamento - Pinheiral (Últimos 7 Dias)")
    if st.button("🔄 Atualizar Gráfico"):
        with st.spinner("Buscando dados sincronizados..."):
            df_fat = carregar_dados_faturamento_nuvem()
            st.session_state['dados_faturamento'] = df_fat
    if 'dados_faturamento' in st.session_state and not st.session_state['dados_faturamento'].empty:
        df_exibicao = st.session_state['dados_faturamento']
        total_periodo = df_exibicao['TONS'].sum()
        total_fmt = f"{total_periodo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        col1, col2 = st.columns(2)
        col1.metric("Total Faturado (7 dias)", f"{total_fmt} Ton")
        ordem_grafico = df_exibicao['Label_X'].tolist()
        grafico = alt.Chart(df_exibicao).mark_bar(size=40, color='#0078D4').encode(x=alt.X('Label_X', sort=ordem_grafico, axis=alt.Axis(title=None, labelAngle=0, labelExpr="split(datum.value, '\\n')")), y=alt.Y('TONS', title='Toneladas'), tooltip=['Data_Str', 'TONS']).properties(height=400)
        st.altair_chart(grafico, use_container_width=True)
    elif 'dados_faturamento' in st.session_state and st.session_state['dados_faturamento'].empty: st.warning("Nenhum faturamento recente encontrado na planilha de sincronização.")
    else: st.info("Clique no botão acima para carregar os indicadores.")

def exibir_aba_producao():
    st.subheader("🏭 Painel de Produção (Pinheiral)")
    if st.button("🔄 Atualizar Produção"):
        with st.spinner("Carregando indicadores..."):
            st.session_state['dados_producao'] = carregar_dados_producao_nuvem()
    
    if 'dados_producao' in st.session_state and not st.session_state['dados_producao'].empty:
        df = st.session_state['dados_producao']
        
        # Filtro de Período
        periodo = st.radio("Selecione o Período:", ["Últimos 7 Dias", "Acumulado Mês Corrente"], horizontal=True)
        
        # Lógica de Filtro
        if periodo == "Últimos 7 Dias":
            data_limite = datetime.now() - timedelta(days=6) # Hoje + 6 anteriores
            df_filtro = df[df['DATA_DT'] >= data_limite]
        else:
            data_limite = datetime.now().replace(day=1)
            df_filtro = df[df['DATA_DT'] >= data_limite]

        if df_filtro.empty:
            st.warning("Nenhum dado encontrado para este período.")
            return

        # KPIs Gerais
        total_prod = df_filtro['VOLUME'].sum()
        total_fmt = f"{total_prod:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        dias_unicos = df_filtro['DATA_DT'].nunique()
        media_diaria = total_prod / dias_unicos if dias_unicos > 0 else 0
        media_fmt = f"{media_diaria:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        k1, k2 = st.columns(2)
        k1.metric("Total Produzido", f"{total_fmt} Ton")
        k2.metric("Média Diária", f"{media_fmt} Ton")
        st.divider()

        # Gráficos Individuais por Máquina (Layout 2 Colunas)
        maquinas = sorted(df_filtro['MAQUINA'].unique())
        col_esq, col_dir = st.columns(2)
        
        for i, mq in enumerate(maquinas):
            df_mq = df_filtro[df_filtro['MAQUINA'] == mq]
            
            grafico = alt.Chart(df_mq).mark_bar().encode(
                x=alt.X('DATA', title=None, axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('VOLUME', title='Tons'),
                color=alt.Color('TURNO', legend=alt.Legend(title="Turno", orient='top')),
                tooltip=['DATA', 'TURNO', 'VOLUME']
            ).properties(
                title=f"Produção: {mq}",
                height=300
            )
            
            if i % 2 == 0:
                with col_esq: st.altair_chart(grafico, use_container_width=True)
            else:
                with col_dir: st.altair_chart(grafico, use_container_width=True)

    elif 'dados_producao' in st.session_state and st.session_state['dados_producao'].empty:
        st.warning("Nenhum dado na planilha de produção.")
    else:
        st.info("Clique no botão para carregar.")

def exibir_carteira_pedidos():
    titulo_prefixo = "Carteira de Pedidos"
    tipo_usuario = st.session_state['usuario_tipo'].lower()
    if "gerente" in tipo_usuario: titulo_prefixo = "Gerência de Carteira"
    st.title(f"{titulo_prefixo}: {st.session_state['usuario_nome']}")
    df_total = carregar_dados_pedidos()
    if df_total is not None and not df_total.empty:
        df_total = df_total.dropna(subset=["Número do Pedido"])
        df_total = df_total[~df_total["Número do Pedido"].isin(["000nan", "00None", "000000"])]
        nome_filtro = st.session_state['usuario_filtro']
        if tipo_usuario in ["admin", "gerente"]:
            vendedores_unicos = sorted(df_total["Vendedor Correto"].dropna().unique())
            filtro_vendedor = st.selectbox(f"Filtrar Vendedor ({tipo_usuario.capitalize()})", ["Todos"] + vendedores_unicos)
            if filtro_vendedor != "Todos": df_filtrado = df_total[df_total["Vendedor Correto"] == filtro_vendedor].copy()
            else: df_filtrado = df_total.copy()
        elif tipo_usuario == "gerente comercial":
            if "Gerente Correto" in df_total.columns: df_filtrado = df_total[df_total["Gerente Correto"].str.lower() == nome_filtro.lower()].copy()
            else: df_filtrado = pd.DataFrame()
        else: df_filtrado = df_total[df_total["Vendedor Correto"].str.lower().str.contains(nome_filtro.lower(), regex=False, na=False)].copy()

        if df_filtrado.empty: st.info(f"Nenhum pedido pendente encontrado.")
        else:
            df_filtrado['Quantidade_Num'] = pd.to_numeric(df_filtrado['Quantidade'], errors='coerce').fillna(0)
            df_filtrado['Peso (ton)'] = df_filtrado['Quantidade_Num'].apply(formatar_peso_brasileiro)
            try:
                df_filtrado['Prazo_dt'] = pd.to_datetime(df_filtrado['Prazo'], dayfirst=True, errors='coerce')
                df_filtrado['Prazo'] = df_filtrado['Prazo_dt'].dt.strftime('%d/%m/%Y').fillna("-")
            except: pass
            colunas_visiveis = ["Número do Pedido", "Cliente Correto", "Produto", "Peso (ton)", "Prazo", "Máquina/Processo"]
            if tipo_usuario in ["admin", "gerente", "gerente comercial"]: colunas_visiveis.insert(5, "Vendedor Correto")
            colunas_finais = [c for c in colunas_visiveis if c in df_filtrado.columns]
            df_final = df_filtrado[colunas_finais]
            total_pedidos = len(df_filtrado)
            total_peso = df_filtrado['Quantidade_Num'].sum()
            total_peso_str = f"{total_peso:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            kpi1, kpi2 = st.columns(2)
            kpi1.metric("Itens Programados:", total_pedidos)
            kpi2.metric("Volume Total (Tons):", total_peso_str)
            st.divider()
            texto_busca = st.text_input("🔍 Filtro:", placeholder="Digite cliente, pedido, produto ou máquina...")
            if texto_busca:
                mask = df_final.astype(str).apply(lambda x: x.str.contains(texto_busca, case=False, na=False)).any(axis=1)
                df_exibicao = df_final[mask]
            else: df_exibicao = df_final
            st.dataframe(df_exibicao, hide_index=True, use_container_width=True, column_config={"Prazo": st.column_config.TextColumn("Previsão")})
            if texto_busca and df_exibicao.empty: st.warning(f"Nenhum resultado encontrado para '{texto_busca}'")
    else: st.error("Não foi possível carregar a planilha de pedidos.")

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
        st.title("🔒 Login - Painel do Vendedor - Dox Brasil")
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            u = st.text_input("Login").strip()
            s = st.text_input("Senha", type="password").strip()
            if st.button("Acessar", type="primary"):
                df = carregar_usuarios()
                user = df[(df['Login'].str.lower() == u.lower()) & (df['Senha'] == s)]
                if not user.empty:
                    d = user.iloc[0]
                    st.session_state.update({'logado': True, 'usuario_nome': d['Nome Vendedor'].split()[0], 'usuario_filtro': d['Nome Vendedor'], 'usuario_email': d.get('Email', ''), 'usuario_tipo': d['Tipo']})
                    registrar_acesso(u, d['Nome Vendedor'])
                    st.rerun()
                else: st.error("Dados incorretos.")
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
        a1, a2, a3, a4, a5, a6, a7 = st.tabs(["📂 Itens Programados", "📝 Acessos", "📑 Certificados", "🧾 Notas Fiscais", "🔍 Logs", "📊 Faturamento", "🏭 Produção"])
        with a1: exibir_carteira_pedidos()
        with a2: st.dataframe(carregar_solicitacoes(), use_container_width=True)
        with a3: exibir_aba_certificados(True)
        with a4: exibir_aba_notas(True) 
        with a5: st.dataframe(carregar_logs_acessos(), use_container_width=True)
        with a6: exibir_aba_faturamento()
        with a7: exibir_aba_producao()
    else:
        a1, a2, a3 = st.tabs(["📂 Itens Programados", "📑 Certificados", "🧾 Notas Fiscais"])
        with a1: exibir_carteira_pedidos()
        with a2: exibir_aba_certificados(False)
        with a3: exibir_aba_notas(False)