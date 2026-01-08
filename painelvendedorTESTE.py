import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime, timedelta
import pytz
import altair as alt

# ==============================================================================
# CONFIGURAÇÕES GERAIS E ÍCONE
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
# FUNÇÕES DE BANCO DE DADOS (LEITURA)
# ==============================================================================

def carregar_dados_faturamento_nuvem():
    try:
        df = conn.read(worksheet="Dados_Faturamento", ttl=0)
        
        if df.empty:
            return pd.DataFrame()

        # 1. Tratamento básico
        if df['TONS'].dtype == object:
             df['TONS'] = df['TONS'].astype(str).str.replace(',', '.')
        df['TONS'] = pd.to_numeric(df['TONS'], errors='coerce').fillna(0)
        df['DATA_EMISSAO'] = pd.to_datetime(df['DATA_EMISSAO'], format='%d/%m/%Y', errors='coerce')
        
        # 2. CRIAR RÉGUA DE DATAS (ÚLTIMOS 7 DIAS FIXOS)
        hoje = datetime.now()
        datas_fixas = [(hoje - timedelta(days=i)).strftime('%d/%m/%Y') for i in range(6, -1, -1)]
        df_base = pd.DataFrame({'Data_Str': datas_fixas})
        
        # 3. Preparar dados do banco
        df['Data_Str'] = df['DATA_EMISSAO'].dt.strftime('%d/%m/%Y')
        df_agrupado = df.groupby('Data_Str')[['TONS']].sum().reset_index()
        
        # 4. CRUZAMENTO (MERGE) E CRIAÇÃO DO RÓTULO COM VALOR
        df_final = pd.merge(df_base, df_agrupado, on='Data_Str', how='left')
        df_final['TONS'] = df_final['TONS'].fillna(0)
        
        # Cria a coluna Label: Data + Quebra de Linha + Valor Formatado
        def formatar_rotulo(row):
            valor_fmt = f"{row['TONS']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return f"{row['Data_Str']}\n{valor_fmt}"
            
        df_final['Label_X'] = df_final.apply(formatar_rotulo, axis=1)
        
        return df_final

    except Exception as e:
        st.error(f"Erro ao ler faturamento da nuvem: {e}")
        return pd.DataFrame()

def carregar_usuarios():
    try:
        df_users = conn.read(worksheet="Usuarios", ttl=0)
        df_users = df_users.astype(str)
        return df_users
    except Exception as e:
        st.error(f"Erro ao carregar base de usuários. Verifique se a aba 'Usuarios' existe.")
        return pd.DataFrame()

def carregar_solicitacoes():
    try:
        df = conn.read(worksheet="Solicitacoes", ttl=0)
        return df
    except Exception:
        return pd.DataFrame(columns=["Nome", "Email", "Login", "Senha", "Data", "Status"])

def carregar_solicitacoes_fotos():
    try:
        df = conn.read(worksheet="Solicitacoes_Fotos", ttl=0)
        if not df.empty and "Lote" in df.columns:
            df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except Exception:
        return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

def carregar_solicitacoes_certificados():
    try:
        df = conn.read(worksheet="Solicitacoes_Certificados", ttl=0)
        if not df.empty and "Lote" in df.columns:
            df["Lote"] = df["Lote"].astype(str).str.replace("'", "") 
        return df
    except Exception:
        return pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])

def carregar_logs_acessos():
    try:
        df = conn.read(worksheet="Acessos", ttl=0)
        if not df.empty and "Data" in df.columns:
             try:
                 df["Data_Dt"] = pd.to_datetime(df["Data"], dayfirst=True, errors='coerce')
                 df = df.sort_values(by="Data_Dt", ascending=False).drop(columns=["Data_Dt"])
             except:
                 pass
        return df
    except Exception:
        return pd.DataFrame(columns=["Data", "Login", "Nome"])

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
                    df_limpo["Número do Pedido"] = (
                        df_limpo["Número do Pedido"]
                        .astype(str)
                        .str.replace(r'\.0$', '', regex=True)
                        .str.strip()
                        .str.zfill(6)
                    )
                dados_consolidados.append(df_limpo)
        except Exception:
            continue
            
    if dados_consolidados:
        return pd.concat(dados_consolidados, ignore_index=True)
    return pd.DataFrame()

# ==============================================================================
# FUNÇÕES DE BANCO DE DADOS (ESCRITA)
# ==============================================================================

def registrar_acesso(login, nome):
    try:
        try:
            df_logs = conn.read(worksheet="Acessos", ttl=0)
        except:
            df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        if df_logs.empty and "Data" not in df_logs.columns:
             df_logs = pd.DataFrame(columns=["Data", "Login", "Nome"])
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M:%S")
        novo_log = pd.DataFrame([{"Data": agora_br, "Login": login, "Nome": nome}])
        df_final = pd.concat([df_logs, novo_log], ignore_index=True)
        conn.update(worksheet="Acessos", data=df_final)
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

def salvar_nova_solicitacao(nome, email, login, senha):
    try:
        df_existente = carregar_solicitacoes()
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Nome": nome, "Email": email, "Login": login, "Senha": senha, "Data": agora_br, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes", data=df_final)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar solicitação: {e}")
        return False

def salvar_solicitacao_foto(vendedor_nome, vendedor_email, lote):
    try:
        try:
            df_existente = conn.read(worksheet="Solicitacoes_Fotos", ttl=0)
        except:
            df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns:
             df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes_Fotos", data=df_final)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar pedido de foto: {e}")
        return False

def salvar_solicitacao_certificado(vendedor_nome, vendedor_email, lote):
    try:
        try:
            df_existente = conn.read(worksheet="Solicitacoes_Certificados", ttl=0)
        except:
            df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        if df_existente.empty and "Data" not in df_existente.columns:
             df_existente = pd.DataFrame(columns=["Data", "Vendedor", "Email", "Lote", "Status"])
        lote_formatado = f"'{lote}"
        agora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")
        nova_linha = pd.DataFrame([{"Data": agora_br, "Vendedor": vendedor_nome, "Email": vendedor_email, "Lote": lote_formatado, "Status": "Pendente"}])
        df_final = pd.concat([df_existente, nova_linha], ignore_index=True)
        conn.update(worksheet="Solicitacoes_Certificados", data=df_final)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar pedido de certificado: {e}")
        return False

def formatar_peso_brasileiro(valor):
    try:
        if pd.isna(valor) or valor == "": return "0"
        texto = f"{float(valor):.3f}"
        texto = texto.replace('.', ',').rstrip('0').rstrip(',')
        return texto
    except:
        return str(valor)

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
        col1, col2 = st.columns(2)
        col1.metric("Total Faturado (7 dias)", f"{total_periodo:,.2f} Ton")
        
        # --- GRÁFICO PERSONALIZADO (ALTAIR) ---
        ordem_grafico = df_exibicao['Label_X'].tolist()
        
        # AJUSTE V23: Adicionado labelExpr="split(datum.value, '\n')"
        # Isso força o gráfico a entender o "Enter" e colocar o número na linha de baixo
        grafico = alt.Chart(df_exibicao).mark_bar(size=40, color='#0078D4').encode(
            x=alt.X('Label_X', sort=ordem_grafico, axis=alt.Axis(title=None, labelAngle=0, labelExpr="split(datum.value, '\\n')")), 
            y=alt.Y('TONS', title='Toneladas'),
            tooltip=['Data_Str', 'TONS']
        ).properties(
            height=400
        )
        
        st.altair_chart(grafico, use_container_width=True)
            
    elif 'dados_faturamento' in st.session_state and st.session_state['dados_faturamento'].empty:
        st.warning("Nenhum faturamento recente encontrado na planilha de sincronização.")
    else:
        st.info("Clique no botão acima para carregar os indicadores.")

def exibir_carteira_pedidos():
    titulo_prefixo = "Carteira de Pedidos"
    tipo_usuario = st.session_state['usuario_tipo'].lower()
    if "gerente" in tipo_usuario: 
        titulo_prefixo = "Gerência de Carteira"
    st.title(f"{titulo_prefixo}: {st.session_state['usuario_nome']}")
    df_total = carregar_dados_pedidos()
    if df_total is not None and not df_total.empty:
        df_total = df_total.dropna(subset=["Número do Pedido"])
        df_total = df_total[~df_total["Número do Pedido"].isin(["000nan", "00None", "000000"])]
        nome_filtro = st.session_state['usuario_filtro']
        if tipo_usuario in ["admin", "gerente"]:
            vendedores_unicos = sorted(df_total["Vendedor Correto"].dropna().unique())
            filtro_vendedor = st.selectbox(f"Filtrar Vendedor ({tipo_usuario.capitalize()})", ["Todos"] + vendedores_unicos)
            if filtro_vendedor != "Todos":
                df_filtrado = df_total[df_total["Vendedor Correto"] == filtro_vendedor].copy()
            else:
                df_filtrado = df_total.copy()
        elif tipo_usuario == "gerente comercial":
            if "Gerente Correto" in df_total.columns:
                df_filtrado = df_total[df_total["Gerente Correto"].str.lower() == nome_filtro.lower()].copy()
            else:
                df_filtrado = pd.DataFrame()
        else:
            df_filtrado = df_total[df_total["Vendedor Correto"].str.lower().str.contains(nome_filtro.lower(), regex=False, na=False)].copy()

        if df_filtrado.empty:
            st.info(f"Nenhum pedido pendente encontrado.")
        else:
            df_filtrado['Quantidade_Num'] = pd.to_numeric(df_filtrado['Quantidade'], errors='coerce').fillna(0)
            df_filtrado['Peso (ton)'] = df_filtrado['Quantidade_Num'].apply(formatar_peso_brasileiro)
            try:
                df_filtrado['Prazo_dt'] = pd.to_datetime(df_filtrado['Prazo'], dayfirst=True, errors='coerce')
                df_filtrado['Prazo'] = df_filtrado['Prazo_dt'].dt.strftime('%d/%m/%Y').fillna("-")
            except: pass
            
            colunas_visiveis = ["Número do Pedido", "Cliente Correto", "Produto", "Peso (ton)", "Prazo", "Máquina/Processo"]
            if tipo_usuario in ["admin", "gerente", "gerente comercial"]:
                colunas_visiveis.insert(5, "Vendedor Correto")
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
            else:
                df_exibicao = df_final
            st.dataframe(df_exibicao, hide_index=True, use_container_width=True, column_config={"Prazo": st.column_config.TextColumn("Previsão")})
            if texto_busca and df_exibicao.empty:
                st.warning(f"Nenhum resultado encontrado para '{texto_busca}'")
    else:
        st.error("Não foi possível carregar a planilha de pedidos.")

def exibir_aba_fotos(is_admin=False):
    st.subheader("📷 Solicitação de Fotos (Material em RDQ)")
    st.markdown("""
        Digite o número do Lote/Bobina abaixo para solicitar fotos de materiais defeituosos.
        Se encontrarmos o registro, enviaremos as fotos para o seu e-mail.
    """)
    with st.form("form_foto"):
        col_f1, col_f2 = st.columns([1, 2])
        with col_f1:
            lote_input = st.text_input("Lote / Bobina:")
        with col_f2:
            email_padrao = st.session_state.get('usuario_email', '')
            email_input = st.text_input("Enviar para o e-mail:", value=email_padrao)
        btn_pedir_foto = st.form_submit_button("Solicitar Fotos", type="primary")
        if btn_pedir_foto:
            if not lote_input:
                st.warning("Por favor, digite o número do lote.")
            elif not email_input:
                st.warning("Por favor, preencha o e-mail.")
            else:
                sucesso = salvar_solicitacao_foto(st.session_state['usuario_nome'], email_input, lote_input)
                if sucesso:
                    st.success(f"Solicitação do lote **{lote_input}** enviada! Verifique seu e-mail em breve.")
    if is_admin:
        st.divider()
        st.markdown("### 🛠️ Gestão de Pedidos de Fotos (Visão Admin)")
        df_fotos = carregar_solicitacoes_fotos()
        if not df_fotos.empty:
            st.dataframe(df_fotos, use_container_width=True, column_config={"Lote": st.column_config.TextColumn("Lote")})
            if st.button("Atualizar Lista de Fotos"):
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("Nenhum pedido de foto registrado ainda.")

def exibir_aba_certificados(is_admin=False):
    st.subheader("📑 Solicitação de Certificados de Qualidade")
    st.markdown("""
        Digite o número do Lote/Bobina para receber o certificado de qualidade.
        A busca será feita em todas as pastas de certificados.
    """)
    with st.form("form_certificado"):
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            lote_cert = st.text_input("Lote / Bobina (Certificado):")
            st.caption("ℹ️ Lotes que só alteram o sequencial final são provenientes da mesma matéria prima. Exemplo: 06818601001, 06818601002, 06818601003 representam a mesma bobina pai.")
        with col_c2:
            email_padrao = st.session_state.get('usuario_email', '')
            email_cert = st.text_input("Enviar para o e-mail:", value=email_padrao, key="email_cert_input")
        btn_pedir_cert = st.form_submit_button("Solicitar Certificado", type="primary")
        if btn_pedir_cert:
            if not lote_cert:
                st.warning("Por favor, digite o número do lote.")
            elif not email_cert:
                st.warning("Por favor, preencha o e-mail.")
            else:
                sucesso = salvar_solicitacao_certificado(st.session_state['usuario_nome'], email_cert, lote_cert)
                if sucesso:
                    st.success(f"Solicitação de certificado do lote **{lote_cert}** enviada! Verifique seu e-mail em breve.")
    if is_admin:
        st.divider()
        st.markdown("### 🛠️ Gestão de Pedidos de Certificados (Visão Admin)")
        df_cert = carregar_solicitacoes_certificados()
        if not df_cert.empty:
            st.dataframe(df_cert, use_container_width=True, column_config={"Lote": st.column_config.TextColumn("Lote")})
            if st.button("Atualizar Lista de Certificados"):
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("Nenhum pedido de certificado registrado ainda.")

# --- GESTÃO DE ESTADO (SESSÃO) ---
if 'logado' not in st.session_state:
    st.session_state['logado'] = False
    st.session_state['usuario_nome'] = ""
    st.session_state['usuario_filtro'] = ""
    st.session_state['usuario_email'] = "" 
    st.session_state['usuario_tipo'] = ""
if 'fazendo_cadastro' not in st.session_state:
    st.session_state['fazendo_cadastro'] = False

# ==============================================================================
# LÓGICA DE LOGIN E CADASTRO
# ==============================================================================
if not st.session_state['logado']:
    if st.session_state['fazendo_cadastro']:
        st.title("📝 Solicitação de Acesso")
        st.markdown("Preencha os dados abaixo. Seu cadastro passará por aprovação.")
        with st.form("form_cadastro"):
            nome_completo = st.text_input("Nome Completo")
            email_user = st.text_input("E-mail")
            novo_login = st.text_input("Crie um Login (Usuário)")
            nova_senha = st.text_input("Crie uma Senha", type="password")
            col_b1, col_b2 = st.columns(2)
            with col_b1:
                btn_enviar = st.form_submit_button("Enviar Solicitação", type="primary", use_container_width=True)
            with col_b2:
                btn_voltar = st.form_submit_button("Voltar para Login", use_container_width=True)
        if btn_voltar:
            st.session_state['fazendo_cadastro'] = False
            st.rerun()
        if btn_enviar:
            if not nome_completo or not email_user or not novo_login or not nova_senha:
                st.warning("Por favor, preencha todos os campos.")
            else:
                df_users = carregar_usuarios()
                login_existe = False
                if not df_users.empty and 'Login' in df_users.columns:
                     if novo_login.lower() in df_users['Login'].str.lower().values:
                         login_existe = True
                if login_existe:
                    st.error("Este login já está em uso por outro usuário. Escolha outro.")
                else:
                    sucesso = salvar_nova_solicitacao(nome_completo, email_user, novo_login, nova_senha)
                    if sucesso:
                        st.success("✅ Solicitação enviada com sucesso! Aguarde um e-mail informando quando seu cadastro estiver concluído.")
    else:
        st.title("🔒 Login - Painel do Vendedor - Dox Brasil")
        st.markdown("Entre com suas credenciais para visualizar a carteira.")
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            usuario_input = st.text_input("Login").strip()
            senha_input = st.text_input("Senha", type="password").strip()
            if st.button("Acessar Sistema", type="primary"):
                df_users = carregar_usuarios()
                if not df_users.empty:
                    usuario_encontrado = df_users[
                        (df_users['Login'].str.lower() == usuario_input.lower()) & 
                        (df_users['Senha'] == senha_input)
                    ]
                    if not usuario_encontrado.empty:
                        dados_user = usuario_encontrado.iloc[0]
                        st.session_state['logado'] = True
                        st.session_state['usuario_nome'] = dados_user['Nome Vendedor'].split()[0]
                        st.session_state['usuario_filtro'] = dados_user['Nome Vendedor']
                        if 'Email' in dados_user.index:
                             st.session_state['usuario_email'] = dados_user['Email']
                        else:
                             st.session_state['usuario_email'] = "" 
                        st.session_state['usuario_tipo'] = dados_user['Tipo']
                        registrar_acesso(usuario_input, dados_user['Nome Vendedor'])
                        st.rerun()
                    else:
                        st.error("Login ou Senha incorretos.")
                else:
                    st.error("Erro ao conectar com base de usuários.")
            st.markdown("---")
            if st.button("Não tem acesso? Solicite aqui"):
                st.session_state['fazendo_cadastro'] = True
                st.rerun()
else:
    with st.sidebar:
        st.write(f"Bem-vindo, **{st.session_state['usuario_nome'].upper()}**")
        agora = datetime.now(FUSO_BR)
        dias_semana = {0: 'Segunda-feira', 1: 'Terça-feira', 2: 'Quarta-feira', 3: 'Quinta-feira', 4: 'Sexta-feira', 5: 'Sábado', 6: 'Domingo'}
        meses = {1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'}
        texto_data = f"{dias_semana[agora.weekday()]}, {agora.day} de {meses[agora.month]} de {agora.year}"
        st.markdown(f"<small><i>{texto_data}</i></small>", unsafe_allow_html=True)
        st.caption(f"Perfil: {st.session_state['usuario_tipo']}")
        if st.button("Sair"):
            st.session_state['logado'] = False
            st.session_state['usuario_nome'] = ""
            st.rerun()
        st.divider()
        if st.button("🔄 Atualizar Dados"):
            st.cache_data.clear()
            st.rerun()

    if st.session_state['usuario_tipo'].lower() == "admin":
        aba1, aba2, aba3, aba4, aba5 = st.tabs([
            "📂 Carteira de Pedidos", 
            "📝 Solicitações de Acesso", 
            "📑 Certificados",
            "🔍 Histórico de Acessos",
            "📊 Faturamento"
        ])
        with aba1:
            exibir_carteira_pedidos()
        with aba2:
            st.subheader("Gerenciamento de Solicitações de Cadastro")
            st.info("Aqui estão os usuários que pediram acesso pelo site. Copie os dados para a aba 'Usuarios' do Excel para aprovar.")
            df_solicitacoes = carregar_solicitacoes()
            if not df_solicitacoes.empty:
                st.dataframe(df_solicitacoes, use_container_width=True)
                if st.button("Atualizar Lista de Acessos"):
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("Nenhuma solicitação pendente.")
        with aba3:
            exibir_aba_certificados(is_admin=True)
        with aba4:
            st.subheader("🔍 Histórico de Logins no Sistema")
            df_logs = carregar_logs_acessos()
            if not df_logs.empty:
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
                if st.button("Atualizar Logs"):
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info("Nenhum registro de acesso encontrado.")
        with aba5:
            exibir_aba_faturamento()
    else:
        aba1, aba2 = st.tabs([
            "📂 Carteira de Pedidos", 
            "📑 Certificados"
        ])
        with aba1:
            exibir_carteira_pedidos()
        with aba2:
            exibir_aba_certificados(is_admin=False)