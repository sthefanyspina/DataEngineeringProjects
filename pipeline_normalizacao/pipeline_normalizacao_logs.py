import pandas as pd
import numpy as np
import sqlite3
import re

# ===========================================================
# 1. Funções utilitárias
# ===========================================================

def corrigir_nomes_colunas(df):
    """
    Corrige nomes de colunas incorretos e espaços extras.
    Mapeia para nomes padronizados do schema final.
    """
    # Renomeia ignorando diferenças de maiúsculas e espaços
    df.columns = [re.sub(r'\s+', '', c.strip().lower()) for c in df.columns]

    # Mapeamento para nomes corretos
    mapeamento = {
        "nome": "Nome",
        "nome_": "Nome",
        "datanasc": "Data_nasc",
        "datanascimento": "Data_nasc",
        "salarioreais": "Salário",
        "salario": "Salário",
        "cidade": "Cidade"
    }

    # Renomeia conforme o mapeamento
    df.rename(columns={k: v for k, v in mapeamento.items() if k in df.columns}, inplace=True)
    return df

# ===========================================================
# 2. Funções de normalização
# ===========================================================

def normalizar_texto(col, erros, nome_coluna):
    col = col.astype(str)
    valores_invalidos = col[col.str.isnumeric()]
    for i, valor in valores_invalidos.items():
        erros.append({"linha": i, "coluna": nome_coluna, "valor": valor, "erro": "Texto numérico inválido"})
    return col.str.strip().str.title()

def normalizar_data(col, erros, nome_coluna):
    datas_convertidas = pd.to_datetime(col, errors='coerce', dayfirst=True)
    linhas_invalidas = col[datas_convertidas.isna()]
    for i, valor in linhas_invalidas.items():
        erros.append({"linha": i, "coluna": nome_coluna, "valor": valor, "erro": "Data inválida"})
    return datas_convertidas

def normalizar_numero(col, erros, nome_coluna):
    col = col.astype(str)
    col = col.str.replace('.', '', regex=False)
    col = col.str.replace(',', '.', regex=False)
    numeros = pd.to_numeric(col, errors='coerce')
    linhas_invalidas = col[numeros.isna()]
    for i, valor in linhas_invalidas.items():
        erros.append({"linha": i, "coluna": nome_coluna, "valor": valor, "erro": "Número inválido"})
    return numeros

def remover_duplicatas_e_vazios(df):
    df = df.drop_duplicates()
    df = df.dropna(how="all")
    return df

# ===========================================================
# 3. Função principal do pipeline
# ===========================================================

def pipeline_normalizacao(caminho_entrada, caminho_saida, banco_dados, tabela):
    erros = []

    print("🚀 Iniciando pipeline de normalização...")
    df = pd.read_csv(caminho_entrada, dtype=str, low_memory=False)

    # Corrigir nomes de colunas
    df = corrigir_nomes_colunas(df)

    # Normalizar colunas existentes
    if 'Nome' in df.columns:
        df['Nome'] = normalizar_texto(df['Nome'], erros, 'Nome')

    if 'Cidade' in df.columns:
        df['Cidade'] = normalizar_texto(df['Cidade'], erros, 'Cidade')

    if 'Data_nasc' in df.columns:
        df['Data_nasc'] = normalizar_data(df['Data_nasc'], erros, 'Data_nasc')

    if 'Salário' in df.columns:
        df['Salário'] = normalizar_numero(df['Salário'], erros, 'Salário')

    # Limpeza geral
    df = remover_duplicatas_e_vazios(df)

    # Salvar CSV limpo
    df.to_csv(caminho_saida, index=False)
    print(f"✅ Dataset limpo salvo em: {caminho_saida}")

    # Salvar em banco SQLite (pode trocar engine)
    conn = sqlite3.connect(banco_dados)
    df.to_sql(tabela, conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Dados salvos na tabela '{tabela}' do banco '{banco_dados}'")

    # Log de erros
    if erros:
        df_erros = pd.DataFrame(erros)
        df_erros.to_csv("logs/log_erros.csv", index=False)
        print(f"⚠️ Log de erros salvo: log_erros.csv ({len(df_erros)} registros problemáticos)")
    else:
        print("✅ Nenhum erro detectado.")

    print("🏁 Pipeline concluído com sucesso!")

# ===========================================================
# 4. Executar o pipeline
# ===========================================================

pipeline_normalizacao(
    caminho_entrada="data/dados_bagunçados.csv",
    caminho_saida="data/dados_limpos.csv",
    banco_dados="data/dados_normalizados.db",
    tabela="clientes"
)
