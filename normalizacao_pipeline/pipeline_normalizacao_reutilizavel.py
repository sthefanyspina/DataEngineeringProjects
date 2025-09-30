import pandas as pd
import numpy as np
import sqlite3

# -------------------------------
# 1. Funções de Normalização
# -------------------------------

def remover_vazios(df):
    """Remove linhas completamente vazias."""
    df.dropna(inplace=True)
    return df

def remover_duplicatas(df):
    """Remove linhas duplicadas e completamente vazias."""
    df = df.drop_duplicates()
    return df

def normalizar_texto(col, erros, nome_coluna):
    """Remove espaços extras e padroniza capitalização."""
    try:
        return col.str.strip().str.title()
    except Exception as e:
        erros.append({"coluna": nome_coluna, "erro": str(e)})
        return col

def normalizar_data(col, erros, nome_coluna):
    """Converte datas para o formato YYYY-MM-DD, salvando erros."""
    datas_convertidas = pd.to_datetime(col, errors='coerce', dayfirst=True)
    linhas_invalidas = col[datas_convertidas.isna()]
    if len(linhas_invalidas) > 0:
        for i, valor in linhas_invalidas.items():
            erros.append({"linha": i, "coluna": nome_coluna, "valor": valor, "erro": "Data inválida"})
    return datas_convertidas

def normalizar_numero(col, erros, nome_coluna):
    """Remove separadores de milhar, converte vírgulas para pontos e transforma em float."""
    col = col.astype(str)
    col = col.str.replace('.', '', regex=False)  # remove pontos (milhar)
    col = col.str.replace(',', '.', regex=False)  # troca vírgula por ponto
    numeros = pd.to_numeric(col, errors='coerce')
    linhas_invalidas = col[numeros.isna()]
    if len(linhas_invalidas) > 0:
        for i, valor in linhas_invalidas.items():
            erros.append({"linha": i, "coluna": nome_coluna, "valor": valor, "erro": "Número inválido"})
    return numeros


# -------------------------------
# 2. Função Principal do Pipeline
# -------------------------------

def pipeline_normalizacao(caminho_entrada, caminho_saida, banco_dados, tabela):
    # Lista para armazenar erros
    erros = []

    # 1. Ler dataset
    df = pd.read_csv(caminho_entrada)

    # 2. Remover linhas vazias
    df = remover_vazios(df)

    # 3. Remover duplicatas
    df = remover_duplicatas(df)

    # 4. Normalizar colunas (ajuste conforme necessidade)
    if 'studyName' in df.columns:
        df['studyName'] = normalizar_texto(df['studyName'], erros, 'studyName')
    if 'Sample Number' in df.columns:
        df['Sample Number'] = normalizar_texto(df['Sample Number'], erros, 'Sample Number')
    if 'Species' in df.columns:
        df['Species'] = normalizar_data(df['Species'], erros, 'Species')
    if 'Island' in df.columns:
        df['Island'] = normalizar_texto(df['Island'], erros, 'Island')
    if 'Clutch Completion' in df.columns:
        df['Clutch Completion'] = normalizar_numero(df['Clutch Completion'], erros, 'Clutch Completion')
    if 'Date Egg' in df.columns:
        df['Date Egg'] = normalizar_texto(df['Date Egg'], erros, 'Date Egg')
    if 'Culmen Length (mm)' in df.columns:
        df['Culmen Length (mm)'] = normalizar_texto(df['Culmen Length (mm)'], erros, 'Culmen Length (mm)')
    if 'Culmen Depth (mm)' in df.columns:
        df['Culmen Depth (mm)'] = normalizar_data(df['Culmen Depth (mm)'], erros, 'Culmen Depth (mm)')
    if 'Flipper Length (mm)' in df.columns:
        df['Flipper Length (mm)'] = normalizar_numero(df['Flipper Length (mm)'], erros, 'Flipper Length (mm)')
    if 'Body Mass (g)' in df.columns:
        df['Body Mass (g)'] = normalizar_texto(df['Body Mass (g)'], erros, 'Body Mass (g)')
    if 'Sex' in df.columns:
        df['Sex'] = normalizar_texto(df['Sex'], erros, 'Sex')

    # 5. Salvar dataset limpo em CSV
    df.to_csv(caminho_saida, index=False)
    print(f"✅ Dataset limpo salvo em: {caminho_saida}")

    # 6. Salvar no banco de dados
    conn = sqlite3.connect(banco_dados)
    df.to_sql(tabela, conn, if_exists="replace", index=False)
    conn.close()
    print(f"✅ Dados carregados na tabela '{tabela}' do banco '{banco_dados}'")

    # 7. Salvar log de erros (se houver)
    if erros:
        df_erros = pd.DataFrame(erros)
        df_erros.to_csv("log_erros.csv", index=False)
        print(f"⚠️ Log de erros salvo em: log_erros.csv ({len(df_erros)} registros problemáticos)")
    else:
        print("✅ Nenhum erro encontrado durante a normalização.")

# -------------------------------
# 3. Executando o Pipeline
# -------------------------------

pipeline_normalizacao(
    caminho_entrada="penguins_lter.csv",
    caminho_saida="penguins_lter_limpo.csv",
    banco_dados="penguins_lter.db",
    tabela="penguins"
)
