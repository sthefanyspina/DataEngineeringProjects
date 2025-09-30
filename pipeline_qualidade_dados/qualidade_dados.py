import pandas as pd
import json

def validar_qualidade_dados(arquivo_entrada, relatorio_saida="relatorio_qualidade.json"):
    # 1. Ler o dataset
    try:
        df = pd.read_csv(arquivo_entrada)
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return

    erros = {}

    # 2. Verificar valores nulos
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
        erros["valores_nulos"] = nulls.to_dict()

    # 3. Verificar tipos inconsistentes (exemplo: tentar converter numéricos)
    inconsistentes = {}
    for coluna in df.columns:
        if df[coluna].dtype == object:
            # se a coluna deveria ser numérica, converte para numeric
            if "preco" in coluna.lower() or "quantidade" in coluna.lower() or "valor" in coluna.lower():
                convertida = pd.to_numeric(df[coluna], errors="coerce")
                linhas_invalidas = df[convertida.isna() & df[coluna].notna()]
                if not linhas_invalidas.empty:
                    inconsistentes[coluna] = linhas_invalidas.to_dict(orient="records")
    if inconsistentes:
        erros["tipos_inconsistentes"] = inconsistentes

    # 4. Verificar duplicatas (considerando todas as colunas)
    duplicatas = df[df.duplicated()]
    if not duplicatas.empty:
        erros["duplicatas"] = duplicatas.to_dict(orient="records")

    # 5. Gerar relatório
    with open(relatorio_saida, "w", encoding="utf-8") as f:
        json.dump(erros, f, indent=4, ensure_ascii=False)

    print(f"Validação concluída. Relatório salvo em: {relatorio_saida}")
    if not erros:
        print("Nenhum problema encontrado ✅")
    else:
        print(f"Foram encontrados {len(erros)} tipos de problemas.")

# ---------- EXECUÇÃO ----------
if __name__ == "__main__":
    validar_qualidade_dados("penguins_lter.csv")
