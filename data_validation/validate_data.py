# ==========================================
# validate_data.py
# ==========================================
import os
import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date

# -------------------------
# Classe de Relatório
# -------------------------
class DQReport:
    def __init__(self):
        self.results = {}
        self.fail_samples_dir = 'dq_reports/fail_samples'
        os.makedirs(self.fail_samples_dir, exist_ok=True)

    def add(self, name, passed, details=None, sample_df=None):
        self.results[name] = {'passed': bool(passed), 'details': details}
        if sample_df is not None and not passed:
            fname = os.path.join(self.fail_samples_dir, f"{name}.csv")
            sample_df.to_csv(fname, index=False)
            self.results[name]['sample_path'] = fname

    def save(self):
        os.makedirs('dq_reports', exist_ok=True)
        with open('dq_reports/report.json', 'w', encoding='utf8') as f:
            json.dump(self.results, f, default=str, indent=2)

# -------------------------
# Funções de Checagem
# -------------------------
def check_not_null(df, col): return df[col].notnull().all()
def check_unique(df, col): return df[col].is_unique
def check_valid_email(df, col): return df[col].apply(lambda x: re.match(r"[^@]+@[^@]+\.[^@]+", str(x)) is not None).all()
def check_positive(df, col): return (df[col] >= 0).all()
def check_date_consistency(df): return (df['data_inicio'] <= df['data_fim']).all()
def check_percentual(df): return df['percentual'].between(0,100).all()
def check_enum_status(df): return df['status'].isin(['ativo','inativo','pendente']).all()
def check_no_duplicates(df): return not df.duplicated().any()
def check_valor_total(df): return np.allclose(df['valor_total'], df['quantidade'] * df['preco'], atol=20)
def check_created_before_updated(df): return (pd.to_datetime(df['created_at']) <= pd.to_datetime(df['updated_at'])).fillna(True).all()
def check_parcelas(df): return df['parcelas'].between(1,12).all()
def check_country_state(df): return all((p=='US' and e=='CA') or (p=='BR' and e in ['SP','RJ']) for p,e in zip(df['pais'], df['estado']))
def check_future_dates(df): return not (pd.to_datetime(df['data_fim']) > datetime.now() + timedelta(days=365)).any()
def check_length_nome(df): return df['nome'].apply(lambda x: len(str(x))>0 and len(str(x))<=100).all()
def check_id_positive(df): return (df['id']>0).all()
def check_cpf_length(df): return df['cpf'].apply(lambda x: len(str(x))==11).all()
def check_no_negative_quantidade(df): return (df['quantidade']>=0).all()
def check_preco(df): return (df['preco']>=0).all()
def check_no_future_created(df): return not (pd.to_datetime(df['created_at'])>datetime.now()).any()
def check_id_cliente_valid(df): return (df['id_cliente']>0).all()

# -------------------------
# Execução de Todas as Regras
# -------------------------
def run_all_checks(df):
    report = DQReport()
    checks = [
        ("not_null_nome", check_not_null(df,'nome')),
        ("unique_id", check_unique(df,'id')),
        ("valid_email", check_valid_email(df,'email')),
        ("positive_preco", check_positive(df,'preco')),
        ("date_consistency", check_date_consistency(df)),
        ("percentual_range", check_percentual(df)),
        ("enum_status", check_enum_status(df)),
        ("no_duplicates", check_no_duplicates(df)),
        ("valor_total_check", check_valor_total(df)),
        ("created_before_updated", check_created_before_updated(df)),
        ("parcelas_range", check_parcelas(df)),
        ("country_state_match", check_country_state(df)),
        ("future_dates_check", check_future_dates(df)),
        ("length_nome", check_length_nome(df)),
        ("id_positive", check_id_positive(df)),
        ("cpf_length", check_cpf_length(df)),
        ("no_negative_quantidade", check_no_negative_quantidade(df)),
        ("preco_positive", check_preco(df)),
        ("no_future_created", check_no_future_created(df)),
        ("id_cliente_valid", check_id_cliente_valid(df))
    ]

    for name, passed in checks:
        report.add(name, passed)

    report.save()
    return report

# -------------------------
# Execução Principal
# -------------------------
if __name__ == "__main__":
    df = pd.read_csv("data/synthetic_data.csv")
    print("🔍 Executando 20 validações de qualidade...")
    report = run_all_checks(df)
    print("✅ Relatório salvo em reports/report.json")
