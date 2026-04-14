import pandas as pd
from pathlib import Path

nfs_files = sorted(Path('/Users/prova/Downloads').glob('Fatture NFS Pagato*2026*.csv'))
nfs = nfs_files[0] if nfs_files else None
pisa = Path('/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx')

# NFS raw
nd = pd.read_csv(nfs, sep=None, engine='python', encoding='utf-8-sig')
print('NFS cols:', nd.columns.tolist())
print('NFS rows raw:', len(nd))

# Protocolli NFS
prot = nd['FT_PROT'].astype(str).str.strip().str.upper()
print('\nFT_PROT valori unici:')
print(prot.value_counts().to_string())

# Pisa
pd_ = pd.read_excel(pisa, sheet_name=None)
sheet = list(pd_.keys())[0]
pisa_df = pd_[sheet]
print('\nPisa rows:', len(pisa_df))

# Check PISANI cartacee (SDI vuoto) - quante sono?
sdi_pisa = pisa_df['Identificativo SDI'].astype(str).str.strip()
null_pisa = sdi_pisa.isin(['','nan','None','nan ',' ','-'])
print('\nPisa SDI vuote (cartacee):', int(null_pisa.sum()))
print('Pisa SDI piene (elettroniche):', int((~null_pisa).sum()))
