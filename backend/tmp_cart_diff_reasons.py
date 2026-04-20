from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from app.services.file_processor import CompareFTFileProcessor


def nt(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().upper()


def nd(value: object) -> str:
    dt = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def na(value: object) -> str:
    try:
        num = float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0])
    except Exception:
        num = 0.0
    return f"{num:.2f}"


def main() -> None:
    base_dir = Path("/Users/prova/Desktop/query_fatture_nfs_pagato")
    nfs_path = next(base_dir.glob("Fatture NFS Pagato*.xlsx"))
    pisa_path = next(base_dir.glob("Fatture Pisa Pagato*.xlsx"))

    proc = CompareFTFileProcessor()

    raw = proc._load_nfs_compare_df(nfs_path)
    prot_raw = raw["FAT_PROT"].astype(str).str.strip().str.upper()
    raw = raw[prot_raw.isin(proc.NFS_ALLOWED_PROTOCOLS)].copy()
    raw = raw.drop_duplicates(subset=["FAT_DATDOC", "C_NOME", "TMC_G8"]).copy()

    df_nfs = raw[proc.NFS_REQUIRED_COLUMNS].copy().rename(columns=proc.NFS_RENAME_MAP)
    df_nfs["Data Fatture"] = proc._parse_date_series(df_nfs["Data Fatture"])
    df_nfs["Imponibile"] = proc._to_number_series(df_nfs["Imponibile"]).fillna(0)
    if "Tot. Ritenuta" in df_nfs.columns:
        df_nfs["Tot. Ritenuta"] = proc._to_number_series(df_nfs["Tot. Ritenuta"]).fillna(0)
    else:
        df_nfs["Tot. Ritenuta"] = 0.0

    seg = df_nfs["Segno"].fillna("").astype(str).str.strip().str.upper()
    mult = seg.eq("A").map({True: -1.0, False: 1.0})
    prot_series = df_nfs["Prot."].fillna("").astype(str).str.strip().str.upper()
    signed_imp = (df_nfs["Imponibile"] * mult).round(2)
    signed_rit = (df_nfs["Tot. Ritenuta"] * mult).round(2)
    net_imp = signed_imp.where(~prot_series.isin(["EL", "2EL", "L"]), signed_imp - signed_rit).round(2)
    df_nfs["Importo Pagamento"] = net_imp

    pisa = proc._load_pisa_compare_df(pisa_path)
    pisa["Data emissione"] = proc._parse_date_series(pisa["Data emissione"])
    pisa["Importo fattura"] = proc._to_number_series(pisa["Importo fattura"]).fillna(0)
    pisa["_SDI_KEY"] = proc._normalize_sdi(pisa["Identificativo SDI"])
    pisa_cart = pisa[proc._is_empty_sdi(pisa["_SDI_KEY"])].copy()

    nfs_cart = df_nfs[prot_series.isin(proc.NFS_CARTACEE_PROTOCOLS)].copy()

    key_nfs = lambda r: f"{nt(r.get('N.fatture',''))}|{nd(r.get('Data Fatture',None))}|{na(r.get('Importo Pagamento',0.0))}|{nt(r.get('Ragione sociale',''))}"
    key_pisa = lambda r: f"{nt(r.get('Numero fattura',''))}|{nd(r.get('Data emissione',None))}|{na(r.get('Importo fattura',0.0))}|{nt(r.get('Creditore',''))}"

    nfs_keys = [key_nfs(r) for _, r in nfs_cart.iterrows()]
    pisa_keys = [key_pisa(r) for _, r in pisa_cart.iterrows()]

    only_pisa = list((Counter(pisa_keys) - Counter(nfs_keys)).elements())
    show_n = max(0, len(pisa_cart) - len(nfs_cart))
    show_keys = only_pisa[:show_n]

    idx_cart_num_date = defaultdict(list)
    idx_any_num_date = defaultdict(list)
    for _, r in df_nfs.iterrows():
        num = nt(r.get("N.fatture", ""))
        dt = nd(r.get("Data Fatture", None))
        pr = nt(r.get("Prot.", ""))
        nm = nt(r.get("Ragione sociale", ""))
        amt = float(r.get("Importo Pagamento", 0.0))
        idx_any_num_date[(num, dt)].append((pr, nm, amt))
        if pr in proc.NFS_CARTACEE_PROTOCOLS:
            idx_cart_num_date[(num, dt)].append((pr, nm, amt))

    reasons = Counter()
    for k in show_keys:
        num, dt, amt_s, name = k.split("|", 3)
        num = num.strip().upper()
        dt = dt.strip()
        name = name.strip().upper()
        amt = float(amt_s)

        cart_c = idx_cart_num_date.get((num, dt), [])
        any_c = idx_any_num_date.get((num, dt), [])
        if cart_c:
            same_name = [c for c in cart_c if c[1] == name]
            if same_name:
                same_amt = [c for c in same_name if abs(c[2] - amt) < 0.01]
                reasons["stesso_num+data+nome_ma_importo_diverso" if not same_amt else "match_completo"] += 1
            else:
                reasons["stesso_num+data_ma_ragione_sociale_diversa"] += 1
        elif any_c:
            reasons["presente_in_nfs_ma_protocollo_non_cartaceo"] += 1
        else:
            reasons["non_trovata_in_nfs_per_num+data"] += 1

    print("PISA_CART", len(pisa_cart))
    print("NFS_CART", len(nfs_cart))
    print("CART_DIFF (Pisa-NFS)", len(pisa_cart) - len(nfs_cart))
    print("SOLO_PISA_CART_SHOWN", len(show_keys))
    print("REASONS", dict(reasons))


if __name__ == "__main__":
    main()

