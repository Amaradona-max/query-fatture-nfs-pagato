from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err: Exception | None = None
    for enc in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc, on_bad_lines="skip", dtype=str)
        except Exception as exc:
            last_err = exc
    raise last_err or ValueError("Impossibile leggere CSV")


def main() -> None:
    nfs_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")

    allowed_protocols = {
        "EP",
        "2EP",
        "EZ",
        "2EZ",
        "EZP",
        "EL",
        "2EL",
        "L",
        "P",
        "2P",
        "FPIC",
        "FSIC",
        "FPEC",
        "FSEC",
        "FCBI",
        "FCSI",
        "FCBE",
        "FCSE",
        "AFIC",
        "ASIC",
        "AFEC",
        "ASEC",
        "ACBI",
        "ACSI",
        "ACBE",
        "ACSE",
    }

    df = read_csv_robust(nfs_path)

    col_doc = "N_DOCUMENTO"
    col_date = "DATA_FATTURA"
    col_name = "RAGIONE SOCIALE"
    col_prot = "FT_PROT"
    col_sdi = "IDENT_SDI"

    missing = [c for c in (col_doc, col_date, col_name, col_prot, col_sdi) if c not in df.columns]
    if missing:
        raise SystemExit(f"Colonne mancanti nel CSV: {missing}")

    work = df[[col_doc, col_date, col_name, col_prot, col_sdi]].copy()
    for c in (col_doc, col_date, col_name):
        work[c] = work[c].fillna("").astype(str).str.strip()
    work[col_prot] = work[col_prot].fillna("").astype(str).str.strip().str.upper()
    work[col_sdi] = work[col_sdi].fillna("").astype(str).str.strip()
    work[col_sdi] = work[col_sdi].replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})

    base_rows = len(work)
    work1 = work.drop_duplicates(subset=[col_doc, col_date, col_name], keep="first")
    after_dedup = len(work1)
    work2 = work1[work1[col_prot].isin(allowed_protocols)].copy()
    after_protocol = len(work2)

    cart_mask = work2[col_sdi].eq("")
    elet_mask = ~cart_mask

    print("BASE_ROWS", base_rows)
    print("AFTER_DEDUP", after_dedup)
    print("AFTER_PROTOCOL_FILTER", after_protocol)
    print("CARTACEE_SDI_VUOTO", int(cart_mask.sum()))
    print("ELETTRONICHE_SDI_PIENO", int(elet_mask.sum()))
    print()

    vc = work2[col_prot].value_counts().sort_index()
    print("PROTOCOL_DISTRIBUTION_ALLOWED")
    for prot, cnt in vc.items():
        print(f"{prot}\t{int(cnt)}")

    print()
    vc_cart = work2.loc[cart_mask, col_prot].value_counts().sort_index()
    print("PROTOCOL_DISTRIBUTION_ALLOWED_CARTACEE")
    for prot, cnt in vc_cart.items():
        print(f"{prot}\t{int(cnt)}")

    print()
    vc_elet = work2.loc[elet_mask, col_prot].value_counts().sort_index()
    print("PROTOCOL_DISTRIBUTION_ALLOWED_ELETTRONICHE")
    for prot, cnt in vc_elet.items():
        print(f"{prot}\t{int(cnt)}")


if __name__ == "__main__":
    main()

