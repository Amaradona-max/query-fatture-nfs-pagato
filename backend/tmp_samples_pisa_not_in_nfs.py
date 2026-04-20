from __future__ import annotations

from pathlib import Path

import pandas as pd


def norm_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip().upper()


def norm_date(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if not s:
        return ""
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


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
    pisa_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx")

    cartacee_prot = {"P", "2P", "L", "FCBI", "FCSI", "FCBE", "FCSE"}
    elet_prot = {"EP", "2EP", "EZ", "2EZ", "EZP", "EL", "2EL", "FPIC", "FSIC", "FPEC", "FSEC"}
    autofatt_prot = {"AFIC", "ASIC", "AFEC", "ASEC", "ACBI", "ACSI", "ACBE", "ACSE"}
    allowed_prot = cartacee_prot | elet_prot | autofatt_prot

    # NFS (istruzioni aggiornate): dedup su DATA_FATTURA + RAGIONE SOCIALE + IDENT_SDI, poi filtro protocolli
    nfs = read_csv_robust(nfs_path)
    need_nfs = ["DATA_FATTURA", "RAGIONE SOCIALE", "IDENT_SDI", "FT_PROT", "N_DOCUMENTO"]
    missing_nfs = [c for c in need_nfs if c not in nfs.columns]
    if missing_nfs:
        raise SystemExit(f"NFS: colonne mancanti {missing_nfs}")

    nfsw = nfs[need_nfs].copy()
    nfsw["DATA_FATTURA"] = nfsw["DATA_FATTURA"].map(norm_date)
    nfsw["RAGIONE SOCIALE"] = nfsw["RAGIONE SOCIALE"].map(norm_text)
    nfsw["IDENT_SDI"] = (
        nfsw["IDENT_SDI"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    )
    nfsw["FT_PROT"] = nfsw["FT_PROT"].map(norm_text)
    nfsw["N_DOCUMENTO"] = nfsw["N_DOCUMENTO"].map(norm_text)

    nfsw = nfsw.drop_duplicates(subset=["DATA_FATTURA", "RAGIONE SOCIALE", "IDENT_SDI"], keep="first")
    nfsw = nfsw[nfsw["FT_PROT"].isin(allowed_prot)].copy()

    nfs_elet_sdi = set(nfsw.loc[nfsw["FT_PROT"].isin(elet_prot | autofatt_prot), "IDENT_SDI"].astype(str).str.strip())
    nfs_elet_sdi = {s for s in nfs_elet_sdi if s}

    nfs_cart = nfsw[nfsw["FT_PROT"].isin(cartacee_prot)].copy()
    nfs_cart_key = (nfs_cart["RAGIONE SOCIALE"] + "|" + nfs_cart["N_DOCUMENTO"] + "|" + nfs_cart["DATA_FATTURA"]).tolist()
    nfs_cart_keys = set(nfs_cart_key)

    # Pisa: cartacee/elettroniche da Identificativo SDI vuoto/pieno
    pisa = pd.read_excel(pisa_path, sheet_name="Sheet1", dtype=str)
    need_pisa = ["Creditore", "Numero fattura", "Data emissione", "Identificativo SDI"]
    missing_pisa = [c for c in need_pisa if c not in pisa.columns]
    if missing_pisa:
        raise SystemExit(f"Pisa: colonne mancanti {missing_pisa}")

    pisaw = pisa[need_pisa].copy()
    pisaw["Creditore"] = pisaw["Creditore"].map(norm_text)
    pisaw["Numero fattura"] = pisaw["Numero fattura"].map(norm_text)
    pisaw["Data emissione"] = pisaw["Data emissione"].map(norm_date)
    pisaw["Identificativo SDI"] = (
        pisaw["Identificativo SDI"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    )

    pisa_cart_mask = pisaw["Identificativo SDI"].eq("")
    pisa_elet_mask = ~pisa_cart_mask

    # elettroniche: match by SDI
    pisa_elet_missing = pisaw[pisa_elet_mask & ~pisaw["Identificativo SDI"].isin(nfs_elet_sdi)].copy()

    # cartacee: match by (Creditore, Numero fattura, Data emissione) ~ (Ragione sociale, N_DOCUMENTO, DATA_FATTURA)
    pisa_cart_key = pisaw["Creditore"] + "|" + pisaw["Numero fattura"] + "|" + pisaw["Data emissione"]
    pisa_cart_missing = pisaw[pisa_cart_mask & ~pisa_cart_key.isin(nfs_cart_keys)].copy()

    pisa_cart_missing = pisa_cart_missing.sort_values(by=["Creditore", "Numero fattura", "Data emissione"])
    pisa_elet_missing = pisa_elet_missing.sort_values(by=["Identificativo SDI", "Creditore", "Numero fattura"])

    print("CARTACEE_PISA_NON_IN_NFS_COUNT", int(len(pisa_cart_missing)))
    print("ELETTRONICHE_PISA_NON_IN_NFS_COUNT", int(len(pisa_elet_missing)))
    print()

    print("CAMPIONE_CARTACEE_PISA_NON_IN_NFS (2)")
    for i, row in enumerate(pisa_cart_missing.head(2).to_dict(orient="records"), 1):
        print(
            f"{i}) Creditore={row['Creditore']} | Numero fattura={row['Numero fattura']} | Data emissione={row['Data emissione']} | SDI=(vuoto)"
        )
    print()

    print("CAMPIONE_ELETTRONICHE_PISA_NON_IN_NFS (2)")
    for i, row in enumerate(pisa_elet_missing.head(2).to_dict(orient="records"), 1):
        print(
            f"{i}) Creditore={row['Creditore']} | Numero fattura={row['Numero fattura']} | Data emissione={row['Data emissione']} | SDI={row['Identificativo SDI']}"
        )


if __name__ == "__main__":
    main()
