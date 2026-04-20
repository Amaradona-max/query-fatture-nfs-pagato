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


def to_number_it(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.replace(" ", "", regex=False).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0)


def main() -> None:
    nfs_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    pisa_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx")

    allowed_prot = {
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
    cart_prot = {"P", "2P", "L", "FCBI", "FCSI", "FCBE", "FCSE"}
    elet_prot = {"EP", "2EP", "EL", "2EL", "EZ", "2EZ", "EZP", "FPIC", "FSIC", "FPEC", "FSEC"}
    auto_prot = {"AFIC", "ASIC", "AFEC", "ASEC", "ACBI", "ACSI", "ACBE", "ACSE"}

    rit_adj_prot = {"EL", "2EL", "L"}

    nfs = read_csv_robust(nfs_path)
    cols_needed = [
        "DATA_FATTURA",
        "RAGIONE SOCIALE",
        "IDENT_SDI",
        "FT_PROT",
        "FT_SEGNO",
        "IMP_TOT_IVA",
        "IMP_TOT_FATTURA",
        "IMPONIBILE",
        "IMP_TOT_MAND",
        "IMP_TOT_RIT",
    ]
    missing = [c for c in cols_needed if c not in nfs.columns]
    if missing:
        raise SystemExit(f"Colonne mancanti nel CSV NFS: {missing}")

    w = nfs[cols_needed].copy()
    w["DATA_FATTURA"] = w["DATA_FATTURA"].fillna("").astype(str).str.strip()
    w["RAGIONE SOCIALE"] = w["RAGIONE SOCIALE"].fillna("").astype(str).str.strip()
    w["IDENT_SDI"] = (
        w["IDENT_SDI"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    )
    w["FT_PROT"] = w["FT_PROT"].fillna("").astype(str).str.strip().str.upper()
    w["FT_SEGNO"] = w["FT_SEGNO"].fillna("").astype(str).str.strip().str.upper()

    num_cols = ["IMP_TOT_IVA", "IMP_TOT_FATTURA", "IMPONIBILE", "IMP_TOT_MAND", "IMP_TOT_RIT"]
    for c in num_cols:
        w[c] = to_number_it(w[c])

    base_rows = int(len(w))

    # step 003
    w1 = w.drop_duplicates(subset=["DATA_FATTURA", "RAGIONE SOCIALE", "IDENT_SDI"], keep="first")
    after_dedup = int(len(w1))

    # step 004
    w2 = w1[w1["FT_PROT"].isin(allowed_prot)].copy()
    after_filter = int(len(w2))

    # step 005
    neg_mask = w2["FT_SEGNO"].eq("A")
    segno_a_count = int(neg_mask.sum())
    for c in num_cols:
        w2.loc[neg_mask, c] = -w2.loc[neg_mask, c]

    # step 006
    adj_mask = w2["FT_PROT"].isin(rit_adj_prot)
    w2.loc[adj_mask, "IMPONIBILE"] = (w2.loc[adj_mask, "IMPONIBILE"] - w2.loc[adj_mask, "IMP_TOT_RIT"]).round(2)

    cart_cnt = int(w2["FT_PROT"].isin(cart_prot).sum())
    elet_cnt = int(w2["FT_PROT"].isin(elet_prot).sum())
    auto_cnt = int(w2["FT_PROT"].isin(auto_prot).sum())

    cart_amt = float(w2.loc[w2["FT_PROT"].isin(cart_prot), "IMPONIBILE"].sum())
    elet_amt = float(w2.loc[w2["FT_PROT"].isin(elet_prot), "IMPONIBILE"].sum())
    auto_amt = float(w2.loc[w2["FT_PROT"].isin(auto_prot), "IMPONIBILE"].sum())
    tot_amt = float(w2["IMPONIBILE"].sum())

    prot_counts = w2["FT_PROT"].value_counts().sort_index()

    pisa = pd.read_excel(pisa_path, sheet_name="Sheet1", dtype=str)
    psdi = (
        pisa["Identificativo SDI"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    )
    pisa_cart = int(psdi.eq("").sum())
    pisa_elet = int((~psdi.eq("")).sum())

    print("NFS_BASE_ROWS", base_rows)
    print("NFS_AFTER_DEDUP", after_dedup)
    print("NFS_AFTER_PROTOCOL_FILTER", after_filter)
    print("NFS_FT_SEGNO_A_COUNT", segno_a_count)
    print("NFS_RIT_ADJ_ROWS_EL2EL_L", int(adj_mask.sum()))
    print()
    print("NFS_COUNTS_BY_CATEGORY")
    print("CARTACEE", cart_cnt)
    print("ELETTRONICHE", elet_cnt)
    print("AUTOFATTURE", auto_cnt)
    print("TOTALE", cart_cnt + elet_cnt + auto_cnt)
    print()
    print("NFS_IMPORTO_PAGATO_BY_CATEGORY (IMPONIBILE post regole)")
    print("CARTACEE", round(cart_amt, 2))
    print("ELETTRONICHE", round(elet_amt, 2))
    print("AUTOFATTURE", round(auto_amt, 2))
    print("TOTALE", round(tot_amt, 2))
    print()
    print("PISA_COUNTS_BY_SDI")
    print("CARTACEE_SDI_VUOTO", pisa_cart)
    print("ELETTRONICHE_SDI_PIENO", pisa_elet)
    print("TOTALE", int(len(pisa)))
    print()
    print("DELTA_COUNTS (PISA - NFS)")
    print("CARTACEE", pisa_cart - cart_cnt)
    print("ELETTRONICHE", pisa_elet - (elet_cnt + auto_cnt))
    print()
    print("NFS_PROTOCOL_DISTRIBUTION_ALLOWED")
    for k, v in prot_counts.items():
        print(f"{k}\t{int(v)}")


if __name__ == "__main__":
    main()

