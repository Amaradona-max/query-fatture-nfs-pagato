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


def normalize_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def normalize_upper(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


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

    cartacee_prot = {"P", "2P", "L", "FCBI", "FCSI", "FCBE", "FCSE"}
    elet_prot = {
        "EP",
        "2EP",
        "EZ",
        "2EZ",
        "EZP",
        "EL",
        "2EL",
        "FPIC",
        "FSIC",
        "FPEC",
        "FSEC",
        "AFIC",
        "ASIC",
        "AFEC",
        "ASEC",
        "ACBI",
        "ACSI",
        "ACBE",
        "ACSE",
    }
    prot_for_importo = {"EL", "2EL"}

    col_dat = "DATA_FATTURA"
    col_nome = "RAGIONE SOCIALE"
    col_sdi = "IDENT_SDI"
    col_prot = "FT_PROT"
    col_segno = "FT_SEGNO"
    col_trib = "COD_TRIBUTO"

    num_cols = ["IMP_TOT_IVA", "IMP_TOT_FATTURA", "IMPONIBILE", "IMP_TOT_MAND", "IMP_TOT_RIT"]

    nfs = read_csv_robust(nfs_path)
    needed = [col_dat, col_nome, col_sdi, col_prot, col_segno, col_trib, *num_cols]
    missing_nfs = [c for c in needed if c not in nfs.columns]
    if missing_nfs:
        raise SystemExit(f"NFS: colonne mancanti {missing_nfs}")

    w = nfs[needed].copy()
    w[col_dat] = normalize_text(w[col_dat])
    w[col_nome] = normalize_text(w[col_nome])
    w[col_sdi] = normalize_text(w[col_sdi]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    w[col_prot] = normalize_upper(w[col_prot])
    w[col_segno] = normalize_upper(w[col_segno])
    w[col_trib] = normalize_upper(w[col_trib])
    for c in num_cols:
        w[c] = to_number_it(w[c])

    segno_a = w[col_segno].eq("A")
    for c in num_cols:
        w.loc[segno_a, c] = -w.loc[segno_a, c]

    base_rows = int(len(w))
    w1 = w.drop_duplicates(subset=[col_dat, col_nome, col_sdi], keep="first")
    after_dedup = int(len(w1))
    w2 = w1[w1[col_prot].isin(allowed_prot)].copy()
    after_filter = int(len(w2))

    segno_a_post = int(w2[col_segno].eq("A").sum())

    mask_cart = w2[col_prot].isin(cartacee_prot)
    mask_elet = w2[col_prot].isin(elet_prot)

    cart_cnt = int(mask_cart.sum())
    elet_cnt = int(mask_elet.sum())

    pisa = pd.read_excel(pisa_path, sheet_name="Sheet1", dtype=str)
    if "Identificativo SDI" not in pisa.columns:
        raise SystemExit("Pisa: colonna Identificativo SDI mancante")
    psdi = normalize_text(pisa["Identificativo SDI"]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    pisa_cart_cnt = int(psdi.eq("").sum())
    pisa_elet_cnt = int((~psdi.eq("")).sum())

    mprot = w2[col_prot].isin(prot_for_importo)
    mi9 = w2[col_trib].eq("I9")
    impon_tot = float(w2.loc[mprot, "IMPONIBILE"].sum())
    impon_i9 = float(w2.loc[mprot & mi9, "IMPONIBILE"].sum())
    impon_net = impon_tot - impon_i9
    i9_cnt = int((mprot & mi9).sum())

    print("=== RIEPILOGO ISTRUZIONI (ULTIMA VERSIONE) ===")
    print("NFS_BASE_ROWS", base_rows)
    print("NFS_AFTER_DEDUP(DATA_FATTURA+RAGIONE_SOCIALE+IDENT_SDI)", after_dedup)
    print("NFS_AFTER_PROTOCOL_FILTER", after_filter)
    print("NFS_FT_SEGNO_A_COUNT_POST", segno_a_post)
    print()
    print("NFS_COUNTS_BY_CATEGORY_(DA_PROTOCOLLO)")
    print("CARTACEE", cart_cnt)
    print("ELETTRONICHE", elet_cnt)
    print("TOTALE", cart_cnt + elet_cnt)
    print()
    print("PISA_COUNTS_BY_SDI")
    print("CARTACEE_SDI_VUOTO", pisa_cart_cnt)
    print("ELETTRONICHE_SDI_PIENO", pisa_elet_cnt)
    print("TOTALE", int(len(pisa)))
    print()
    print("DELTA_COUNTS_(PISA_MINUS_NFS)")
    print("CARTACEE", pisa_cart_cnt - cart_cnt)
    print("ELETTRONICHE", pisa_elet_cnt - elet_cnt)
    print()
    print("NFS_IMPORTO_PAGATO_(SOLO_EL_2EL)__IMPONIBILE_CON_SEGNO_-_I9")
    print("IMPONIBILE_TOT_EL2EL", round(impon_tot, 2))
    print("I9_TOT_EL2EL", round(impon_i9, 2))
    print("NETTO_EL2EL", round(impon_net, 2))
    print("I9_COUNT_EL2EL", i9_cnt)
    print()
    print("NFS_PROTOCOL_DISTRIBUTION_ALLOWED")
    for k, v in w2[col_prot].value_counts().sort_index().items():
        print(f"{k}\t{int(v)}")


if __name__ == "__main__":
    main()

