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

    # protocolli come da Istruzioni
    cartacee_prot = {"P", "2P", "L", "FCBI", "FCSI", "FCBE", "FCSE"}
    elet_prot = {"EP", "2EP", "EZ", "2EZ", "EZP", "EL", "2EL", "FPIC", "FSIC", "FPEC", "FSEC"}
    autofatt_prot = {"AFIC", "ASIC", "AFEC", "ASEC", "ACBI", "ACSI", "ACBE", "ACSE"}
    allowed_prot = cartacee_prot | elet_prot | autofatt_prot

    # step 3 (istruzioni aggiornate): dedup su DATA_FATTURA, RAGIONE SOCIALE, IDENT_SDI
    col_dat = "DATA_FATTURA"
    col_nome = "RAGIONE SOCIALE"
    col_sdi = "IDENT_SDI"
    col_prot = "FT_PROT"
    col_imp = "IMPONIBILE"
    col_trib = "COD_TRIBUTO"

    nfs = read_csv_robust(nfs_path)
    missing_nfs = [c for c in (col_dat, col_nome, col_sdi, col_prot, col_imp, col_trib) if c not in nfs.columns]
    if missing_nfs:
        raise SystemExit(f"Colonne mancanti NFS: {missing_nfs}")

    work = nfs[[col_dat, col_nome, col_sdi, col_prot, col_imp, col_trib]].copy()
    work[col_dat] = normalize_text(work[col_dat])
    work[col_nome] = normalize_text(work[col_nome])
    work[col_sdi] = normalize_text(work[col_sdi]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    work[col_prot] = normalize_upper(work[col_prot])
    work[col_trib] = normalize_upper(work[col_trib])
    work["_IMP"] = to_number_it(work[col_imp])

    base_rows = int(len(work))
    work1 = work.drop_duplicates(subset=[col_dat, col_nome, col_sdi], keep="first")
    after_dedup = int(len(work1))
    work2 = work1[work1[col_prot].isin(allowed_prot)].copy()
    after_filter = int(len(work2))

    prot = work2[col_prot]
    mask_cart = prot.isin(cartacee_prot)
    mask_elet = prot.isin(elet_prot)
    mask_auto = prot.isin(autofatt_prot)

    # step 14: totale imponibile pagato = IMPONIBILE - (solo COD_TRIBUTO=I9)
    mask_i9 = work2[col_trib].eq("I9")
    total_imp = float(work2["_IMP"].sum())
    total_i9 = float(work2.loc[mask_i9, "_IMP"].sum())
    total_net = total_imp - total_i9
    i9_count = int(mask_i9.sum())

    # Pisa: cartacee/elettroniche da SDI vuoto/pieno
    pisa = pd.read_excel(pisa_path, sheet_name="Sheet1", dtype=str)
    if "Identificativo SDI" not in pisa.columns:
        raise SystemExit("Colonna Identificativo SDI mancante in Pisa")
    psdi = normalize_text(pisa["Identificativo SDI"]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    pisa_cart = psdi.eq("")
    pisa_elet = ~pisa_cart

    print("=== REPORT ISTRUZIONI AGGIORNATE ===")
    print("NFS_BASE_ROWS", base_rows)
    print("NFS_AFTER_DEDUP(DATA_FATTURA+RAGIONE_SOCIALE+IDENT_SDI)", after_dedup)
    print("NFS_AFTER_PROTOCOL_FILTER", after_filter)
    print()
    print("NFS_TOTALE_PER_SEZIONE (da protocollo)")
    print("CARTACEE", int(mask_cart.sum()))
    print("ELETTRONICHE", int(mask_elet.sum()))
    print("AUTOFATTURE", int(mask_auto.sum()))
    print("TOTALE", int((mask_cart | mask_elet | mask_auto).sum()))
    print()
    print("NFS_PROTOCOL_DISTRIBUTION_ALLOWED")
    for k, v in prot.value_counts().sort_index().items():
        print(f"{k}\t{int(v)}")
    print()
    print("PISA_COUNTS_BY_SDI")
    print("CARTACEE_SDI_VUOTO", int(pisa_cart.sum()))
    print("ELETTRONICHE_SDI_PIENO", int(pisa_elet.sum()))
    print("TOTALE", int(len(pisa)))
    print()
    # delta (istruzioni: NFS cartacee da protocolli; Pisa cartacee da SDI vuoto)
    print("DELTA_COUNTS_(PISA_MINUS_NFS)")
    print("CARTACEE", int(pisa_cart.sum()) - int(mask_cart.sum()))
    print("ELETTRONICHE", int(pisa_elet.sum()) - int((mask_elet | mask_auto).sum()))
    print()
    print("NFS_IMPORTO_IMPONIBILE_NETTO_(IMPONIBILE_-_I9)")
    print("IMPONIBILE_TOTALE", round(total_imp, 2))
    print("I9_TOTALE", round(total_i9, 2))
    print("NETTO", round(total_net, 2))
    print("I9_COUNT", i9_count)


if __name__ == "__main__":
    main()
