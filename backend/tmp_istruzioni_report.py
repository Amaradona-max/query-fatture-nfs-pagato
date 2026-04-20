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


def normalize_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def normalize_upper(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def main() -> None:
    nfs_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    pisa_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx")

    cartacee_prot = {"P", "2P", "L", "FCBI", "FCSI", "FCBE", "FCSE"}
    elet_prot = {"EP", "2EP", "EZ", "2EZ", "EZP", "EL", "2EL", "FPIC", "FSIC", "FPEC", "FSEC"}
    autofatt_prot = {"AFIC", "ASIC", "AFEC", "ASEC", "ACBI", "ACSI", "ACBE", "ACSE"}
    allowed_prot = cartacee_prot | elet_prot | autofatt_prot

    nfs = read_csv_robust(nfs_path)

    col_doc = "N_DOCUMENTO"
    col_dat = "DATA_FATTURA"
    col_nome = "RAGIONE SOCIALE"
    col_prot = "FT_PROT"
    col_sdi = "IDENT_SDI"
    col_imp = "IMPONIBILE"
    col_trib = "COD_TRIBUTO"

    missing = [c for c in (col_doc, col_dat, col_nome, col_prot, col_sdi, col_imp, col_trib) if c not in nfs.columns]
    if missing:
        raise SystemExit(f"Colonne mancanti NFS: {missing}")

    work = nfs[[col_doc, col_dat, col_nome, col_prot, col_sdi, col_imp, col_trib]].copy()
    work[col_doc] = normalize_text(work[col_doc])
    work[col_dat] = normalize_text(work[col_dat])
    work[col_nome] = normalize_text(work[col_nome])
    work[col_prot] = normalize_upper(work[col_prot])
    work[col_sdi] = normalize_text(work[col_sdi]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    work[col_trib] = normalize_upper(work[col_trib])
    work["_IMP"] = to_number_it(work[col_imp])

    base_rows = int(len(work))
    work1 = work.drop_duplicates(subset=[col_doc, col_dat, col_nome], keep="first")
    after_dedup = int(len(work1))
    work2 = work1[work1[col_prot].isin(allowed_prot)].copy()
    after_filter = int(len(work2))

    prot = work2[col_prot]
    mask_cart = prot.isin(cartacee_prot)
    mask_elet = prot.isin(elet_prot)
    mask_auto = prot.isin(autofatt_prot)

    mask_i9 = work2[col_trib].eq("I9")

    def net_amount(mask: pd.Series) -> tuple[float, float, float, int]:
        total = float(work2.loc[mask, "_IMP"].sum())
        i9 = float(work2.loc[mask & mask_i9, "_IMP"].sum())
        net = total - i9
        i9_cnt = int((mask & mask_i9).sum())
        return total, i9, net, i9_cnt

    cart_cnt = int(mask_cart.sum())
    elet_cnt = int(mask_elet.sum())
    auto_cnt = int(mask_auto.sum())

    cart_total, cart_i9, cart_net, cart_i9_cnt = net_amount(mask_cart)
    elet_total, elet_i9, elet_net, elet_i9_cnt = net_amount(mask_elet)
    auto_total, auto_i9, auto_net, auto_i9_cnt = net_amount(mask_auto)
    all_total, all_i9, all_net, all_i9_cnt = net_amount(mask_cart | mask_elet | mask_auto)

    pisa = pd.read_excel(pisa_path, sheet_name="Sheet1", dtype=str)
    if "Identificativo SDI" not in pisa.columns:
        raise SystemExit("Colonna Identificativo SDI mancante in Pisa")
    psdi = normalize_text(pisa["Identificativo SDI"]).replace({"nan": "", "None": "", "none": "", "NULL": "", "null": ""})
    pisa_cart = psdi.eq("")
    pisa_elet = ~pisa_cart

    print("=== REPORT ISTRUZIONI (NFS Pagato vs Pisa Pagato) ===")
    print("NFS_BASE_ROWS", base_rows)
    print("NFS_AFTER_DEDUP", after_dedup)
    print("NFS_AFTER_PROTOCOL_FILTER", after_filter)
    print()
    print("NFS_COUNTS_BY_PROTOCOL_CATEGORY")
    print("CARTACEE", cart_cnt)
    print("ELETTRONICHE", elet_cnt)
    print("AUTOFATTURE", auto_cnt)
    print("TOTALE", cart_cnt + elet_cnt + auto_cnt)
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
    print("DELTA_COUNTS_(PISA_MINUS_NFS)")
    print("CARTACEE", int(pisa_cart.sum()) - cart_cnt)
    print("ELETTRONICHE", int(pisa_elet.sum()) - int((mask_elet | mask_auto).sum()))
    print()
    print("NFS_IMPORTO_PAGATO_RULE_IMPONIBILE_MINUS_I9")
    print("CARTACEE_TOTAL", round(cart_total, 2), "CARTACEE_I9", round(cart_i9, 2), "CARTACEE_NET", round(cart_net, 2), "CARTACEE_I9_COUNT", cart_i9_cnt)
    print("ELETTRONICHE_TOTAL", round(elet_total, 2), "ELETTRONICHE_I9", round(elet_i9, 2), "ELETTRONICHE_NET", round(elet_net, 2), "ELETTRONICHE_I9_COUNT", elet_i9_cnt)
    print("AUTOFATTURE_TOTAL", round(auto_total, 2), "AUTOFATTURE_I9", round(auto_i9, 2), "AUTOFATTURE_NET", round(auto_net, 2), "AUTOFATTURE_I9_COUNT", auto_i9_cnt)
    print("TOTALE_TOTAL", round(all_total, 2), "TOTALE_I9", round(all_i9, 2), "TOTALE_NET", round(all_net, 2), "TOTALE_I9_COUNT", all_i9_cnt)


if __name__ == "__main__":
    main()

