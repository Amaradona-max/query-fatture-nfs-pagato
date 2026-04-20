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

    nfs = read_csv_robust(nfs_path)
    need = ["DATA_FATTURA", "RAGIONE SOCIALE", "IDENT_SDI", "FT_PROT", "FT_SEGNO", "IMPONIBILE", "IMP_TOT_RIT"]
    missing = [c for c in need if c not in nfs.columns]
    if missing:
        raise SystemExit(f"Mancano colonne: {missing}")

    w = nfs[need].copy()
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
    w["IMPONIBILE"] = to_number_it(w["IMPONIBILE"])
    w["IMP_TOT_RIT"] = to_number_it(w["IMP_TOT_RIT"])

    # dedup + filter protocolli
    w = w.drop_duplicates(subset=["DATA_FATTURA", "RAGIONE SOCIALE", "IDENT_SDI"], keep="first")
    w = w[w["FT_PROT"].isin(allowed_prot)].copy()

    # apply segno A
    neg = w["FT_SEGNO"].eq("A")
    w.loc[neg, "IMPONIBILE"] = -w.loc[neg, "IMPONIBILE"]
    w.loc[neg, "IMP_TOT_RIT"] = -w.loc[neg, "IMP_TOT_RIT"]

    # subtract rit only for EL/2EL
    mask_el = w["FT_PROT"].isin({"EL", "2EL"})
    base_total = float(w["IMPONIBILE"].sum())
    rit_el = float(w.loc[mask_el, "IMP_TOT_RIT"].sum())
    net_total = base_total - rit_el

    print(f"IMPONIBILE_TOTALE_CON_SEGNO = {base_total:,.2f}")
    print(f"RIT_EL2EL_DA_SOTTRARRE = {rit_el:,.2f}")
    print(f"IMPONIBILE_NETTO_PAGATO = {net_total:,.2f}")


if __name__ == "__main__":
    main()

