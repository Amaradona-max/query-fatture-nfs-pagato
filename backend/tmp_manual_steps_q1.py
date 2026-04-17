from __future__ import annotations

from pathlib import Path

import pandas as pd


def to_number(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(" ", "", regex=False).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce").fillna(0)


def main() -> None:
    path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig", on_bad_lines="skip", dtype=str)

    allowed = {
        "P",
        "2P",
        "LABI",
        "FCBI",
        "FCSI",
        "FCBE",
        "FCSE",
        "EP",
        "2EP",
        "EL",
        "2EL",
        "EZ",
        "2EZ",
        "EZP",
        "FPIC",
        "FSIC",
        "FPEC",
        "FSEC",
    }

    date_gen = pd.to_datetime(df["DATA_GEN_MANDATO"], errors="coerce", dayfirst=True)
    mask_q1 = (date_gen >= pd.Timestamp("2026-01-01")) & (date_gen <= pd.Timestamp("2026-03-31"))
    base = df[mask_q1].copy()

    keys = pd.DataFrame(
        {
            "imp": base["IMPONIBILE"].astype(str).str.strip(),
            "date": base["DATA_FATTURA"].astype(str).str.strip(),
            "doc": base["N_DOCUMENTO"].astype(str).str.strip(),
        }
    )
    after1 = base[~keys.duplicated(keep="first")].copy()

    prot = after1["FT_PROT"].astype(str).str.strip().str.upper()
    after2 = after1[prot.isin(allowed)].copy()

    imp_sum = float(to_number(after2["IMPONIBILE"]).sum())
    pag_sum = float(to_number(after2["IMPORTO_PAGATO"]).sum())

    print("NFS rows after DATA_GEN_MANDATO Q1:", len(base))
    print("After dedup (IMPONIBILE+DATA_FATTURA+N_DOCUMENTO):", len(after1))
    print("After protocol filter:", len(after2))
    print("SUM IMPONIBILE:", round(imp_sum, 2))
    print("SUM IMPORTO_PAGATO:", round(pag_sum, 2))
    print("Top protocols:")
    print(prot.value_counts().head(12).to_string())


if __name__ == "__main__":
    main()

