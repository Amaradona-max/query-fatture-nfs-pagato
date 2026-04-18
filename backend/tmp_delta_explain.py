from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.file_processor import CompareFTFileProcessor


def main() -> None:
    nfs_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    pisa_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx")

    proc = CompareFTFileProcessor()

    nfs_raw = proc._load_nfs_compare_df(nfs_path)
    pisa_df = proc._load_pisa_compare_df(pisa_path)

    if "DATA_GEN_MANDATO" in nfs_raw.columns and nfs_raw["DATA_GEN_MANDATO"].astype(str).str.strip().ne("").any():
        nfs_raw = proc._filter_by_file_quarter(nfs_raw, "DATA_GEN_MANDATO", nfs_path)
    else:
        nfs_raw = proc._filter_by_file_quarter(nfs_raw, "FAT_DATREG", nfs_path)
    pisa_df = proc._filter_by_file_quarter(pisa_df, "Data emissione", pisa_path)

    prot = nfs_raw["FAT_PROT"].astype(str).str.strip().str.upper()
    nfs_f = nfs_raw[prot.isin(proc.NFS_ALLOWED_PROTOCOLS)].copy()
    nfs_d = nfs_f.drop_duplicates(subset=["FAT_NDOC", "FAT_DATDOC", "C_NOME"]).copy()

    nfs = nfs_d[proc.NFS_REQUIRED_COLUMNS].copy()
    nfs.rename(columns=proc.NFS_RENAME_MAP, inplace=True)
    nfs["Data Fatture"] = proc._parse_date_series(nfs["Data Fatture"])
    nfs["Datat reg."] = proc._parse_date_series(nfs["Datat reg."])

    nfs["Imponibile"] = proc._to_number_series_it(nfs["Imponibile"]).fillna(0)
    segno = nfs["Segno"].fillna("").astype(str).str.strip().str.upper()
    mult = segno.eq("A").map({True: -1.0, False: 1.0})
    nfs["Importo Pagamento"] = (nfs["Imponibile"] * mult).round(2)

    pisa_df["Data emissione"] = proc._parse_date_series(pisa_df["Data emissione"])
    pisa_df["Importo fattura"] = proc._to_number_series(pisa_df["Importo fattura"]).fillna(0)

    nfs["_SDI_KEY"] = proc._normalize_sdi(nfs["Identificativo SDI"])
    pisa_df["_SDI_KEY"] = proc._normalize_sdi(pisa_df["Identificativo SDI"])

    nfs_protocol = nfs["Prot."].astype(str).str.strip().str.upper()
    nfs_elet = nfs[nfs_protocol.isin(proc.NFS_ELETTRONICHE_PROTOCOLS | proc.NFS_AUTOFATTURE_PROTOCOLS)].copy()
    pisa_elet = pisa_df[~proc._is_empty_sdi(pisa_df["_SDI_KEY"])].copy()

    nfs_total = float(nfs_elet["Importo Pagamento"].sum())
    pisa_total = float(pisa_elet["Importo fattura"].sum())

    nfs_grp = nfs_elet.groupby("_SDI_KEY", dropna=False)["Importo Pagamento"].sum().rename("nfs_sum")
    pisa_grp = pisa_elet.groupby("_SDI_KEY", dropna=False)["Importo fattura"].sum().rename("pisa_sum")
    merged = pd.concat([nfs_grp, pisa_grp], axis=1).fillna(0)
    merged["delta"] = (merged["nfs_sum"] - merged["pisa_sum"]).round(2)

    only_pisa = merged[(merged["nfs_sum"] == 0) & (merged["pisa_sum"] != 0)].copy()
    only_nfs = merged[(merged["pisa_sum"] == 0) & (merged["nfs_sum"] != 0)].copy()
    both = merged[(merged["pisa_sum"] != 0) & (merged["nfs_sum"] != 0)].copy()

    print("ELET totals")
    print("NFS_count", len(nfs_elet), "NFS_sum", round(nfs_total, 2))
    print("Pisa_count", len(pisa_elet), "Pisa_sum", round(pisa_total, 2))
    print("Delta", round(nfs_total - pisa_total, 2))
    print()
    print("Breakdown by SDI sums")
    print("SDI_only_Pisa", len(only_pisa), "sum_only_Pisa", round(float(only_pisa["pisa_sum"].sum()), 2))
    print("SDI_only_NFS", len(only_nfs), "sum_only_NFS", round(float(only_nfs["nfs_sum"].sum()), 2))
    print("SDI_in_both", len(both), "sum_delta_in_both", round(float(both["delta"].sum()), 2))
    print()
    print("Top 10 SDI by abs(delta) among shared SDI")
    top = both.reindex(both["delta"].abs().sort_values(ascending=False).head(10).index)
    for sdi, row in top.iterrows():
        print(sdi, "nfs", round(float(row["nfs_sum"]), 2), "pisa", round(float(row["pisa_sum"]), 2), "delta", round(float(row["delta"]), 2))
    print()
    print("First 10 SDI only Pisa (count mismatch drivers)")
    for sdi, row in only_pisa.head(10).iterrows():
        print(sdi, "pisa_sum", round(float(row["pisa_sum"]), 2))


if __name__ == "__main__":
    main()

