from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.file_processor import CompareFTFileProcessor


def main() -> None:
    nfs = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    pisa = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture Pisa Pagato I° Trim.2026.xlsx")

    proc = CompareFTFileProcessor()
    nfs_raw = proc._load_nfs_compare_df(nfs)
    pisa_df = proc._load_pisa_compare_df(pisa)

    nfs_raw = proc._filter_by_file_quarter(nfs_raw, "FAT_DATREG", nfs)
    if "DATA_GEN_MANDATO" in nfs_raw.columns and nfs_raw["DATA_GEN_MANDATO"].astype(str).str.strip().ne("").any():
        nfs_raw = proc._filter_by_file_quarter(nfs_raw, "DATA_GEN_MANDATO", nfs)

    prot = nfs_raw["FAT_PROT"].astype(str).str.strip().str.upper()
    nfs_f = nfs_raw[prot.isin(proc.NFS_ALLOWED_PROTOCOLS)].copy()
    nfs_d = nfs_f.drop_duplicates(subset=["FAT_NDOC", "FAT_DATDOC", "IMPONIBILE"]).copy()

    nfs_d["IMPONIBILE"] = pd.to_numeric(nfs_d["IMPONIBILE"], errors="coerce").fillna(0)
    if "FAT_TOTFAT" in nfs_d.columns:
        nfs_d["FAT_TOTFAT"] = pd.to_numeric(nfs_d["FAT_TOTFAT"], errors="coerce").fillna(0)

    pisa_df["Importo fattura"] = proc._to_number_series(pisa_df["Importo fattura"]).fillna(0)

    print("NFS rows after filters+dedup:", len(nfs_d))
    print("Pisa rows after filters:", len(pisa_df))
    print("SUM NFS IMPONIBILE:", round(float(nfs_d["IMPONIBILE"].sum()), 2))
    print("SUM Pisa Importo fattura:", round(float(pisa_df["Importo fattura"].sum()), 2))
    if "FAT_TOTFAT" in nfs_d.columns:
        print("SUM NFS FAT_TOTFAT:", round(float(nfs_d["FAT_TOTFAT"].sum()), 2))

    prot_d = nfs_d["FAT_PROT"].astype(str).str.strip().str.upper()
    cart_mask = prot_d.isin(proc.NFS_CARTACEE_PROTOCOLS)
    elet_mask = prot_d.isin(proc.NFS_ELETTRONICHE_PROTOCOLS)
    print("NFS cart count:", int(cart_mask.sum()), "elet count:", int(elet_mask.sum()))
    print("NFS cart IMPONIBILE:", round(float(nfs_d.loc[cart_mask, "IMPONIBILE"].sum()), 2))
    print("NFS elet IMPONIBILE:", round(float(nfs_d.loc[elet_mask, "IMPONIBILE"].sum()), 2))
    if "FAT_TOTFAT" in nfs_d.columns:
        print("NFS cart FAT_TOTFAT:", round(float(nfs_d.loc[cart_mask, "FAT_TOTFAT"].sum()), 2))
        print("NFS elet FAT_TOTFAT:", round(float(nfs_d.loc[elet_mask, "FAT_TOTFAT"].sum()), 2))

    print("Pisa columns:", pisa_df.columns.tolist())


if __name__ == "__main__":
    main()

