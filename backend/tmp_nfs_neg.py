from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.file_processor import CompareFTFileProcessor


def main() -> None:
    proc = CompareFTFileProcessor()
    nfs_path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")

    nfs_raw = proc._load_nfs_compare_df(nfs_path)
    if "DATA_GEN_MANDATO" in nfs_raw.columns and nfs_raw["DATA_GEN_MANDATO"].astype(str).str.strip().ne("").any():
        nfs_raw = proc._filter_by_file_quarter(nfs_raw, "DATA_GEN_MANDATO", nfs_path)

    prot = nfs_raw["FAT_PROT"].astype(str).str.strip().str.upper()
    nfs_f = nfs_raw[prot.isin(proc.NFS_ALLOWED_PROTOCOLS)].copy()
    nfs_d = nfs_f.drop_duplicates(subset=["FAT_NDOC", "FAT_DATDOC", "C_NOME"]).copy()

    nfs_df = nfs_d[proc.NFS_REQUIRED_COLUMNS].copy()
    nfs_df = nfs_df.rename(columns=proc.NFS_RENAME_MAP)

    seg = nfs_df["Segno"].fillna("").astype(str).str.strip().str.upper()
    imp = proc._to_number_series_it(nfs_df["Imponibile"]).fillna(0)
    prot2 = nfs_df["Prot."].astype(str).str.strip().str.upper()
    elet_mask = prot2.isin(proc.NFS_ELETTRONICHE_PROTOCOLS | proc.NFS_AUTOFATTURE_PROTOCOLS)

    neg_sum = float(imp[elet_mask & seg.eq("A")].sum())
    pos_sum = float(imp[elet_mask & ~seg.eq("A")].sum())
    neg_count = int((elet_mask & seg.eq("A")).sum())

    print("NFS eletr pos sum", round(pos_sum, 2))
    print("NFS eletr neg sum (A)", round(neg_sum, 2))
    print("NFS eletr net", round(pos_sum - neg_sum, 2))
    print("NFS neg_count", neg_count)


if __name__ == "__main__":
    main()

