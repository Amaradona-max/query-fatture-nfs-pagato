from __future__ import annotations

from pathlib import Path

import pandas as pd


def fast_count_rows_csv(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        return max(sum(1 for _ in f) - 1, 0)


def pick_column(columns: list[str], *candidates: str) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


def main() -> None:
    path = Path("/Users/prova/Desktop/query_fatture_nfs_pagato/Fatture NFS Pagato I° Trim.2026.csv")
    if not path.exists():
        raise SystemExit(f"File non trovato: {path}")

    raw_rows = fast_count_rows_csv(path)
    preview = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig", nrows=0)
    cols = list(preview.columns)

    col_imp = pick_column(cols, "IMPONIBILE", "Imponibile", "IMPONIBILE ")
    col_date = pick_column(cols, "DATA_FATTURA", "Data_Fattura", "DATA FATTURA", "DATA_FATTURA ")
    col_doc = pick_column(cols, "N_DOCUMENTO", "N.Documento", "N DOCUMENTO", "N_DOCUMENTO ")
    col_doc_alt = pick_column(cols, "N_FATTURA", "N.Fattura", "N FATTURA", "N_FATTURA ")
    col_prot = pick_column(cols, "FT_PROT", "FT_Prot", "FT_PROT ")

    missing = [k for k, v in [("IMPONIBILE", col_imp), ("DATA_FATTURA", col_date), ("N_DOCUMENTO", col_doc), ("FT_PROT", col_prot)] if v is None]
    if missing:
        raise SystemExit(f"Colonne non trovate: {', '.join(missing)}\nColonne presenti: {cols}")

    usecols = [col_imp, col_date, col_doc, col_prot]
    if col_doc_alt and col_doc_alt not in usecols:
        usecols.append(col_doc_alt)
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig", usecols=usecols, dtype=str)

    imp = df[col_imp].astype(str).str.strip()
    date = df[col_date].astype(str).str.strip()
    doc = df[col_doc].astype(str).str.strip()
    doc_alt = df[col_doc_alt].astype(str).str.strip() if col_doc_alt else None
    prot = df[col_prot].astype(str).str.strip().str.upper()

    def to_number_series(series: pd.Series) -> pd.Series:
        normalized = series.astype(str).str.replace(" ", "", regex=False).str.strip()
        has_dot = normalized.str.contains(r"\.", regex=True, na=False)
        has_comma = normalized.str.contains(",", regex=False, na=False)
        both_mask = has_dot & has_comma
        normalized = normalized.where(~both_mask, normalized.str.replace(".", "", regex=False))
        normalized = normalized.str.replace(",", ".", regex=False)
        return pd.to_numeric(normalized, errors="coerce")

    def parse_date_series(series: pd.Series) -> pd.Series:
        as_text = series.astype(str).str.strip()
        iso_mask = as_text.str.match(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")
        parsed_iso = pd.to_datetime(series.where(iso_mask), errors="coerce", dayfirst=False)
        parsed_other = pd.to_datetime(series.where(~iso_mask), errors="coerce", dayfirst=True)
        return parsed_iso.fillna(parsed_other)

    imp_num = to_number_series(imp)
    imp_num_round2 = imp_num.round(2)
    date_parsed = parse_date_series(date)
    date_day = date_parsed.dt.normalize()

    keys_raw = pd.DataFrame({"imp": imp, "date": date, "doc": doc})
    keep_first_raw = ~keys_raw.duplicated(keep="first")
    df1_raw = df[keep_first_raw]

    if doc_alt is not None:
        keys_raw_alt = pd.DataFrame({"imp": imp, "date": date, "doc": doc_alt})
        keep_first_raw_alt = ~keys_raw_alt.duplicated(keep="first")
        df1_raw_alt = df[keep_first_raw_alt]
    else:
        keep_first_raw_alt = None
        df1_raw_alt = None

    keys_norm = pd.DataFrame({"imp": imp_num, "date": date_parsed, "doc": doc})
    keep_first_norm = ~keys_norm.duplicated(keep="first")
    df1_norm = df[keep_first_norm]

    keys_norm_day = pd.DataFrame({"imp": imp_num, "date": date_day, "doc": doc})
    keep_first_norm_day = ~keys_norm_day.duplicated(keep="first")
    df1_norm_day = df[keep_first_norm_day]

    keys_norm_round2 = pd.DataFrame({"imp": imp_num_round2, "date": date_day, "doc": doc})
    keep_first_norm_round2 = ~keys_norm_round2.duplicated(keep="first")
    df1_norm_round2 = df[keep_first_norm_round2]

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
    prot1_raw = prot[keep_first_raw]
    keep_allowed_raw = prot1_raw.isin(allowed)
    df2_raw = df1_raw[keep_allowed_raw]

    if keep_first_raw_alt is not None:
        prot1_raw_alt = prot[keep_first_raw_alt]
        keep_allowed_raw_alt = prot1_raw_alt.isin(allowed)
        df2_raw_alt = df1_raw_alt[keep_allowed_raw_alt]
    else:
        df2_raw_alt = None

    prot1_norm = prot[keep_first_norm]
    keep_allowed_norm = prot1_norm.isin(allowed)
    df2_norm = df1_norm[keep_allowed_norm]

    prot1_norm_day = prot[keep_first_norm_day]
    keep_allowed_norm_day = prot1_norm_day.isin(allowed)
    df2_norm_day = df1_norm_day[keep_allowed_norm_day]

    prot1_norm_round2 = prot[keep_first_norm_round2]
    keep_allowed_norm_round2 = prot1_norm_round2.isin(allowed)
    df2_norm_round2 = df1_norm_round2[keep_allowed_norm_round2]

    # Variante: filtro protocolli prima, poi deduplica (come verifica)
    keep_allowed_first = prot.isin(allowed)
    df_allowed_first = df[keep_allowed_first]
    imp_af = imp[keep_allowed_first]
    date_af = date[keep_allowed_first]
    doc_af = doc[keep_allowed_first]
    keys_af = pd.DataFrame({"imp": imp_af, "date": date_af, "doc": doc_af})
    df_allowed_first_dedup = df_allowed_first[~keys_af.duplicated(keep="first")]

    print("File:", path.name)
    print("Righe iniziali (raw):", raw_rows)
    print("Righe dopo passaggio 1 (dedup su IMPONIBILE+DATA_FATTURA+N_DOCUMENTO) [raw string]:", len(df1_raw))
    print("Righe dopo passaggio 2 (filtro protocolli ammessi) [raw string]:", len(df2_raw))
    if df1_raw_alt is not None and df2_raw_alt is not None:
        print("Righe dopo passaggio 1 (dedup su IMPONIBILE+DATA_FATTURA+N_FATTURA) [raw string]:", len(df1_raw_alt))
        print("Righe dopo passaggio 2 (filtro protocolli ammessi) [N_FATTURA]:", len(df2_raw_alt))
    print("Righe dopo passaggio 1 (dedup) [normalizzato numero+data]:", len(df1_norm))
    print("Righe dopo passaggio 2 (filtro protocolli) [normalizzato numero+data]:", len(df2_norm))
    print("Righe dopo passaggio 1 (dedup) [data arrotondata a giorno]:", len(df1_norm_day))
    print("Righe dopo passaggio 2 (filtro protocolli) [data arrotondata a giorno]:", len(df2_norm_day))
    print("Righe dopo passaggio 1 (dedup) [IMPONIBILE arrotondato 2 decimali]:", len(df1_norm_round2))
    print("Righe dopo passaggio 2 (filtro protocolli) [IMPONIBILE arrotondato 2 decimali]:", len(df2_norm_round2))
    print("Righe (variante) filtro protocolli -> dedup:", len(df_allowed_first_dedup))


if __name__ == "__main__":
    main()
