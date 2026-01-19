import requests
import camelot
import pandas as pd
import os
from datetime import datetime
import logging

# --- Configuration des logs ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Dossier de sortie
OUTPUT_DIR = "./data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# URL directe du PDF (d’après ton dernier message)
PDF_URL = "https://ansade.mr/wp-content/uploads/2026/01/Note-INPC-decembre-2025_FR_VF.pdf"

def download_pdf(pdf_url):
    """Télécharge le PDF de manière robuste"""
    logging.info(f"Téléchargement du PDF : {pdf_url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/pdf"
    }
    resp = requests.get(pdf_url, headers=headers, timeout=30)
    resp.raise_for_status()  # Lève une exception si code != 200
    pdf_path = os.path.join(OUTPUT_DIR, "INPC-2025-12.pdf")
    with open(pdf_path, "wb") as f:
        f.write(resp.content)
    if os.path.getsize(pdf_path) < 100000:  # 100 Ko minimal
        raise ValueError("PDF trop petit ou incorrect.")
    return pdf_path

def extract_table2(pdf_path):
    """Extrait le Tableau 2 avec Camelot"""
    logging.info(f"Extraction du Tableau 2 depuis {pdf_path}")
    tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
    if len(tables) < 2:
        raise ValueError("Tableau 2 non trouvé dans le PDF.")
    df = tables[1].df  # Le 2ᵉ tableau est le Tableau 2
    return df

def clean_table(df):
    """Nettoyage et normalisation du tableau"""
    logging.info("Nettoyage du tableau")
    # Première ligne comme header
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.iloc[0]]
    df = df[1:].reset_index(drop=True)
    
    # Conversion des colonnes numériques (après la première colonne)
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col].str.replace(",", ".").str.replace(" ", ""), errors="coerce")
    
    # Ajout date de scraping
    df['scraped_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def main():
    try:
        pdf_path = download_pdf(PDF_URL)
        df = extract_table2(pdf_path)
        df = clean_table(df)
        out_csv = os.path.join(OUTPUT_DIR, "inpc_table2.csv")
        df.to_csv(out_csv, index=False)
        logging.info(f"inpc_table2.csv généré ({len(df)} lignes)")
    except Exception as e:
        logging.error(f"Erreur INPC PDF : {e}")

if __name__ == "__main__":
    main()
