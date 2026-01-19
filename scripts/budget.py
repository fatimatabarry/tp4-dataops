import os
import requests
import pandas as pd
from datetime import datetime
import logging
import time

# --- Configuration des Logs ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration via variables d'environnement ---
BUDGET_URL = os.getenv("BUDGET_URL", "https://services.tresor.mr/secure/public/budget/level1/")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", 60))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": USER_AGENT}

def fetch_budget_json(url, retries=3):
    """Récupère les données JSON avec un mécanisme de retry."""
    for i in range(retries):
        try:
            logging.info(f"Tentative {i+1}/{retries} pour récupérer JSON depuis {url}")
            res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            res.raise_for_status()
            data = res.json()
            if "items" not in data:
                logging.error("Clé 'items' manquante dans le JSON")
                return None
            return data
        except Exception as e:
            logging.warning(f"Erreur sur tentative {i+1}: {e}")
            time.sleep(2)
    return None

def parse_budget_json(data):
    """Convertit la liste d'items en DataFrame propre (Q1.3 & Q1.4)."""
    items = data.get("items", [])
    if not items:
        logging.error("Aucun item trouvé dans le JSON")
        return pd.DataFrame()
    
    df = pd.DataFrame(items)
    
    # Sélection et renommage des colonnes pour le schéma cible
    cols_mapping = {
        "noTitle": "id_ministere",
        "title": "ministere_libelle",
        "total": "montant_initial",
        "used": "montant_depense",
        "available": "montant_disponible",
        "percent": "taux_execution"
    }
    
    # On ne garde que ce qui existe dans le JSON
    available_cols = [c for c in cols_mapping.keys() if c in df.columns]
    df = df[available_cols].rename(columns=cols_mapping)
    
    # Nettoyage et conversion numérique forcée
    numeric_cols = ["montant_initial", "montant_depense", "montant_disponible", "taux_execution"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Arrondir les valeurs pour la lisibilité
    df = df.round(2)
    
    # Ajout métadonnées de traçabilité
    df["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_url"] = BUDGET_URL
    
    return df

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    data = fetch_budget_json(BUDGET_URL)
    if not data:
        logging.error("Échec de la récupération des données JSON.")
        return
    
    df = parse_budget_json(data)
    if df.empty:
        logging.error("Le DataFrame est vide après traitement.")
        return
    
    # Sauvegarde avec encodage gérant les caractères spéciaux (arabe/français)
    output_path = os.path.join(OUTPUT_DIR, "budget_execution.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logging.info(f"Fichier sauvegardé avec succès : {output_path} ({len(df)} lignes)")

if __name__ == "__main__":
    main()