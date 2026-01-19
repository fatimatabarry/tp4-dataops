import os
import json
import logging
from datetime import datetime

# Importer vos scripts existants
import football
import inpc_pdf
import budget

# --- Configuration logs ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

OUTPUT_DIR = "./data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_pipeline():
    kpi = {}
    sources = {
        "football": football.main,
        "inpc": inpc_pdf.main,
        "budget": budget.main
    }

    for name, func in sources.items():
        try:
            logging.info(f"Lancement de la collecte {name}")
            func()
            # Essayer de compter les lignes produites
            filepath = os.path.join(OUTPUT_DIR, f"{name}_results.csv" if name=="football" else f"{name}_table2.csv")
            if os.path.exists(filepath):
                import pandas as pd
                df = pd.read_csv(filepath)
                kpi[name] = {
                    "status": "OK",
                    "rows": len(df),
                    "missing_values": df.isna().sum().sum(),
                    "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            else:
                kpi[name] = {"status": "FAIL", "rows": 0, "missing_values": None, "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        except Exception as e:
            logging.error(f"Erreur lors de la collecte {name} : {e}")
            kpi[name] = {"status": "FAIL", "rows": 0, "missing_values": None, "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    # Sauvegarde des métriques
    kpi_path = os.path.join(OUTPUT_DIR, "kpi.json")
    with open(kpi_path, "w") as f:
        json.dump(kpi, f, indent=4)
    logging.info(f"kpi.json généré : {kpi_path}")

if __name__ == "__main__":
    run_pipeline()
