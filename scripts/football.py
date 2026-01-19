import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging

# --- Logs ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Variables d'environnement / configuration ---
FOOTBALL_URL = os.getenv(
    "FOOTBALL_URL",
    "https://www.tntsports.co.uk/football/mauritanian-league/2025-2026/calendar-results.shtml"
)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", 20))
HEADERS = {"User-Agent": os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")}

def fetch_football_page(url):
    """Récupérer le HTML de la page."""
    try:
        logging.info(f"Tentative de récupération de : {url}")
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        res.raise_for_status()
        return res.text
    except Exception as e:
        logging.error(f"Erreur HTTP lors de la récupération : {e}")
        return None

def parse_matches(html):
    """Parser le HTML et extraire les matches dans un DataFrame."""
    soup = BeautifulSoup(html, "lxml")

    results = []

    # Chaque bloc de date (ex. 10/01/2026) et les lignes qui suivent
    matchdays = soup.find_all(text=True)

    # Trouver des lignes qui ressemblent à des dates
    # et ensuite lire les 4 lignes suivantes comme équipes/scores
    texts = soup.get_text(separator="\n").split("\n")

    i = 0
    while i < len(texts):
        text = texts[i].strip()

        # Ex. 10/01/2026
        if text and "/" in text and text.count("/") == 2:
            try:
                date = datetime.strptime(text, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                i += 1
                continue

            # On lit les lignes qui suivent
            # Format visible (dans l’ordre) :
            # Equipe1 (score?) Equipe2
            # Parfois des matchs listés sous forme compressée
            j = i + 1
            while j < len(texts) and texts[j].strip():
                line = texts[j].strip()

                parts = line.split()
                # Si on a au moins 4 éléments, on peut trouver score
                # Ex. Kaédi 1 0 Nouakchott King´s
                if len(parts) >= 4:
                    # On suppose format : home_team home_score away_score away_team
                    home_team = parts[0]
                    if parts[1].isdigit() and parts[2].isdigit():
                        home_score = int(parts[1])
                        away_score = int(parts[2])
                        away_team = " ".join(parts[3:])
                        status = "played"
                    else:
                        home_score = None
                        away_score = None
                        away_team = " ".join(parts[1:])
                        status = "upcoming"

                    results.append({
                        "match_date": date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "home_score": home_score,
                        "away_score": away_score,
                        "status": status
                    })

                j += 1

        i += 1

    # Création DataFrame
    df = pd.DataFrame(results)

    # Ajouter métadonnées
    df["source_url"] = FOOTBALL_URL
    df["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Normaliser types
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce")

    # Suppression doublons
    df = df.drop_duplicates()

    return df

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html = fetch_football_page(FOOTBALL_URL)
    if not html:
        return

    df = parse_matches(html)
    if df.empty:
        logging.error("Aucun match trouvé ou DataFrame vide après parsing.")
        return

    output_path = os.path.join(OUTPUT_DIR, "football_results.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logging.info(f"Fichier football_results.csv généré : {output_path} ({len(df)} lignes)")

if __name__ == "__main__":
    main()
