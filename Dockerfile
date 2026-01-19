# ----------------------------
# Dockerfile pour projet TP4
# ----------------------------

# Image de base
FROM python:3.10-slim

# Définir le répertoire de travail dans le container
WORKDIR /app

# Copier uniquement requirements.txt pour installer les dépendances
COPY requirements.txt .

# Mettre à jour pip et installer les dépendances
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=100 --retries=5 -r requirements.txt

# Copier seulement les fichiers nécessaires du projet
COPY scripts ./scripts
COPY data ./data

# Créer le dossier pour les sorties si besoin
RUN mkdir -p /app/shared_out

# Définir le point d'entrée pour exécuter le pipeline
CMD ["python", "scripts/pipeline.py"]
