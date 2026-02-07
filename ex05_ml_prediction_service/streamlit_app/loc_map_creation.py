import pandas as pd
import json
from pathlib import Path

# Chemin vers ton CSV
csv_path = Path("./references/taxi_zone_lookup.csv")
df = pd.read_csv(csv_path)

# On ne garde que les colonnes nécessaires
df = df[["Location ID", "Zone"]]

# Assurer que les IDs sont int
df["Location ID"] = df["Location ID"].astype(int)

# Générer le mapping ID -> Zone
LOCATION_MAP = dict(zip(df["Location ID"], df["Zone"]))

# Sauvegarder en JSON pour ton app Streamlit
json_path = Path("../artifacts/location_map.json")
json_path.parent.mkdir(exist_ok=True)
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(LOCATION_MAP, f, ensure_ascii=False, indent=2)

print(f"location_map.json généré avec {len(LOCATION_MAP)} zones")

