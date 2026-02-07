import sys
from pathlib import Path
import json
import random

import streamlit as st

# allow imports from src/
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.predict import predict_one

ARTIFACTS_DIR = ROOT / "artifacts"
SPEC_PATH = ARTIFACTS_DIR / "feature_spec.json"
LOCATION_MAP_PATH = ARTIFACTS_DIR / "location_map.json"

# -------------------------
# Load artifacts
# -------------------------
@st.cache_resource
def load_spec():
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))

@st.cache_resource
def load_location_map():
    with open(LOCATION_MAP_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# -------------------------
# Streamlit config
# -------------------------
st.set_page_config(
    page_title="Projet BIG DATA - Prédiction de course",
    layout="centered",
)

st.title("🚕 Prédiction du prix d’une course")

spec = load_spec()
feature_cols = spec["feature_cols"]
categorical_cols = set(spec["categorical_cols"])
LOCATION_MAP = load_location_map()

# Inverse map: Zone name -> LocationID
REVERSE_LOCATION_MAP = {v: int(k) for k, v in LOCATION_MAP.items()}

st.markdown("### Paramètres principaux (pour la démo)")

record = {}

with st.form("prediction_form"):
    # Inputs principaux
    record["passenger_count"] = st.number_input(
        "Nombre de passagers", min_value=1, max_value=10, value=1
    )
    record["PULocationID"] = st.selectbox(
        "Ville de départ", options=list(REVERSE_LOCATION_MAP.keys())
    )
    record["DOLocationID"] = st.selectbox(
        "Ville d’arrivée", options=list(REVERSE_LOCATION_MAP.keys())
    )

    submitted = st.form_submit_button("Prédire")

if submitted:
    # Transformer les noms en IDs pour le modèle
    record["PULocationID"] = REVERSE_LOCATION_MAP[record["PULocationID"]]
    record["DOLocationID"] = REVERSE_LOCATION_MAP[record["DOLocationID"]]

    try:
        yhat = predict_one(record)
        st.success(f"💰 Prix total estimé : **{yhat:.2f} $**")
        st.markdown("### Inputs utilisés (tous les features)")
        st.json(record)
    except Exception as e:
        st.error(f"Erreur pendant la prédiction : {e}")

