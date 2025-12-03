from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import json

# -----------------------------
# Definição do modelo da request
# -----------------------------
class Record(BaseModel):
    scaled_amount: float
    scaled_time: float
    V1: float
    V2: float
    V3: float
    V4: float
    V5: float
    V6: float
    V7: float
    V8: float
    V9: float
    V10: float
    V11: float
    V12: float
    V13: float
    V14: float
    V15: float
    V16: float
    V17: float
    V18: float
    V19: float
    V20: float
    V21: float
    V22: float
    V23: float
    V24: float
    V25: float
    V26: float
    V27: float
    V28: float

# -----------------------------
# Inicializa FastAPI
# -----------------------------
app = FastAPI()

# -----------------------------
# Carrega o modelo
# -----------------------------
model = joblib.load("pipeline/modelo.pkl")

FEATURE_ORDER = [
    "scaled_amount",
    "scaled_time",
    "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8",
    "V9", "V10", "V11", "V12", "V13", "V14", "V15",
    "V16", "V17", "V18", "V19", "V20", "V21", "V22",
    "V23", "V24", "V25", "V26", "V27", "V28"
]

# -----------------------------
# Endpoint REST substituindo o gRPC
# -----------------------------
@app.get("/predict")
def predict(record: Record):
    # Converte a string JSON para dict
    # Transforma o objeto Pydantic em dicionário
    data_dict = record.dict()

    # Extrai features na ordem correta (ordem alfabética, igual ao seu código original)
    features = [data_dict[col] for col in FEATURE_ORDER]

    # Inferência
    pred = int(model.predict([features])[0])

    return {"is_fraud": pred == 1}


# -----------------------------
# Comando para rodar
# -----------------------------
# uvicorn fraud_api:app --host 0.0.0.0 --port 8000
