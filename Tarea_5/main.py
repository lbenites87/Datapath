from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd

# Cargar modelo entrenado
model = joblib.load("modelo_procesos_servidor.pkl")

# Crear la app
app = FastAPI(title="API Clasificación de Procesos Servidores")

# Esquema de entrada
class Proceso(BaseModel):
    Uso_CPU: float
    Uso_Memoria: float
    Numero_Hilos: int
    Tiempo_Ejecucion: float
    Numero_Errores: int
    Tipo_Proceso: str  # Servicio, Aplicación o Sistema

@app.get("/")
def inicio():
    return {"mensaje": "API funcionando. Usa /predict para clasificar un proceso."}

@app.post("/predict")
def predecir_proceso(p: Proceso):
    datos = pd.DataFrame([p.dict()])
    pred = model.predict(datos)[0]
    prob = model.predict_proba(datos)[0][1]
    
    resultado = "Problemático" if pred == 1 else "Normal"
    return {
        "Prediccion": resultado,
        "Probabilidad_Problema": round(prob, 4)
    }
