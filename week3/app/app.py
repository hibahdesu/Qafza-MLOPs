import joblib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import pandas as pd
from typing import Optional

# Load the trained model
model_filename = "/code/final_rf.joblib"
model = joblib.load(model_filename)

# FastAPI app initialization
app = FastAPI()

# Define a Pydantic model for the request body with default values for missing columns
class PredictionRequest(BaseModel):
    carat: float
    color: str
    clarity: str
    width: float
    table: Optional[float] = Field(0)
    y: Optional[float] = Field(0)
    z: Optional[float] = Field(0)
    cut: Optional[str] = Field("G")
    depth: Optional[float] = Field(0)
    x: Optional[float] = Field(0)

# Endpoint to perform prediction
@app.post("/predict/")
async def predict_price(request: PredictionRequest):
    # Prepare the input data into a DataFrame
    input_data = {
        "carat": request.carat,
        "color": request.color,
        "clarity": request.clarity,
        "width": request.width,
        "table": request.table,
        "y": request.y,
        "z": request.z,
        "cut": request.cut,
        "depth": request.depth,
        "x": request.x
    }

    df = pd.DataFrame([input_data])

    # Make prediction
    try:
        predicted_price = model.predict(df)
        rounded_price = round(predicted_price[0], 3)
        return {"predicted_price": f"{rounded_price}$"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@app.get("/info")
def info():
    return {"name": "Hibah", "Description": "Using the app"}

@app.get("/")
def read_root():
    return {"message": "Diamond Price Prediction API is working!"}

# 1189896f15247ac11fee729ca0ac268e94091da7d293dd7bc9ba486e53f8ac67

