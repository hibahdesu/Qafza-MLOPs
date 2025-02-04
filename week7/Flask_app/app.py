import pickle
import numpy as np
import pandas as pd
import joblib
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# Load the pre-trained model
model = joblib.load('best_titanic_model_with_pipeline.pkl')

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        # Get the JSON data from the request
        data = request.get_json()
        app.logger.debug(f"Received data: {data}")  # Log the data received
        
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Check if all required columns are in the input data
        required_columns = ['Age', 'Fare', 'Parch', 'Pclass', 'SibSp']
        if not all(col in data for col in required_columns):
            missing_cols = [col for col in required_columns if col not in data]
            return jsonify({"error": f"Missing columns: {missing_cols}"}), 400

        # Prepare the data for prediction
        input_data = pd.DataFrame([data])

        app.logger.debug(f"Input data for prediction: {input_data}")  # Log the input data

        # Make the prediction using the model
        prediction = model.predict(input_data)

        app.logger.debug(f"Prediction result: {prediction}")  # Log the prediction result

        # Return the prediction result
        response = {
            "prediction": "The passenger would survive the Titanic disaster." if prediction[0] == 1 else "The passenger would not survive the Titanic disaster."
        }
        return jsonify(response)

    except Exception as e:
        app.logger.error(f"Error: {str(e)}")  # Log the error
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
