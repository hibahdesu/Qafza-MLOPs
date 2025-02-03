import streamlit as st
import pandas as pd
import numpy as np
import joblib
from sklearn.preprocessing import StandardScaler

# Load the pre-trained model
model = joblib.load('best_titanic_model_with_pipeline.pkl')

# Function to make prediction
def predict_survival(features):
    # Make prediction using the loaded model
    prediction = model.predict([features])
    return prediction[0]

# Streamlit UI
st.title('Titanic Survival Prediction')

st.write("""
    Enter the passenger information below to predict if they would survive the Titanic disaster.
    """)

# Collect user input for the features
age = st.number_input('Age', min_value=0, max_value=100, value=30)
fare = st.number_input('Fare', min_value=0.0, max_value=500.0, value=50.0)
parch = st.number_input('Number of Parents/Children aboard', min_value=0, max_value=10, value=0)
pclass = st.selectbox('Passenger Class', options=[1, 2, 3], index=1)
sibsp = st.number_input('Number of Siblings/Spouses aboard', min_value=0, max_value=10, value=0)

# Collecting user input for features
user_input = [age, fare, parch, pclass, sibsp]

# Display the input data
st.write(f"Passenger Information: Age={age}, Fare={fare}, Parch={parch}, Pclass={pclass}, SibSp={sibsp}")

# Prediction button
if st.button('Predict Survival'):
    prediction = predict_survival(user_input)
    if prediction == 1:
        st.success("The passenger would survive the Titanic disaster.")
    else:
        st.error("The passenger would not survive the Titanic disaster.")
