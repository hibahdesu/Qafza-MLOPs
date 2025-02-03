from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import pandas as pd
from pandas import DataFrame
import joblib

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def train_model(df: DataFrame, **kwargs) -> pd.DataFrame:
    """
    Train a RandomForestClassifier on the processed data and return the trained model.
    """
    print(f"Input data to train_model: {type(df)}")  # Debugging line
    
    # Ensure that df is a DataFrame, if not, convert it to a DataFrame
    if not isinstance(df, pd.DataFrame):
        print(f"Warning: Data is not a DataFrame, converting it. Current type: {type(df)}")
        df = pd.DataFrame(df)  # Convert to DataFrame if it's not already one

    print(f"After conversion, Input data to train_model: {type(df)}")  # Debugging line

    # Assert that the data is a DataFrame
    assert isinstance(df, pd.DataFrame), f"Input data is not a DataFrame! Type: {type(df)}"

    # Split the data into features and target
    X = df.drop('Survived', axis=1)
    y = df['Survived']

    # Debugging: Check the shape of X and y
    print(f"Shape of X: {X.shape}, Shape of y: {y.shape}")

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train multiple models
    models = {
        'RandomForest': RandomForestClassifier(),
        'LogisticRegression': LogisticRegression(max_iter=1000),
        'SVC': SVC(),
        'KNeighbors': KNeighborsClassifier()
    }

    best_accuracy = 0
    best_model = None
    best_model_name = ""

    # Iterate through each model and evaluate performance using a pipeline
    for model_name, model in models.items():
        print(f"\nTraining {model_name} with pipeline...")

        # Create a pipeline that first scales data and then applies the model
        pipeline = Pipeline([
            ('scaler', StandardScaler()),  # Feature scaling
            ('classifier', model)          # Model training
        ])

        # Train the model
        pipeline.fit(X_train, y_train)

        # Evaluate the model
        y_pred = pipeline.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)

        print(f"{model_name} Accuracy: {accuracy:.4f}")

        # Save the best model
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = pipeline
            best_model_name = model_name

    print(f"\nBest Model: {best_model_name} with accuracy: {best_accuracy:.4f}")

    # Save the best model to a file
    model_filename = 'best_titanic_model_with_pipeline.pkl'
    joblib.dump(best_model, model_filename)
    print(f"Saving model of type: {type(best_model)} to {model_filename}")

    # Return the trained model
    return best_model
