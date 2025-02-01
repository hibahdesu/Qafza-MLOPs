from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
from pandas import DataFrame




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

    # Initialize and train the model
    model = RandomForestClassifier()
    model.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Debugging: Print accuracy
    print(f"Model Accuracy: {accuracy:.4f}")

    # Return the trained model
    return model





