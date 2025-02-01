from mage_ai.data_preparation.decorators import pipeline

@pipeline
def titanic_pipeline():
    # Load data from the API
    df = load_data_from_api()

    # Transform the data (feature engineering and missing value handling)
    transformed_df = transform_df(df)

    # Train the model
    model = train_model(transformed_df)

    # Export the model to a file
    export_model_to_file(model)
