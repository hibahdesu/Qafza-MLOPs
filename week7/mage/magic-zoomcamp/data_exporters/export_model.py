import joblib
@data_exporter
def export_model_to_file(model, **kwargs) -> None:
    filepath = 'titanic_model.pkl'
    
    joblib.dump(model, filepath)



import pickle

# Save model using pickle
with open('titanic_model2.pkl', 'wb') as model_file:
    pickle.dump(model, model_file)

print(f"Saving model of type: {type(model)}")
model = joblib.load('titanic_model.pkl', mmap_mode='r')


    
# def titanic_pipeline():
#     # Load the data
#     df = load_data_from_api()

#     # Transform the data (feature engineering and missing value handling)
#     transformed_df = transform_df(df)

#     # Train the model
#     model = train_model(transformed_df)

#     # Export the model to a file
#     export_model_to_file(model)