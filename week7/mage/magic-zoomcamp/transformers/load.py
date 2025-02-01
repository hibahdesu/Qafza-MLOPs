import pickle



# Save model using pickle
with open('titanic_model.pkl', 'wb') as model_file:
    pickle.dump(model, model_file)

    

model = pickle.load(open('titanic_model.pkl', 'rb'))  # If you used pickle
