import pickle

from mage_ai.io.file import FileIO
from mage_ai.data_preparation.decorators import data_exporter
from pandas import DataFrame

@data_exporter
def export_model_to_file(model, **kwargs) -> None:
    """
    Export the trained model to a file using Pickle
    """
    with open('titanic_model.pkl', 'wb') as model_file:
        pickle.dump(model, model_file)
    FileIO().export('titanic_model.pkl', 'titanic_model.pkl')
