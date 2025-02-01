from mage_ai.io.file import FileIO
from pandas import DataFrame
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.
    Ensure that the input data is a DataFrame before exporting.
    """
    # Ensure df is a DataFrame before exporting
    assert isinstance(df, pd.DataFrame), f"Input data is not a DataFrame! Type: {type(df)}"
    print(f"Type: {type(df)}") 

    filepath = 'titanic_clean.csv'
    FileIO().export(df, filepath)
