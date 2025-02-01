import io
import pandas as pd
import requests
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:
    """
    Template for loading data from API
    """
    url = 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv?raw=True'

    df = pd.read_csv(url)


    # Ensure the data is a DataFrame
    assert isinstance(df, pd.DataFrame), f"Data loaded is not a DataFrame! Type: {type(df)}"
    print(f"Data loaded from API: {type(df)}") 

    return df



@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    # Ensure df is a DataFrame
    assert isinstance(df, pd.DataFrame), f"Output is not a DataFrame! Type: {type(df)}"
    
    assert df is not None, 'The output is undefined'

