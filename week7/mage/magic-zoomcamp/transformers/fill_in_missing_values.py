from pandas import DataFrame
import math
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def select_number_columns(df: DataFrame) -> DataFrame:
    return df[['Age', 'Fare', 'Parch', 'Pclass', 'SibSp', 'Survived']]


def fill_missing_values_with_median(df: DataFrame) -> DataFrame:
    for col in df.columns:
        values = sorted(df[col].dropna().tolist())
        median_value = values[math.floor(len(values) / 2)]
        df[[col]] = df[[col]].fillna(median_value)
    return df

@transformer
def transform_df(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Transform the DataFrame by selecting relevant columns and filling missing values.
    """
    print(f"Type of data before transformation: {type(df)}")  # Debugging line

    # Ensure input is a DataFrame
    assert isinstance(df, pd.DataFrame), f"Input data is not a DataFrame! Type: {type(df)}"

    # Perform the transformations
    transformed_df = fill_missing_values_with_median(select_number_columns(df))

    # Ensure the output is a DataFrame
    assert isinstance(transformed_df, pd.DataFrame), f"Output of transform_df is not a DataFrame! Type: {type(transformed_df)}"
    print(f"Type of Transformed df is: {type(transformed_df)}") 

    return transformed_df



@test
def test_output(df) -> None:
    """
    Template code for testing the output of the block.
    """
    # Ensure df is a DataFrame
    assert isinstance(df, pd.DataFrame), f"Output is not a DataFrame! Type: {type(df)}"
    
    assert df is not None, 'The output is undefined'

