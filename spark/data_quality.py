from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def filter_valid_records(df: DataFrame, conditions: F.Column) -> DataFrame:
    """
    Filters valid records from a DataFrame based on specified conditions.

    :param df: Input DataFrame
    :param conditions: Conditions to filter valid records
    :return: DataFrame with valid records
    """
    return df.filter(conditions)

def filter_invalid_records(df: DataFrame, conditions: F.Column) -> DataFrame:
    """
    Filters invalid records from a DataFrame based on specified conditions.

    :param df: Input DataFrame
    :param conditions: Conditions to filter invalid records
    :return: DataFrame with invalid records
    """
    return df.filter(~conditions)

def deduplicate_records(df: DataFrame, unique_keys: list) -> DataFrame:
    """
    Deduplicates records in a DataFrame based on specified unique keys.

    :param df: Input DataFrame
    :param unique_keys: List of columns to use as unique keys for deduplication
    :return: DataFrame with deduplicated records
    """
    return df.dropDuplicates(unique_keys)
