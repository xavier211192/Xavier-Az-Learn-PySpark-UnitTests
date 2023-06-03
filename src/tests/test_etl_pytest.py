import pytest
from etl.etl import transform_data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    print("----Setup Spark Session---")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Unit-Tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.port.maxRetries", "30")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    print("--- Tear down Spark Session---")
    spark.stop()


@pytest.fixture(scope="session")
def input_data(spark):
    input_schema = StructType(
        [
            StructField("StoreID", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("ItemCount", IntegerType(), True),
        ]
    )
    input_data = [
        (1, "Bangalore", "2021-12-01", 5),
        (2, "Bangalore", "2021-12-01", 3),
        (5, "Amsterdam", "2021-12-02", 10),
        (6, "Amsterdam", "2021-12-01", 1),
        (8, "Warsaw", "2021-12-02", 15),
        (7, "Warsaw", "2021-12-01", 99),
    ]
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    return input_df


@pytest.fixture(scope="session")
def expected_data(spark):
    # Define an expected data frame
    expected_schema = StructType(
        [
            StructField("Location", StringType(), True),
            StructField("TotalItemCount", IntegerType(), True),
        ]
    )
    expected_data = [("Bangalore", 8), ("Warsaw", 114), ("Amsterdam", 11)]
    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
    return expected_df


def test_etl(spark, input_data, expected_data):
    # Apply transforamtion on the input data frame
    transformed_df = transform_data(input_data)

    # Compare schema of transformed_df and expected_df
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, transformed_df.schema.fields)]
    fields2 = [*map(field_list, expected_data.schema.fields)]
    res = set(fields1) == set(fields2)

    # assert
    # Compare data in transformed_df and expected_df
    assert sorted(expected_data.collect()) == sorted(transformed_df.collect())
