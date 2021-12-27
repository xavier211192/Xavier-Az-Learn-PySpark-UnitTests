from pyspark.sql.types import *
from pyspark.sql.functions import *


def transform_data(input_df):
    transformed_df = (input_df.groupBy('Location',).agg(sum('ItemCount').alias('TotalItemCount')))
    return transformed_df
