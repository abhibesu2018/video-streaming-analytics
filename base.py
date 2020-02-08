'''
This is base class of other ETL in the
video streaming analytics
'''

from pyspark.sql import DataFrame, SparkSession


class ETLPipeline(object):

    @classmethod
    def run(cls, spark, params):
        """

        Args:
            spark (SparkSession):
            dbutils:
            params (dict):

        Returns:
            DataFrame:

        """
        raise NotImplementedError

    @classmethod
    def extract(cls, spark, **kwargs):
        pass

    @classmethod
    def transform(cls, spark, **kwargs):
        pass

    @classmethod
    def load(cls, spark, final_df, **kwargs):
        pass