from unittest import TestCase
from pyspark.sql import SparkSession
from src.jobs.utils.spark_utils import adobe_data


class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        adobe_data_obj = adobe_data(self.spark, "src/jobs/data/adobe-data.tsv")
        adobe_df = adobe_data_obj.select_adobe_fields(adobe_data_obj.adobe_raw_df)
        result_count = adobe_df.count()
        self.assertEqual(result_count, 21, "Record count should be 21")

    def test_cast_filer(self):
        adobe_data_obj = adobe_data(self.spark, "src/jobs/data/adobe-data.tsv")
        adobe_df = adobe_data_obj.select_adobe_fields(adobe_data_obj.adobe_raw_df)
        adobe_df = adobe_data_obj.cast_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.explode_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.filter_adobe_df(adobe_df)
        result_count = adobe_df.count()
        self.assertEqual(result_count, 5, "Record count should be 5")


    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
