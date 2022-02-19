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
        adobe_data_obj = adobe_data(self.spark, "data/adobe-data.tsv")
        adobe_df = adobe_data_obj.select_adobe_fields(adobe_data_obj.adobe_raw_df)
        result_count = adobe_df.count()
        self.assertEqual(result_count, 21, "Record count should be 21")

    # def test_country_count(self):
    #     sample_df = load_survey_df(self.spark, "data/sample.csv")
    #     count_list = count_by_country(sample_df).collect()
    #     count_dict = dict()
    #     for row in count_list:
    #         count_dict[row["Country"]] = row["count"]
    #     self.assertEqual(count_dict["United States"], 4, "Count for United States should be 4")
    #     self.assertEqual(count_dict["Canada"], 2, "Count for Canada should be 2")
    #     self.assertEqual(count_dict["United Kingdom"], 1, "Count for Unites Kingdom should be 1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
