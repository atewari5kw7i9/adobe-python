from unittest import TestCase
from pyspark.sql import SparkSession
from src.jobs.utils.spark_utils import adobe_data

data_file = 'src/jobs/data/adobe-data.tsv'
#data_file = 'data/adobe-data.tsv'
class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        adobe_data_obj = adobe_data(self.spark, data_file)
        adobe_df = adobe_data_obj.select_adobe_fields()
        result_count = adobe_df.count()
        self.assertEqual(result_count, 21, "Record count should be 21")

    def test_cast_filer(self):
        adobe_data_obj = adobe_data(self.spark, data_file)
        adobe_df = adobe_data_obj.select_adobe_fields()
        adobe_df = adobe_data_obj.cast_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.explode_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.filter_adobe_df(adobe_df)
        result_count = adobe_df.count()
        self.assertEqual(result_count, 5, "Record count should be 5")

    def test_end_results(self):
        adobe_data_obj = adobe_data(self.spark, data_file)
        adobe_df = adobe_data_obj.select_adobe_fields()
        adobe_df = adobe_data_obj.cast_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.explode_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.filter_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.split_adobe_df(adobe_df)
        adobe_df = adobe_data_obj.scrap_search_url(adobe_df)
        adobe_df_list = adobe_data_obj.group_result(adobe_df).collect()
        adobe_dict = {}
        for row in adobe_df_list:
            combined_dict_key = row["domain_name"]+"-"+str(row["search1"])
            adobe_dict[combined_dict_key] = row["Total_Revenue"]
        self.assertEqual(adobe_dict["www.esshopzilla.com-None"], 3290.0, "1st Sum should be 3290.0")
        self.assertEqual(adobe_dict["www.esshopzilla.com-Testing"], 2500, "2nd Sum should be 3290.0")
        self.assertEqual(adobe_dict["www.esshopzilla.com-Ultimate"], 480.0, "3rd Sum should be 480.0")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
