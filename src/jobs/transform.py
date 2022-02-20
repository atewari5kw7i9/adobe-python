from src.jobs.utils.spark_utils import adobe_data
from pyspark.sql import SparkSession
from src.jobs.utils.Settings import Settings


def process(input_path):
    print("Starting HelloSpark")

    spark_config = Settings.spark_config
    spark_builder = SparkSession.builder.appName(Settings.app_name)
    for k, v in spark_config.items():
        spark_builder.config(k, v)
    spark = spark_builder.getOrCreate()
    output_path = Settings.output_path
    adobe_data_obj = adobe_data(spark, input_path)
    adobe_df = adobe_data_obj.select_adobe_fields(adobe_data_obj.adobe_raw_df)
    adobe_df = adobe_data_obj.cast_adobe_df(adobe_df)
    adobe_df = adobe_data_obj.explode_adobe_df(adobe_df)
    adobe_df = adobe_data_obj.filter_adobe_df(adobe_df)
    adobe_df = adobe_data_obj.split_adobe_df(adobe_df)
    adobe_df = adobe_data_obj.scrap_search_url(adobe_df)
    adobe_df = adobe_data_obj.group_result(adobe_df)
    adobe_data_obj.write_out_file(adobe_df, output_path)
    spark.stop()
