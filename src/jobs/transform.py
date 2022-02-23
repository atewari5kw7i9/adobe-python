from src.jobs.utils.spark_utils import adobe_data
from pyspark.sql import SparkSession
from src.jobs.utils.Settings import Settings


def process(input_path):
    '''
    This method accepts the input file and apply different methods on dataframes
    to transform the file in order to make meaningful revenue insights.
    '''
    try:
        print("Starting Adobe-Transform")

        # Create Spark configurations
        spark_config = Settings.spark_config
        spark_builder = SparkSession.builder.appName(Settings.app_name)
        for k, v in spark_config.items():
            spark_builder.config(k, v)
        spark = spark_builder.getOrCreate()

        output_path = Settings.output_path

        # Instantiate the spark_utils.adobe_data class
        adobe_data_obj = adobe_data(spark, input_path)

        # Only select data that is of interest
        adobe_df = adobe_data_obj.select_adobe_fields()

        # Create list datatype from product_list and event_list columns
        # New columns product_list_arr and event_list_arr will be added and corresponding
        # original string columns will be dropped
        adobe_df = adobe_data_obj.cast_adobe_df(adobe_df)

        # This method explode the product_list_arr and event_list_arr columns into rows.
        adobe_df = adobe_data_obj.explode_adobe_df(adobe_df)

        # Method filters the events of Purchase type (Event Type = 1)
        adobe_df = adobe_data_obj.filter_adobe_df(adobe_df)

        # Method splits product_attributes based on Semicolon and picks up
        # Fourth attribute which is related to the Total_Revenue
        adobe_df = adobe_data_obj.split_adobe_df(adobe_df)

        # Method scraps the referrer URL and extracts the Search Keyword and Domain Name
        adobe_df = adobe_data_obj.scrap_search_url(adobe_df)

        # Method group the result based on "domain_name", "search1" and sum total revenue
        adobe_df = adobe_data_obj.group_result(adobe_df)

        # Method writes data to S3 location
        adobe_data_obj.write_out_file(adobe_df, output_path)
        spark.stop()
    except Exception as e:
        print(str(e))
        raise
