import sys
from src.jobs.utils.logger_utils import Log4j
from src.jobs.utils.spark_utils import *
from pyspark.sql.functions import split, col


def process(spark, input_path, output_path, save_mode='append'):
    logger = Log4j(spark)

    # if len(sys.argv) != 2:
    #     logger.error("Usage: HelloSpark <filename>")
    #     sys.exit(-1)

    logger.info("Starting HelloSpark")

    adobe_raw_df = load_adobe_df(spark, input_path)
    adobe_filtered_df = select_adobe_df(adobe_raw_df)
    adobe_filtered_cast_df = adobe_filtered_df.select(split(col("product_list"), ",").alias("product_list_arr"),\
                                                      split(col("event_list"), ",").alias("event_list_arr"),\
                                                      col("event_list"), col("page_url"), col("referrer")) \
        .drop("product_list").drop("event_list")
    adobe_explode_df = explode_adobe_df(adobe_filtered_cast_df)
    adobe_df = filter_adobe_df(adobe_explode_df)
    adobe_df = split_adobe_df(adobe_df)
    adobe_df = scrap_search_url(adobe_df)
    write_out_file(adobe_df, output_path, save_mode)

    logger.info("Finished HelloSpark")
    spark.stop()