from pyspark.sql.functions import explode, col, split
from src.jobs.utils.Settings import Settings


class adobe_data:
    '''
    This is the helper class which holds all transformation methods. These methods are
    called from transform.py module
    '''

    def __init__(self, spark, input_path):
        self.app_name = 'Adobe-Data-Product'
        self.adobe_raw_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", "\t") \
            .csv(input_path)

    def select_adobe_fields(self):
        print("Step1")
        select_cols = Settings.columns_select
        return self.adobe_raw_df.select(*select_cols)

    def cast_adobe_df(self, adobe_df):
        print("Step2")
        return adobe_df.select(split(col("product_list"), ",").alias("product_list_arr"),\
                               split(col("event_list"), ",").alias("event_list_arr"),\
                               col("event_list"), col("page_url"), col("referrer")) \
                               .drop("product_list").drop("event_list")

    def explode_adobe_df(self, adobe_df):
        print("Step3")
        adobe_df_tmp = adobe_df.select(explode(adobe_df.product_list_arr).alias("product_attributes"), adobe_df.event_list_arr, adobe_df.page_url, adobe_df.referrer)
        adobe_df_tmp = adobe_df_tmp.select(explode(adobe_df_tmp.event_list_arr).alias("events_types"), adobe_df_tmp.product_attributes, adobe_df_tmp.page_url, adobe_df_tmp.referrer)
        adobe_df_tmp = adobe_df_tmp.withColumn("events_types", col("events_types").cast("int"))
        return adobe_df_tmp

    def filter_adobe_df(self, adobe_df):
        print("Step4")
        return adobe_df.filter("events_types = 1") \
             .select("events_types", "product_attributes", "page_url", "referrer")

    def split_adobe_df(self, adobe_df):
        print("Step5")
        split_cols = split(adobe_df['product_attributes'], ';')
        df1 = adobe_df.withColumn('Total_Revenue', split_cols.getItem(Settings.revenue_locator))
        return df1

    def scrap_search_url(self, adobe_df):
        print("Step6")
        search_str_begin = Settings.search_str
        search_str_terminator = Settings.terminate_str
        domain_str_begin = Settings.domain_str_begin
        domain_str_end = Settings.domain_str_end

        df1 = adobe_df.withColumn("search1", split(split(adobe_df.referrer, search_str_begin)[1], search_str_terminator)[0])
        df1 = df1.withColumn("domain_name", split(split(adobe_df.referrer, domain_str_begin)[1], domain_str_end)[0])
        df1 = df1.drop("referrer").drop("page_url")
        return df1

    def group_result(self, adobe_df):
        print("Step7")
        df = adobe_df.selectExpr("cast(domain_name as string) domain_name",
                             "cast(search1 as string) search1",
                             "cast(Total_Revenue as double) Total_Revenue")
        summary_df = df.groupBy("domain_name", "search1").\
                        sum("Total_Revenue"). \
                        withColumnRenamed("domain_name", "Search Engine Domain"). \
                        withColumnRenamed("search1", "Search Keyword"). \
                        withColumnRenamed("sum(Total_Revenue)", "Revenue").\
                        sort(col("Revenue").desc())

        return summary_df

    def write_out_file(self, adobe_df, s3_out_path):
        print("Step8")
        try:
            #adobe_df.show()
            adobe_df.coalesce(1).\
                write.format("csv").\
                option("header", True). \
                option("delimiter", "\t").\
                mode("overwrite"). \
                save(s3_out_path)
        except Exception as e:
            print(e)
            raise