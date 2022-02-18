
def process(spark, input_path, output_path, save_mode='append'):

    # read data
    #df = spark.read.parquet(input_path)
    df = spark.read.option("delimiter", "\t") \
        .option("header", "true")\
        .csv(input_path)

    # processing
    pass

    # output
    #df.write.parquet(output_path, save_mode=save_mode)
    df.write.format('json').save(output_path)



