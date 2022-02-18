
from src.jobs.utils.logger_utils import Log4j
from pyspark.sql import SparkSession
import src.jobs.transform as transform

jobs = {
    'job_transform': transform.process
}


def run(parameters):
    print("***Running***")
    spark_config = parameters['spark_config']
    spark_builder = SparkSession.builder.appName("adobe-logs")
    for k,v in spark_config.items():
        spark_builder.config(k, v)
    spark = spark_builder.getOrCreate()
    job_name = parameters['job_name']
    process_function = jobs[job_name]

    #logger = Log4j(spark)

    # for parameter, value in parameters.items():
    #     logger.info('Param {param}: {value}'.format(param=parameter, value=value))

    process_function(
        spark=spark,
        input_path=parameters['input_path'],
        output_path=parameters['output_path'],
        save_mode='append'
    )
