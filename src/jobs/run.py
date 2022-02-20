from pyspark.sql import SparkSession
import src.jobs.transform as transform
from src.jobs.utils.Settings import Settings

jobs = Settings.jobs


def run(parameters):
    # print("***Running***")
    # spark_config = parameters['spark_config']
    # spark_builder = SparkSession.builder.appName("adobe-logs")
    # for k, v in spark_config.items():
    #     spark_builder.config(k, v)
    # spark = spark_builder.getOrCreate()
    # job_name = parameters['job_name']
    # process_function = jobs[job_name]
    #
    # process_function(
    #     spark=spark,
    #     input_path=parameters['input_path'],
    #     output_path=parameters['output_path']
    # )

    print("***Running***")
    job_name = parameters['job_name']
    process_function = jobs[job_name]

    process_function(
        input_path=parameters['input_path']
    )
