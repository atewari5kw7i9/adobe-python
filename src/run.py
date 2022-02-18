from jobs import \
    transform,\
    validate
from utils.logger_utils import Log4j
from utils.spark_utils import create_spark_session


jobs = {
    'job_transform': transform.process,
    'job_validate': validate.process
}


def run(parameters):
    spark_config = parameters['spark_config']
    spark = create_spark_session(spark_config=spark_config)
    job_name = parameters['job_name']
    process_function = jobs[job_name]

    logger = Log4j(spark)

    for parameter, value in parameters.items():
        logger.info('Param {param}: {value}'.format(param=parameter, value=value))

    process_function(
        spark=spark,
        input_path=parameters['input_path'],
        output_path=parameters['output_path'],
        save_mode='append'
    )
