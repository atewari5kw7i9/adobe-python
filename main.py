import ast
import sys
from src.jobs.run import run


if __name__ == '__main__':
    str_parameters = sys.argv[1]
    parameters = ast.literal_eval(str_parameters)
    # str_parameters = {"spark_config": {"--executor-memory": "1G", "--driver-memory": "1G"},
    #  "job_name": "job_transform",
    #  "input_path": "src/jobs/data/adobe-data.tsv",
    #  "output_path": "src/jobs/data/logs-adobe-outboud"
    #  }
    parameters = str_parameters
    run(parameters)