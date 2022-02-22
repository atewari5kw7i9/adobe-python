import ast
import sys
from src.jobs.run import run


if __name__ == '__main__':
    '''
    This is entry script for EMR job. run method of run module is being called through the EMR job
    using this call Sample parameters are
    "{'spark_config': {'--executor-memory': '1G', '--driver-memory': '1G'},'job_name': 'job_transform','input_path': 'src/jobs/data/adobe-data.tsv','output_path': 'src/jobs/data/logs-adobe-outboud'}" 
    '''
    print("Main")
    str_parameters = sys.argv[1]
    parameters = ast.literal_eval(str_parameters)
    run(parameters)