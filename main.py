import ast
import sys
from src.jobs.run import run


if __name__ == '__main__':
    print("Main")
    str_parameters = sys.argv[1]
    parameters = ast.literal_eval(str_parameters)
    run(parameters)