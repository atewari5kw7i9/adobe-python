import src.jobs.transform as transform


def run(parameters):
    '''
    This method is used to call appropriate python script based on the job name being passed.
    '''
    print("***Running***")
    jobs = {
        'job_transform': transform.process
    }
    job_name = parameters['job_name']
    process_function = jobs[job_name]

    process_function(
        input_path=parameters['input_path']
    )
