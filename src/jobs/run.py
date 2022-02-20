import src.jobs.transform as transform


def run(parameters):
    print("***Running***")
    jobs = {
        'job_transform': transform.process
    }
    job_name = parameters['job_name']
    process_function = jobs[job_name]

    process_function(
        input_path=parameters['input_path']
    )
