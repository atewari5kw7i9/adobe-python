import src.jobs.transform as transform

class Settings:
    spark_config = {'--executor-memory': '1G',  '--driver-memory': '2G'}
    output_path = "s3://logs-adobe-outbound/data/raw"
    jobs = {
        'job_transform': transform.process
    }
    app_name = 'Adobe-Data-Product'
    columns_select = ["event_list", "product_list", "page_url", "referrer"]
    search_str = "(p=|q=|k=)"
    terminate_str = "&"
    domain_str_begin = "//"
    domain_str_end = "/"
