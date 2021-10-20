"""
This is the main module of the selcure model.This module holds main entry point for rest of the module 
"""
import argparse
from selfcuremodel.abstract_job_factory.abstract_job_factory import AbstractJob

def selfcure_main(job_name):
    """
    This is main function for rest of the module and it takes one argumnets as input usuallay this parameter
    shud be the name of the job thta handled / defines
    in abstract hjob factory class
    """
    job_module = AbstractJob.get_job_class(job_name)
    job_module.run_job()

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Python datapaipeline ArgumentParser')
    PARSER.add_argument('--job_Name', type=str, required=True, dest='job_Name', help="job_Name")
    ARGS = PARSER.parse_args()
    selfcure_main(ARGS.job_Name)
