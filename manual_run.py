"""
This is the main module of the selcure model.This module holds main entry point for rest of the module 
"""
import argparse
from selfcuremodel.selfcure import selfcure_main

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description='Python datapaipeline ArgumentParser')
    PARSER.add_argument('--job_Name', type=str, required=True, dest='job_Name', help="job_Name")
    ARGS = PARSER.parse_args()
    selfcure_main(ARGS.job_Name)
