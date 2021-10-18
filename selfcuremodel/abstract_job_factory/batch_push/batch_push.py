"""
This module initializing the stored procedure from dvprime darabase to tranfer the predicted data to ILP process
"""
from airflow.hooks.oracle_hook import OracleHook
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.entity.parser.config import  Config
class BatchPush():
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor for BatchPush class and dependancy initialization
        """
        default_config = Config()
        self.config = default_config
    @logger
    def run_job(self):
        """
        This function calls sequence of other module functions for completing BatchPush phase
        """
        try:
            database = OracleHook(oracle_conn_id=self.config.get_connection())
            conn = database.get_conn()
            cursor = conn.cursor()
            return_val = cursor.callproc('MOGCARS.SPR_GCARS_UPDATE_STAGING')
            info(return_val)
            cursor.close()
        except Exception as err:
            error('Exception Occured while running batchPush job :' + str(err))
            raise Exception('Exception Occured while running batchPush job : {}'.format(str(err)))
