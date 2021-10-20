"""
This module used to call model file produced in training work flow and predict the open invouce
"""
import pandas as pd
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.query_formatter.query_formatter import  Query
from selfcuremodel.database.database import  Database
from selfcuremodel.common_utility.common_utility import  CommonUtil
class Inferencing():
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor for Inferencing class and dependancy initialization
        """
        default_config = Config()
        query = Query(default_config)
        database = Database(default_config)
        common_util = CommonUtil(default_config, database)
        self.config = default_config
        self.query = query
        self.database = database
        self.common_util = common_util
    @logger
    def run_job(self):
        """
        This function calls sequence of other module functions for completing Inferencing phase
        """
        try:
            info("fetching testing invoice data from  get_data_after_rescoring_flag_check function")
            testing_data = self.get_data_after_rescoring_flag_check()
            self.common_util.dataframe_empty_check(testing_data)
            info("geting and processing data from customer history")
            customer_history_data_query = self.query.get_customer_history_data_query()
            customer_history_data = self.database.get_data_from_database(customer_history_data_query)
            self.common_util.dataframe_empty_check(testing_data)
            self.common_util.dataframe_empty_check(customer_history_data)
            model_name = self.common_util.load_last_model_name('SC_LR')
            info("inferencing started")
            predicted_data = self.common_util.get_selfcure_lr_prediction(testing_data, customer_history_data, 'SC_LR')
            info("storing prediction to the table")
            self.database.save_prediction_to_table(predicted_data, model_name)
            info("Appling P flag for all predcitited invoice")
            self.database.update_processed_flag_for_open_invoice(predicted_data)
        except Exception as err:
            error('Exception Occured while running inferencingJob job :' + str(err))
            raise Exception('Exception Occured while running inferencingJob job : {}'.format(str(err)))
    @logger
    def get_data_after_rescoring_flag_check(self):
        """
         python module import
        """
        try:
            rescore_flag = self.config.get_rescoring()
            testing_data_df = pd.DataFrame()
            if rescore_flag == 'False':
                info('inside rescoring flag false if condition')
                info("geting training data from training table")
                testing_data_query = self.query.get_testing_data_query()
                testing_data = self.database.get_data_from_database(testing_data_query)
                testing_data_df = testing_data
            else:
                info('inside rescoring flag true if condition')
                info("geting training data from input table")
                testing_data_query = self.query.get_testing_data_query()
                testing_data = self.database.get_data_from_database(testing_data_query)
                info("geting previous day open invoice that are still open")
                automatic_re_score_dataset_query = self.query.get_automatic_re_score_dataset_query()
                automatic_re_score_dataset_query_data = self.database.get_data_from_database(automatic_re_score_dataset_query)
                info(testing_data.shape)
                info(automatic_re_score_dataset_query_data.shape)
                testing_data_df = pd.concat([testing_data, automatic_re_score_dataset_query_data])
                del automatic_re_score_dataset_query_data
            return testing_data_df
        except Exception as err:
            error('Exception Occured while running inferencingJob job :' + str(err))
            raise Exception('Exception Occured while running inferencingJob job : {}'.format(str(err)))
