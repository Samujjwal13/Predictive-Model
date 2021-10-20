"""
This module used for validating the scoring phase and to do sanity check before 
running validate for selfcure
"""
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.query_formatter.query_formatter import  Query
from selfcuremodel.database.database import  Database
from selfcuremodel.common_utility.common_utility import  CommonUtil
class ValidateTest():
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor for ValidateTest class and dependancy initialization
        """
        default_config = Config()
        query = Query(default_config)
        database = Database(default_config)
        common_util = CommonUtil(default_config)
        self.config = default_config
        self.query = query
        self.database = database
        self.common_util = common_util
    @logger
    def run_job(self):
        """
        This function calls sequence of other module functions for completing ValidateTest phase
        """
        try:
            info("status update for set_no_history_flag")
            self.set_no_history_flag()
            info("status update for set_rescore_record_r_flag")
            self.set_rescore_record_r_flag()
            info("validating model input open invoice data avaiaility")
            test_valiate_query = self.query.get_test_valiate_query()
            self.database.validate_testing_data(test_valiate_query)
            info("validating customer history data avaiaility")
            customer_history_data_validate_query = self.query.get_customer_history_data_validate_query()
            self.database.validate_customer_history_data_availability(customer_history_data_validate_query)
            info("validating model file avaiability")
            self.common_util.validate_model_avaiable_for_scoring()
        except Exception as err:
            error('Exception Occured while running validateTestJob :' + str(err))
            raise Exception('Exception Occured while running validateTestJob : {}'.format(str(err)))
    @logger
    def set_rescore_record_r_flag(self):
        """
        python module import
        """
        rescore_flag = self.config.get_rescoring()
        if rescore_flag == 'False':
            info('inside rescoring flag false for test validate')
            info('updating repeated invoice as R flag when rescoring is off')
            update_r_flag_query_when_resore_false = self.query.get_update_r_flag_query_when_resore_false()
            update_r_flag_when_resore_false_data = self.database.get_data_from_database(update_r_flag_query_when_resore_false)
            self.database.update_r_flag_when_invoice_repeated(update_r_flag_when_resore_false_data)
    @logger
    def set_no_history_flag(self):
        """
        python module import
        """
        no_invoice_history_query = self.query.get_no_invoice_history_query()
        no_invoice_history_data = self.database.get_data_from_database(no_invoice_history_query)
        self.database.update_no_history_flag_as_n(no_invoice_history_data)
        
