"""
This module uses customer history data which produced in previous feature 
engineering phase and producess model object in picle format
"""
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.query_formatter.query_formatter import  Query
from selfcuremodel.database.database import  Database
from selfcuremodel.common_utility.common_utility import  CommonUtil
class ModelTrainiing():
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor for ModelTrainiing class and dependancy initialization
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
        This function calls sequence of other module functions for completing ModelTrainiing phase
        """
        try:
            info("geting training data from training table")
            training_data_query = self.query.get_training_data_query()
            invoice_data = self.database.get_data_from_database(training_data_query)
            self.common_util.dataframe_empty_check(invoice_data)
            invoice_data = self.common_util.filter_data(invoice_data)
            invoice_data = self.common_util.parse_date(invoice_data)
            invoice_data = self.common_util.create_self_cure_label(invoice_data)
            self.common_util.dataframe_empty_check(invoice_data)
            info("geting and processing data from customer history")
            customer_history_data_query = self.query.get_customer_history_data_query()
            customer_history_data = self.database.get_data_from_database(customer_history_data_query)
            self.common_util.dataframe_empty_check(customer_history_data)
            info("merging training data and customer history data")
            customer_history_data["DEBTOR_NUMBER"] = customer_history_data["DEBTOR_NUMBER"].astype(int)
            #invoice_data["DEBTOR_NUMBER"] = invoice_data["DEBTOR_NUMBER"].astype(int)
            #customer_history_data = self.common_util.merge_data(customer_history_data, invoice_data, "DEBTOR_NUMBER")
            info("started training the model")
            #self.common_util.dataframe_empty_check(customer_history_data)
            #model_object = self.common_util.selfcure_modeling_creation(customer_history_data)
            #info("Saving the model to pickle file")
            #self.common_util.save_to_pickle(model_object)
        except Exception as err:
            error('Exception Occured while running batchPush job :' + str(err))
            raise Exception('Exception Occured while running batchPush job : {}'.format(str(err)))
            