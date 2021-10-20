"""
This phase of workflow call closed invoicess and derives features for the model training 
this features then stored in the customer i=history table 
"""
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.query_formatter.query_formatter import  Query
from selfcuremodel.database.database import  Database
from selfcuremodel.common_utility.common_utility import  CommonUtil
class FeatureEngineering():
    """
    Class Definition
    """
    @logger
    def __init__(self):
        """
        Constructor for FeatureEngineering class and dependancy initialization
        """
        info('inside init of feature engineering')
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
        This function calls sequence of other module functions for completing FeatureEngineering phase
        """
        try:
            info("geting new debtors and inserting new records for new debtor Id into customer history table")
            new_debtor_number_query = self.query.get_new_debtor_number_query()
            new_debtor_number_df = self.database.get_data_from_database(new_debtor_number_query)
            self.database.insert_new_debtors_customer_history(new_debtor_number_df)
            info("geting and processing invoice data for various features")
            training_data_query = self.query.get_training_data_query()
            invoice_data = self.database.get_data_from_database(training_data_query)
            self.common_util.dataframe_empty_check(invoice_data)
            invoice_data = self.common_util.filter_data(invoice_data)
            invoice_data = self.common_util.parse_date(invoice_data)
            invoice_data = self.common_util.create_self_cure_label(invoice_data)
            self.common_util.dataframe_empty_check(invoice_data)
            invoice_data = self.common_util.closed_invoice_features(invoice_data)
            self.common_util.dataframe_empty_check(invoice_data)
            invoice_data = self.common_util.window_features(invoice_data)
            self.common_util.dataframe_empty_check(invoice_data)
            info("geting and processing touch data for various features")
            touch_data_query = self.query.get_touch_data_query()
            input_touch_data = self.database.get_data_from_database(touch_data_query)
            self.common_util.dataframe_empty_check(input_touch_data)
            invoice_touch_features_df = self.common_util.invoice_touch_features(input_touch_data)
            info("merging invoice data & touch features")
            invoice_data = self.common_util.merge_data(invoice_data, invoice_touch_features_df, "INVOICE_ID")
            self.common_util.dataframe_empty_check(input_touch_data)
            info("geting and processing customer level data for various features")
            invoice_data = self.common_util.customer_level_touch_features(invoice_data)
            self.common_util.dataframe_empty_check(invoice_data)
            info("inserting customer history data into table")
            #customer_history_insert_data_query=self.query.get_customer_history_insert_data_query()
            self.database.insert_customer_history(invoice_data)
        except Exception as err:
            error('Exception Occured while running featureEngineeringJob :' + str(err))
            raise Exception('Exception Occured while running featureEngineeringJob : {}'.format(str(err)))
            