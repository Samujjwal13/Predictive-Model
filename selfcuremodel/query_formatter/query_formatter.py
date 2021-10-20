"""
This module contains all the query which are used in the database module .
all this query constructed dynamically based on the paramaters configured in default config file
"""
from selfcuremodel.logging_wrapper.logger import logger, info
class Query:
    """
        Class Definition
    """
    def __init__(self, config):
        """
        Query Class Constructor
        """
        self.config = config
    @logger
    def get_new_debtor_number_query(self):
        """
        This function construct query for fetching new debtor number from training 
        dynamically
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        data_retention_period = self.config.get_retionperiod()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        # pylint: disable=line-too-long
        new_debtor_number_query = "SELECT DISTINCT DEBTOR_NUMBER FROM MOGCARS.MODEL_TRAINING where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND DOCUMENT_STATUS='CLOSED' AND CLIENT_DOCUMENT_DATE < CLOSE_DATE AND (EXTRACT(YEAR FROM CLIENT_DOCUMENT_DATE) >= EXTRACT(YEAR FROM (SYSDATE - interval '"+data_retention_period+"' year))) AND DEBTOR_NUMBER NOT IN (SELECT DEBTOR_NUMBER FROM MOGCARS.MODEL_CUSTOMER_HISTORY)"
        info(new_debtor_number_query)
        return new_debtor_number_query
    @logger
    def get_train_valiate_query(self):
        """
        This function construct query dynamincally for 
        fetching number of count in model training table
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        data_retention_period = self.config.get_retionperiod()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        # pylint: disable=line-too-long
        train_valiate_query = "SELECT count(*) as count FROM MOGCARS.MODEL_TRAINING  where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND DOCUMENT_STATUS='CLOSED' AND CLIENT_DOCUMENT_DATE < CLOSE_DATE AND (EXTRACT(YEAR FROM CLIENT_DOCUMENT_DATE) >= EXTRACT(YEAR FROM (SYSDATE - interval '"+data_retention_period+"' year)))"
        info(train_valiate_query)
        return train_valiate_query
    @logger
    def get_touch_validate_query(self):
        """
        This function construct query for 
        fetching count from  MODEL_TRAINING_TOUCH_DATA
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        touch_data_query = "SELECT count(*) as count FROM MOGCARS.MODEL_TRAINING_TOUCH_DATA"
        info(touch_data_query)
        return touch_data_query
    @logger
    def get_training_data_query(self):
        """
        This function construct query dynamincally for 
        fetching all data from model training based o paramteres 
        configured in 
        default config file
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        data_retention_period = self.config.get_retionperiod()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        training_data_query = "SELECT * FROM MOGCARS.MODEL_TRAINING  where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND DOCUMENT_STATUS='CLOSED' AND CLIENT_DOCUMENT_DATE < CLOSE_DATE AND (EXTRACT(YEAR FROM CLIENT_DOCUMENT_DATE) >= EXTRACT(YEAR FROM (SYSDATE - interval '"+data_retention_period+"' year))) "
        info(training_data_query)
        return training_data_query
    @logger
    def get_touch_data_query(self):
        """
        This function construct query for 
        fetching INVOICE_ID,SP_DATE from  MODEL_TRAINING_TOUCH_DATA
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        touch_data_query = "SELECT INVOICE_ID,SP_DATE FROM MOGCARS.MODEL_TRAINING_TOUCH_DATA"
        info(touch_data_query)
        return touch_data_query
    @staticmethod
    @logger
    def get_customer_history_data_query():
        """
        This function construct query for 
        fetching all customer history data
        """
        customer_history_data_query = "SELECT avg_dpd,average_first_touch_ageing,avg_contact_duration,avg_touch_duration_by_client,percentage_past_due_invoices,"+"perc_of_disputed_invoices,customer_wadco,number_of_payment_term,debtor_number  FROM MOGCARS.MODEL_CUSTOMER_HISTORY"
        info(customer_history_data_query)
        return customer_history_data_query
    @logger
    def get_test_valiate_query(self):
        """
        This function construct query dynamincally for 
        fetching number of count in model input  table
        for open invoice
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        test_valiate_query = "SELECT count(*) as count FROM MOGCARS.MODEL_INPUT where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND MODEL_STATUS IS NULL AND DOCUMENT_STATUS='OPEN'"
        info(test_valiate_query)
        return test_valiate_query
    @staticmethod
    @logger
    def get_customer_history_data_validate_query():
        """
        query for checking  count for validating customer history
        """
        customer_history_data_validate_query = "SELECT count(*) as count FROM MOGCARS.MODEL_CUSTOMER_HISTORY"
        info(customer_history_data_validate_query)
        return customer_history_data_validate_query
    @logger
    def get_testing_data_query(self):
        """
        constructs query to fetch data from model input for coring purpose
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        testing_data_query = "SELECT INVOICE_ID,DEBTOR_NUMBER,DUE_DATE FROM MOGCARS.MODEL_INPUT where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND MODEL_STATUS IS NULL AND DOCUMENT_STATUS='OPEN'"
        info(testing_data_query)
        return testing_data_query
    @logger
    def get_update_r_flag_query_when_resore_false(self):
        """
        this function constructs query for fetching invoice id which are not 
        present in training phase from customer history to
        update N flag
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        update_r_flag_query_when_resore_false = "SELECT DISTINCT INVOICE_ID FROM MOGCARS.MODEL_OUTPUT WHERE INVOICE_ID IN (SELECT INVOICE_ID FROM MOGCARS.MODEL_INPUT WHERE H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND DOCUMENT_STATUS='OPEN' AND MODEL_STATUS IS NULL )"
        info(update_r_flag_query_when_resore_false)
        return update_r_flag_query_when_resore_false
    @logger
    def get_automatic_re_score_dataset_query(self):
        """
        This function construct query for fetching previous days open invoice 
        for automatic rescoring functionality
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        automatic_re_score_dataset_query = "SELECT INVOICE_ID,DEBTOR_NUMBER,DUE_DATE FROM  MOGCARS.MODEL_INPUT WHERE INVOICE_ID NOT IN (SELECT INVOICE_ID FROM  MOGCARS.MODEL_TRAINING WHERE DOCUMENT_STATUS ='CLOSED' AND INVOICE_ID IS NOT NULL AND CLIENT_DOCUMENT_DATE < CLOSE_DATE AND H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+")) AND DOCUMENT_STATUS ='OPEN' AND MODEL_STATUS ='P' AND H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+")"
        info(automatic_re_score_dataset_query)
        return automatic_re_score_dataset_query
    @logger
    def get_no_invoice_history_query(self):
        """
        This function construct query to fetch debtor nuber from model input which are not 
        present in model input
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        no_invoice_history_query = "SELECT DEBTOR_NUMBER FROM  MOGCARS.MODEL_INPUT WHERE H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND MODEL_STATUS IS NULL AND DOCUMENT_STATUS='OPEN' AND DEBTOR_NUMBER NOT IN (SELECT DEBTOR_NUMBER FROM MOGCARS.MODEL_CUSTOMER_HISTORY)"
        info(no_invoice_history_query)
        return no_invoice_history_query
    @logger
    def get_actual_bin_and_score_query(self):
        """
        This function construct query to fetch debtor nuber from model input which are not 
        present in model input
        """
        actual_bin_and_score_query = "SELECT mo.invoice_id as invoice_id,mt.due_date,mt.close_date,ROUND((mt.close_date-mt.due_date),2) AS DPD FROM  MOGCARS.model_output mo JOIN  MOGCARS.model_training mt ON mt.invoice_id = mo.invoice_id WHERE  mt.document_status ='CLOSED' AND  mo.actual_score is NULL ORDER BY mt.close_date, mt.invoice_id desc"
        info(actual_bin_and_score_query)
        return actual_bin_and_score_query
    
    @staticmethod
    @logger
    def copy_all_customer_history_data_query():
        """
        This function construct query for 
        fetching all customer history data
        """
        customer_history_data_query = "SELECT * FROM MOGCARS.MODEL_CUSTOMER_HISTORY"
        info(customer_history_data_query)
        return customer_history_data_query
    @logger
    def get_training_max_min_dates(self):
        """
        This function construct query dynamincally for 
        fetching number of count in model training table
        """
        h3_business_id = self.config.get_h3_business_id()
        doc_type_description = self.config.get_doc_type_description()
        data_retention_period = self.config.get_retionperiod()
        doc_type_description = doc_type_description.split(",")
        h3_business_id = h3_business_id.split(",")
        training_max_min_dates = "SELECT MAX(CLIENT_DOCUMENT_DATE) as max ,MIN(CLIENT_DOCUMENT_DATE) as min FROM MOGCARS.MODEL_TRAINING  where H3_BUSINESS_ID IN ("+','.join("'{0}'".format(elem) for elem in h3_business_id)+") AND DOC_TYPE_DESCRIPTION IN ("+','.join("'{0}'".format(elem) for elem in doc_type_description)+") AND DOCUMENT_STATUS='CLOSED' AND CLIENT_DOCUMENT_DATE < CLOSE_DATE AND (EXTRACT(YEAR FROM CLIENT_DOCUMENT_DATE) >= EXTRACT(YEAR FROM (SYSDATE - interval '"+data_retention_period+"' year)))"
        info(training_max_min_dates)
        return training_max_min_dates
    @logger
    def get_latest_scoring_summary_data_query(self):
        """
        This function construct query dynamincally for 
        fetching number of count in model training table
        """
        training_max_min_dates = "SELECT COUNT(*) as count,TRUNC(LAST_UPDATED_DATETIME) as SCORE_DATE,MODEL_STATUS FROM MOGCARS.MODEL_INPUT WHERE TRUNC(LAST_UPDATED_DATETIME) = (SELECT max(TRUNC(LAST_UPDATED_DATETIME)) from MOGCARS.MODEL_INPUT) GROUP BY TRUNC(LAST_UPDATED_DATETIME),MODEL_STATUS"
        info(training_max_min_dates)
        return training_max_min_dates
    
