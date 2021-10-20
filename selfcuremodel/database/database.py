"""
This module handle all database related operations 
"""
import warnings
from datetime import datetime
import pandas as pd
from airflow.hooks.oracle_hook import OracleHook
from selfcuremodel.logging_wrapper.logger import logger, info, error
warnings.filterwarnings('ignore')
class Database:
    """
    Class Definition
    """
    def __init__(self, config):
        """
        Constructor and dependancy intializaiton
        """
        self.config = config
    @logger
    def validate_training_data(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        flag = pd.read_sql(query, conn)
        if flag['COUNT'].iloc[0] == 0:
            raise ValueError('Validation Failed : No Training Data found !!')
        else:
            info('Training Validation Passed !!')
        conn.close()
    @logger
    def validate_touch_data(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        flag = pd.read_sql(query, conn)
        if flag['COUNT'].iloc[0] == 0:
            raise ValueError('Validation Failed : No touch Data !!')
        else:
            info('touch data Validation Passed !!')
        conn.close()
    @logger
    def validate_testing_data(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        flag = pd.read_sql(query, conn)
        if flag['COUNT'].iloc[0] == 0:
            raise ValueError('Scoring Validation Failed : No Data for Scoring Found !!')
        else:
            info('Scoring Validation Passed !!')
        conn.close()
    @logger
    def validate_customer_history_data_availability(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        flag = pd.read_sql(query, conn)
        if flag['COUNT'].iloc[0] == 0:
            raise ValueError('Customer History Validation Failed : No Customer History Data')
        else:
            info('Customer History Data avaiablity validation Passed !!')
        conn.close()
    @logger
    def get_data_from_database(self, query):
        """
        this function loads data from database by passing the query and connection
        objects
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        invoice_data_df = pd.read_sql(query, conn)
        return invoice_data_df
    @logger
    def insert_new_debtors_customer_history(self, new_debtor_number_df):
        """
        This function inserts all new customer debtor_number into the data base tabe 
        customer history
        """
        info(new_debtor_number_df.shape)
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        new_debtor_number_df = new_debtor_number_df[['DEBTOR_NUMBER']]
        try:
            cursor.prepare('INSERT INTO MOGCARS.MODEL_CUSTOMER_HISTORY(DEBTOR_NUMBER) values (:1)')
            cursor.executemany(None, new_debtor_number_df.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
        
    @logger
    def insert_customer_history(self, customer_history):
        """
        This function updates new customer agrregated history into the data base tabe 
        customer history
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(customer_history.shape)
        customer_history["DEBTOR_NUMBER"] = customer_history["DEBTOR_NUMBER"].astype(int)
        customer_history = customer_history[['avg_dpd', 'median_dpd', 'avg_ar', 'median_ar', 'total_ar_value_by_client', 'number_invoices_above_20k', 'customer_wadco', 'invoice_frequency', 'disputed_invoices_frequency', 'perc_of_disputed_invoices', 'disputed_invoices_ar_value', 'perc_disputed_invoice_value', 'total_past_due_invoices', 'percentage_past_due_invoices', 'late_payment_invoice_ar_value', 'number_of_payment_term', 'customer_wapt', 'number_bill_to_country_codes', 'last_5_invoice_ageing', 'customer_wadc', 'noi_30_days', 'noi_60_days', 'noi_90_days', 'noi_120_days', 'tpi_30_days', 'tpi_60_days', 'tpi_90_days', 'tpi_120_days', 'ropi_last_30_days', 'ropi_last_60_days', 'ropi_last_90_days', 'ropi_last_120_days', 'tav_last_30_days', 'tav_last_60_days', 'tav_last_90_days', 'tav_last_120_days', 'tpav_last_30_days', 'tpav_last_60_days', 'tpav_last_90_days', 'tpav_last_120_days', 'average_touch_duration', 'touch_frequency', 'avg_touch_duration_by_client', 'average_first_touch_ageing', 'avg_contact_duration', 'total_touches', 'avg_touches_per_dollar', 'DEBTOR_NUMBER']]
        info(customer_history.columns)
        #customer_history.round(2)
        try:
            statement = "UPDATE MOGCARS.MODEL_CUSTOMER_HISTORY set avg_dpd = :1, median_dpd = :2, avg_ar = :3, median_ar = :4,total_ar_value_by_client = :5,number_invoices_above_20k = :6,customer_wadco = :7, invoice_frequency = :8, disputed_invoices_frequency = :9,        perc_of_disputed_invoices = :10, disputed_invoices_ar_value = :11,perc_disputed_invoice_value = :12, total_past_due_invoices = :13, percentage_past_due_invoices = :14, late_payment_invoice_ar_value = :15,number_of_payment_term = :16  ,customer_wapt = :17, number_bill_to_country_codes = :18, last_5_invoice_ageing = :19, customer_wadc = :20,noi_30_days = :21 ,noi_60_days = :22, noi_90_days = :23 , noi_120_days = :24, tpi_30_days = :25, tpi_60_days = :26,tpi_90_days = :27, tpi_120_days = :28, ropi_last_30_days = :29, ropi_last_60_days = :30 , ropi_last_90_days = :31,ropi_last_120_days = :32, tav_last_30_days = :33 , tav_last_60_days = :34, tav_last_90_days = :35, tav_last_120_days = :36 ,tpav_last_30_days = :37, tpav_last_60_days = :38, tpav_last_90_days = :39 ,tpav_last_120_days = :40 , average_touch_duration = :41, touch_frequency = :42,avg_touch_duration_by_client = :43,average_first_touch_ageing = :44, avg_contact_duration = :45, total_touches = :46 ,avg_touches_per_dollar = :47  WHERE DEBTOR_NUMBER = :48"
            cursor.prepare(statement)
            cursor.executemany(None, customer_history.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
    @logger
    def save_prediction_to_table(self, predicted_data, model_name):
        """
        this function saves final invoice score  to invoice output table after final scoring done
        """
        info(predicted_data.columns)
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        username = conn.username
        info(predicted_data.shape)
        predicted_data['propensity_to_pay'] = None
        predicted_data['actual_score'] = None
        predicted_data['curtimestamp'] = datetime.now()
        predicted_data['timestamp'] = datetime.now()
        predicted_data['userid'] = username
        predicted_data['category_bin'] = None
        predicted_data['actual_category_bin'] = None
        predicted_data['model_version'] = model_name
        predicted_data['propensity_to_pay'] = predicted_data['probability_of_being_on_time']
        predicted_data = predicted_data[['INVOICE_ID', 'prediction', 'propensity_to_pay', 'actual_score', 'curtimestamp', 'timestamp', 'userid', 'category_bin', 'actual_category_bin', 'model_version']]
        info(predicted_data.shape)
        try:
            cursor.prepare('INSERT INTO MOGCARS.MODEL_OUTPUT(invoice_id, predicted_score, propensity_to_pay, actual_score, score_date, timestamp, userid,category_bin, actual_category_bin, model_version) values (:1 , :2 , :3 , :4 , :5 , :6 , :7 , :8 , :9 , :10)')
            cursor.executemany(None, predicted_data.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))

    @logger
    def update_processed_flag_for_open_invoice(self, predicted_data):
        """
        this function marks all invoices as P flag in model input table which 
        scored durng the model run
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(predicted_data.shape)
        predicted_data = predicted_data[['INVOICE_ID']]
        info(predicted_data.shape)
        try:
            statement = "UPDATE MOGCARS.MODEL_INPUT set MODEL_STATUS = \'P\' WHERE INVOICE_ID = :1 AND MODEL_STATUS IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, predicted_data.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
    @logger
    def update_r_flag_when_invoice_repeated(self, predicted_data):
        """
        This function updates invoice status as R flag when re scoring is false 
        in model input table
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(predicted_data.shape)
        predicted_data = predicted_data[['INVOICE_ID']]
        info(predicted_data.shape)
        try:
            statement = "UPDATE MOGCARS.MODEL_INPUT set MODEL_STATUS = \'R\' WHERE INVOICE_ID = :1 AND MODEL_STATUS IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, predicted_data.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
    @logger
    def update_no_history_flag_as_n(self, input_data):
        """
        for the invoice if there is no history in customer history table then this records are marked as N flag 
        in model_input table
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(input_data.shape)
        input_data = input_data[['DEBTOR_NUMBER']]
        info(input_data.shape)
        try:
            statement = "UPDATE MOGCARS.MODEL_INPUT set MODEL_STATUS = \'N\' WHERE DEBTOR_NUMBER = :1 AND MODEL_STATUS IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, input_data.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
    @logger
    def update_actual_score(self, input_data):
        """
        for the invoice if there is no history in customer history table then this records are marked as N flag
        in model_input table
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(input_data.shape)
        input_data_late_pay = input_data[input_data['DPD'] > 0]
        input_data_on_time = input_data[input_data['DPD'] <= 0]
        input_data_late_pay = input_data_late_pay[['INVOICE_ID']]
        input_data_on_time = input_data_on_time[['INVOICE_ID']]
        info(input_data_late_pay.shape)
        info(input_data_on_time.shape)
        try:
            statement = "UPDATE MOGCARS.MODEL_OUTPUT set ACTUAL_SCORE = 0  WHERE INVOICE_ID = :1  AND ACTUAL_SCORE IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, input_data_on_time.to_numpy().tolist())
            statement = "UPDATE MOGCARS.MODEL_OUTPUT set ACTUAL_SCORE = 1  WHERE INVOICE_ID = :1  AND ACTUAL_SCORE IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, input_data_late_pay.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
            
    @logger
    def update_missing_feature_as_n_flag(self, input_data):
        """
        for the invoice if there is no history in customer history table then this records are marked as N flag 
        in model_input table
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        cursor = conn.cursor()
        info(input_data.shape)
        input_data = input_data[['INVOICE_ID']]
        info(input_data.shape)
        try:
            statement = "UPDATE MOGCARS.MODEL_INPUT set MODEL_STATUS = \'M\' WHERE INVOICE_ID = :1 AND MODEL_STATUS IS NULL"
            cursor.prepare(statement)
            cursor.executemany(None, input_data.to_numpy().tolist())
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as errorstr:
            error(str(errorstr))
    @logger
    def get_training_data_count(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        flag = pd.read_sql(query, conn)
        count = ""
        if flag['COUNT'].iloc[0] == 0:
            count = 0
        else:
            count = flag['COUNT'].iloc[0]
        conn.close()
        return count
    @logger
    def get_training_max_min_dates_for_email_report(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        data = pd.read_sql(query, conn)
        max_date = data['MAX'][0]
        min_date  = data['MIN'][0]
        conn.close()
        return max_date, min_date
    @logger
    def get_latest_scoring_summary_data(self, query):
        """
        This function checks count of dataframe and returns validation pass or faill
        by returing exception
        """
        database = OracleHook(oracle_conn_id=self.config.get_connection())
        conn = database.get_conn()
        data = pd.read_sql(query, conn)
        score_date = ""
        p_count = 0
        m_count = 0 
        n_count = 0 
        r_count = 0
        null_count = 0
        total_invoice = 0
        for index , row in data.iterrows():
            if row['MODEL_STATUS'] == 'P':
                p_count = p_count+row['COUNT']
                score_date = row['SCORE_DATE']
                total_invoice=total_invoice+row['COUNT']
            elif row['MODEL_STATUS'] == 'R':
                r_count = r_count+row['COUNT']
                score_date = row['SCORE_DATE']
                total_invoice=total_invoice+row['COUNT']
            elif row['MODEL_STATUS'] == 'M':
                m_count = m_count+row['COUNT']
                score_date = row['SCORE_DATE']
                total_invoice=total_invoice+row['COUNT']
            elif row['MODEL_STATUS'] == 'N':
                n_count = n_count+row['COUNT']
                score_date = row['SCORE_DATE']
                total_invoice=total_invoice+row['COUNT']
            else:
                null_count = null_count+row['COUNT']
                score_date = row['SCORE_DATE']
                total_invoice=total_invoice+row['COUNT']
        conn.close()
        df = pd.DataFrame({'SCORE_DATE': score_date, 'P': p_count, 'R': r_count, 'M': m_count, 'N': n_count, 'NULL': null_count}, index=[0])
        return score_date, p_count, r_count, m_count, n_count, null_count, total_invoice
