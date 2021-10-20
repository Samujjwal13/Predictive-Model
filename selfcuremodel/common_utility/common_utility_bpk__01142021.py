"""
This module contains all the common functionality that will be used in the future and rest of the module 
repeatedly
"""
import os
from datetime import datetime
import pickle
import gc
import pandas as pd
import numpy as np
# Modelling and Feature Engineering
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import MinMaxScaler
from selfcuremodel.logging_wrapper.logger import logger, info, error
from selfcuremodel.constants.constants import  SELECTED_FEATURES, CUSTOMER_FEATURES, NUMERICAL_FEATURES
from selfcuremodel.constants.constants import MODEL_ORDER, TRAINING_DROP_COLUMNS, OUTPUT_COLUMNS, CUSTOMER_MODEL_FEATURES
class CommonUtil:
    """
    Class definition
    """
    @logger
    def __init__(self, config,database=None):
        """
        constructor and dependadcy initialization
        """
        self.config = config
        self.database = database
    @staticmethod
    @logger
    def filter_data(data):
        """
        python module import
        """
        data['DOC_TYPE_DESCRIPTION'] = data['DOC_TYPE_DESCRIPTION'].str.lower()
        data['DOC_TYPE_DESCRIPTION'] = data['DOC_TYPE_DESCRIPTION'].str.strip()
        data = data.loc[data['DOC_TYPE_DESCRIPTION'] == 'invoice']
        data = data.loc[data['CONVERTED_ORIGINAL_AMOUNT'] > 0]
        data = data.sort_values(by="CLOSE_DATE")
        data.drop_duplicates(subset="INVOICE_ID", inplace=True)
        return data
    @staticmethod
    @logger
    def parse_date(data):
        """
        Function parses date string to date format
        """
        date_columns = ["CLOSE_DATE", "DUE_DATE", "CLIENT_DOCUMENT_DATE"]
        for column in date_columns:
            data[column] = pd.to_datetime(data[column])
        return data
    @staticmethod
    @logger
    def dataframe_empty_check(data):
        """
        This function checks is usefull to check and validate before passing the data to other function 
        or piece of code whether data is avaiable or not if not 
        it throws the meaning full way so the the program exit smoothly
        """
        if data.empty == True:
            raise ValueError('dataframe_empty_check validation failed : No Data at this point for furthere processing')
        else:
            info('dataframe_empty_check validation Passed')
    @staticmethod
    @logger
    def create_self_cure_label(data):
        """
        Creates label for by derving fields for selfcure
        """
        data["dpd"] = data["CLOSE_DATE"]-data["DUE_DATE"]
        data["dpd"] = data["dpd"].dt.days
        data["is_past_due"] = data["dpd"].apply(lambda x: 1 if x > 0 else 0)
        data["dpd"] = np.where(data["dpd"] < 0, 0, data["dpd"])
        return data
    @staticmethod
    @logger
    def merge_data(left, right, key):
        """
        Common merge function for combining dataframe
        """
        merged_data = pd.merge(left, right, on=key, how="left")
        return merged_data
    @logger
    def closed_invoice_features(self, data):
        """
        Derving closed reports invoice features for feature engineering 
        """
        info("working for dpd and ar related features")
        data['avg_dpd'] = data.groupby('DEBTOR_NUMBER')['dpd'].transform('mean') # used in self cure model
        data['median_dpd'] = data.groupby('DEBTOR_NUMBER')['dpd'].transform('median')
        data['avg_ar'] = data.groupby('DEBTOR_NUMBER')['CONVERTED_ORIGINAL_AMOUNT'].transform('mean')
        data['median_ar'] = data.groupby('DEBTOR_NUMBER')['CONVERTED_ORIGINAL_AMOUNT'].transform('median')
        data["total_ar_value_by_client"] = data.groupby('DEBTOR_NUMBER')['CONVERTED_ORIGINAL_AMOUNT'].transform('sum')
        data["total_ar_value"] = sum(data['CONVERTED_ORIGINAL_AMOUNT'])
        data["ar_above_20k"] = data["CONVERTED_ORIGINAL_AMOUNT"].apply(lambda x: 1 if x > 20000 else 0)
        data["number_invoices_above_20k"] = data.groupby("DEBTOR_NUMBER")["ar_above_20k"].transform("sum")
        data["wadco"] = data["dpd"]*data["CONVERTED_ORIGINAL_AMOUNT"]/data["total_ar_value"] # weighted_avg_days_to_collect_overdue
        data["customer_wadco"] = data.groupby("DEBTOR_NUMBER")["wadco"].transform('sum') # used in self cure model
        info("disputed related features")
        data["DISPUTE_STATUS"] = data["DISPUTE_STATUS"].str.lower()
        data["DISPUTE_STATUS"] = data["DISPUTE_STATUS"].replace("closed", "disputed")
        data["DISPUTE_STATUS"] = data["DISPUTE_STATUS"].fillna("not_disputed")
        dispute_dist = pd.DataFrame(data.loc[data["DISPUTE_STATUS"] == "disputed"].groupby("DEBTOR_NUMBER")["DISPUTE_STATUS"].value_counts().rename("disputed_invoices_frequency").reset_index())
        dispute_dist = dispute_dist[["DEBTOR_NUMBER", "disputed_invoices_frequency"]]
        data["invoice_frequency"] = data.groupby("DEBTOR_NUMBER")["INVOICE_ID"].transform("nunique")
        data = self.merge_data(data, dispute_dist, 'DEBTOR_NUMBER')
        del dispute_dist
        gc.collect()
        data["disputed_invoices_frequency"] = data["disputed_invoices_frequency"].fillna(0)
        data["perc_of_disputed_invoices"] = data["disputed_invoices_frequency"]/data["invoice_frequency"] # used in the self cure model
        disputed_invoices_by_ar = pd.DataFrame(data.loc[data["DISPUTE_STATUS"].isin(["disputed"])].groupby(["DEBTOR_NUMBER"])["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("disputed_invoices_ar_value").reset_index())
        data = self.merge_data(data, disputed_invoices_by_ar, 'DEBTOR_NUMBER')
        del disputed_invoices_by_ar
        gc.collect()
        data["disputed_invoices_ar_value"] = data["disputed_invoices_ar_value"].fillna(0)
        data["perc_disputed_invoice_value"] = data["disputed_invoices_ar_value"]/data["total_ar_value"]
        info("past due related features")
        data["total_past_due_invoices"] = data.groupby(["DEBTOR_NUMBER"])["is_past_due"].transform("sum")
        data["percentage_past_due_invoices"] = data["total_past_due_invoices"]/data["invoice_frequency"]
        data["percentage_past_due_invoices"] = data["percentage_past_due_invoices"]*100 # used in self cure model
        late_payment_invoice_ar_value = pd.DataFrame(data.loc[data["is_past_due"] == 1].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("late_payment_invoice_ar_value").reset_index())
        data = self.merge_data(data, late_payment_invoice_ar_value, 'DEBTOR_NUMBER')
        del late_payment_invoice_ar_value
        gc.collect()
        data["late_payment_invoice_ar_value"] = data["late_payment_invoice_ar_value"].fillna(0)
        info("payment related")
        data["payment_term"] = data["DUE_DATE"] - data["CLIENT_DOCUMENT_DATE"]
        data["payment_term"] = data["payment_term"].dt.days
        data["number_of_payment_term"] = data.groupby("DEBTOR_NUMBER")["payment_term"].transform("nunique") # used in self cure model
        data["wapt"] = data["payment_term"]*data["CONVERTED_ORIGINAL_AMOUNT"]/data["total_ar_value"] #weighted_avg_payment_term
        data["customer_wapt"] = data.groupby("DEBTOR_NUMBER")["wapt"].transform('sum')
        data["number_bill_to_country_codes"] = data.groupby("DEBTOR_NUMBER")["CLIENT_BILL_TO_COUNTRY_CODE"].transform("nunique")
        client_by_last_5_invoices = data.sort_values(by="CLOSE_DATE", ascending=False).groupby("DEBTOR_NUMBER").head().groupby("DEBTOR_NUMBER")["dpd"].mean().rename("last_5_invoice_ageing").reset_index()
        data = self.merge_data(data, client_by_last_5_invoices, 'DEBTOR_NUMBER')
        del client_by_last_5_invoices
        gc.collect()
        data["days_to_collect"] = data["CLOSE_DATE"] - data["CLIENT_DOCUMENT_DATE"]
        data["days_to_collect"] = data["days_to_collect"].dt.days
        data["wadc"] = data["CONVERTED_ORIGINAL_AMOUNT"]*data["days_to_collect"]/data["total_ar_value"] #weighted_avg_days_to_collect
        data["customer_wadc"] = data.groupby("DEBTOR_NUMBER")["wadc"].transform('sum')
        return data
    @logger
    def window_features(self, data):
        """
        deriving window feature based on time freames and durations
        """
        data['latest_invoice_date'] = data.groupby('DEBTOR_NUMBER')['CLIENT_DOCUMENT_DATE'].transform('max')
        data['latest_invoice_date'] = pd.to_datetime(data['latest_invoice_date'])
        data['30'] = 30
        data['60'] = 60
        data['90'] = 90
        data['120'] = 120
        # Make it functional
        data['30_days_prior_to_latest_invoice'] = data['latest_invoice_date']+pd.to_timedelta(data['latest_invoice_date'].dt.day-data['30'], 'D')
        data['120_days_prior_to_latest_invoice'] = data['latest_invoice_date']+pd.to_timedelta(data['latest_invoice_date'].dt.day-data['120'], 'D')
        data['90_days_prior_to_latest_invoice'] = data['latest_invoice_date']+pd.to_timedelta(data['latest_invoice_date'].dt.day-data['90'], 'D')
        data['60_days_prior_to_latest_invoice'] = data['latest_invoice_date']+pd.to_timedelta(data['latest_invoice_date'].dt.day-data['60'], 'D')
        data['is_invoice_in_30_window'] = np.where((data['CLIENT_DOCUMENT_DATE'] >= data['30_days_prior_to_latest_invoice']) & (data['CLIENT_DOCUMENT_DATE'] <= data['latest_invoice_date']), 1, 0)
        data['is_invoice_in_120_window'] = np.where((data['CLIENT_DOCUMENT_DATE'] >= data['120_days_prior_to_latest_invoice']) & (data['CLIENT_DOCUMENT_DATE'] <= data['latest_invoice_date']), 1, 0)
        data['is_invoice_in_60_window'] = np.where((data['CLIENT_DOCUMENT_DATE'] >= data['60_days_prior_to_latest_invoice']) & (data['CLIENT_DOCUMENT_DATE'] <= data['latest_invoice_date']), 1, 0)
        data['is_invoice_in_90_window'] = np.where((data['CLIENT_DOCUMENT_DATE'] >= data['90_days_prior_to_latest_invoice']) & (data['CLIENT_DOCUMENT_DATE'] <= data['latest_invoice_date']), 1, 0)
        data['noi_30_days'] = data.groupby('DEBTOR_NUMBER')['is_invoice_in_30_window'].transform('sum') #number_of_invoices
        data['noi_60_days'] = data.groupby('DEBTOR_NUMBER')['is_invoice_in_60_window'].transform('sum')
        data['noi_90_days'] = data.groupby('DEBTOR_NUMBER')['is_invoice_in_90_window'].transform('sum')
        data['noi_120_days'] = data.groupby('DEBTOR_NUMBER')['is_invoice_in_120_window'].transform('sum')
        data['is_pastdue_in_30_days'] = np.where((data['dpd'] > 0) & (data['is_invoice_in_30_window'] == 1), 1, 0)
        data['is_pastdue_in_60_days'] = np.where((data['dpd'] > 0) & (data['is_invoice_in_60_window'] == 1), 1, 0)
        data['is_pastdue_in_90_days'] = np.where((data['dpd'] > 0) & (data['is_invoice_in_90_window'] == 1), 1, 0)
        data['is_pastdue_in_120_days'] = np.where((data['dpd'] > 0) & (data['is_invoice_in_120_window'] == 1), 1, 0)
        data['tpi_30_days'] = data.groupby('DEBTOR_NUMBER')['is_pastdue_in_30_days'].transform('sum') #total_pastdue_invoices
        data['tpi_60_days'] = data.groupby('DEBTOR_NUMBER')['is_pastdue_in_60_days'].transform('sum')
        data['tpi_90_days'] = data.groupby('DEBTOR_NUMBER')['is_pastdue_in_90_days'].transform('sum')
        data['tpi_120_days'] = data.groupby('DEBTOR_NUMBER')['is_pastdue_in_120_days'].transform('sum')
        data['ropi_last_30_days'] = data['tpi_30_days']/data['noi_30_days']
        data['ropi_last_60_days'] = data['tpi_60_days']/data['noi_60_days']
        data['ropi_last_90_days'] = data['tpi_90_days']/data['noi_90_days']
        data['ropi_last_120_days'] = data['tpi_120_days']/data['noi_120_days']
        total_ar_value_in_last_30_days = pd.DataFrame(data.loc[data["is_invoice_in_30_window"] == 1].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tav_last_30_days").reset_index())
        data = self.merge_data(data, total_ar_value_in_last_30_days, 'DEBTOR_NUMBER')
        del total_ar_value_in_last_30_days
        gc.collect()
        total_ar_value_in_last_60_days = pd.DataFrame(data.loc[data["is_invoice_in_60_window"] == 1].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tav_last_60_days").reset_index())
        data = self.merge_data(data, total_ar_value_in_last_60_days, 'DEBTOR_NUMBER')
        del total_ar_value_in_last_60_days
        gc.collect()
        total_ar_value_in_last_90_days = pd.DataFrame(data.loc[data["is_invoice_in_90_window"] == 1].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tav_last_90_days").reset_index())
        data = self.merge_data(data, total_ar_value_in_last_90_days, 'DEBTOR_NUMBER')
        del total_ar_value_in_last_90_days
        gc.collect()
        total_ar_value_in_last_120_days = pd.DataFrame(data.loc[data["is_invoice_in_120_window"] == 1].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tav_last_120_days").reset_index())
        data = self.merge_data(data, total_ar_value_in_last_120_days, 'DEBTOR_NUMBER')
        del total_ar_value_in_last_120_days
        gc.collect()
        total_pastdue_ar_value_in_last_30_days = pd.DataFrame(data.loc[(data["is_invoice_in_30_window"] == 1) & (data['is_pastdue_in_30_days'])].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tpav_last_30_days").reset_index())
        data = self.merge_data(data, total_pastdue_ar_value_in_last_30_days, 'DEBTOR_NUMBER')
        del total_pastdue_ar_value_in_last_30_days
        gc.collect()
        total_pastdue_ar_value_in_last_60_days = pd.DataFrame(data.loc[(data["is_invoice_in_60_window"] == 1) & (data['is_pastdue_in_60_days'])].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tpav_last_60_days").reset_index())
        data = self.merge_data(data, total_pastdue_ar_value_in_last_60_days, 'DEBTOR_NUMBER')
        del total_pastdue_ar_value_in_last_60_days
        gc.collect()
        total_pastdue_ar_value_in_last_90_days = pd.DataFrame(data.loc[(data["is_invoice_in_90_window"] == 1) & (data['is_pastdue_in_90_days'])].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tpav_last_90_days").reset_index())
        data = self.merge_data(data, total_pastdue_ar_value_in_last_90_days, 'DEBTOR_NUMBER')
        del total_pastdue_ar_value_in_last_90_days
        gc.collect()
        total_pastdue_ar_value_in_last_120_days = pd.DataFrame(data.loc[(data["is_invoice_in_120_window"] == 1) & (data['is_pastdue_in_120_days'])].groupby("DEBTOR_NUMBER")["CONVERTED_ORIGINAL_AMOUNT"].sum().rename("tpav_last_120_days").reset_index())
        data = self.merge_data(data, total_pastdue_ar_value_in_last_120_days, 'DEBTOR_NUMBER')
        del total_pastdue_ar_value_in_last_120_days
        gc.collect()
        data["tav_last_30_days"] = data["tav_last_30_days"].fillna(0)
        data["tav_last_60_days"] = data["tav_last_60_days"].fillna(0)
        data["tav_last_90_days"] = data["tav_last_90_days"].fillna(0)
        data["tav_last_120_days"] = data["tav_last_120_days"].fillna(0)
        data["tpav_last_30_days"] = data["tpav_last_30_days"].fillna(0)
        data["tpav_last_60_days"] = data["tpav_last_60_days"].fillna(0)
        data["tpav_last_90_days"] = data["tpav_last_90_days"].fillna(0)
        data["tpav_last_120_days"] = data["tpav_last_120_days"].fillna(0)
        data['ropi_last_30_days'] = data['ropi_last_30_days'].fillna(0)
        data['ropi_last_60_days'] = data['ropi_last_60_days'].fillna(0)
        data['ropi_last_90_days'] = data['ropi_last_90_days'].fillna(0)
        data['ropi_last_120_days'] = data['ropi_last_120_days'].fillna(0)
        data["ppa_last_120_days"] = data["tpav_last_120_days"]/data["tav_last_120_days"]
        data["ppa_last_90_days"] = data["tpav_last_90_days"]/data["tav_last_90_days"]
        data["ppa_last_60_days"] = data["tpav_last_60_days"]/data["tav_last_60_days"]
        data["ppa_last_30_days"] = data["tpav_last_30_days"]/data["tav_last_30_days"]
        data["ppa_last_120_days"] = data["ppa_last_120_days"].fillna(0)
        data["ppa_last_90_days"] = data["ppa_last_90_days"].fillna(0)
        data["ppa_last_60_days"] = data["ppa_last_60_days"].fillna(0)
        data["ppa_last_30_days"] = data["ppa_last_30_days"].fillna(0)
        return data
    @logger
    def invoice_touch_features(self, touch_data):
        """
        Derving touch features based on number of time invoice is touched 
        """
        touch_data = touch_data[['INVOICE_ID', 'SP_DATE']]
        touch_data["SP_DATE"] = pd.to_datetime(touch_data["SP_DATE"])
        touch_data = touch_data[~touch_data.SP_DATE.isnull()]
        touch_data['first_contact'] = touch_data.groupby('INVOICE_ID')['SP_DATE'].transform('min')
        touch_data['last_contact'] = touch_data.groupby('INVOICE_ID')['SP_DATE'].transform('max')
        touch_data['touch_frequency'] = touch_data.groupby(['INVOICE_ID'], sort=False)['INVOICE_ID'].transform('count')
        touch_data['contact_duration'] = touch_data['last_contact'] - touch_data['first_contact']
        touch_data['contact_duration'] = touch_data['contact_duration'].dt.days
        invoices_with_mutiple_touches = touch_data.loc[touch_data['touch_frequency'] >= 2]
        invoices_with_mutiple_touches['average_touch_duration'] = invoices_with_mutiple_touches['contact_duration']/(invoices_with_mutiple_touches['touch_frequency']-1)
        average_touch_difference_by_invoice = invoices_with_mutiple_touches[['average_touch_duration', 'INVOICE_ID']]
        average_touch_difference_by_invoice.drop_duplicates(subset='INVOICE_ID', inplace=True)
        touch_data = self.merge_data(touch_data, average_touch_difference_by_invoice, "INVOICE_ID")
        touch_data = touch_data[['INVOICE_ID', 'contact_duration', 'average_touch_duration', 'touch_frequency', 'first_contact']]
        touch_data = touch_data.drop_duplicates(subset='INVOICE_ID')
        return touch_data
    @staticmethod
    @logger
    def customer_level_touch_features(data):
        """
        Derving customer level features 
        """
        data['touch_frequency'] = data['touch_frequency'].fillna(0)
        average_touch_duration_mean = data.loc[data['touch_frequency'] > 1]['average_touch_duration'].mean()
        data['average_touch_duration'] = data['average_touch_duration'].fillna(average_touch_duration_mean)
        data['avg_touch_duration_by_client'] = data.groupby('DEBTOR_NUMBER')['average_touch_duration'].transform('mean')
        data['first_contact'] = pd.to_datetime(data['first_contact'])
        data['due_date'] = pd.to_datetime(data['DUE_DATE'])
        data['first_touch_ageing'] = (data['first_contact'] - data["DUE_DATE"]).dt.days
        average_first_touch_ageing = data['first_touch_ageing'].mean()
        data['first_touch_ageing'].fillna(average_first_touch_ageing, inplace=True)
        data['average_first_touch_ageing'] = data.groupby('DEBTOR_NUMBER')['first_touch_ageing'].transform('mean')
        data['contact_duration'].fillna(0, inplace=True)
        data['avg_contact_duration'] = data.groupby('DEBTOR_NUMBER')['contact_duration'].transform('mean')
        data['total_touches'] = data.groupby('DEBTOR_NUMBER')['touch_frequency'].transform('sum')
        data['avg_touches_per_dollar'] = data['total_touches']/data['total_ar_value_by_client']
        data.fillna(0, inplace=True)
        data.replace([np.inf, -np.inf], 0, inplace=True)
        return data
    @staticmethod
    @logger
    def customer_history_creation(invoice_data):
        """
        slicing and addning debture number / formating for customers
        """
        customer_history = invoice_data[CUSTOMER_FEATURES]
        customer_history.drop_duplicates(subset="DEBTOR_NUMBER", inplace=True)
        customer_history = customer_history.round(6)
        customer_history["DEBTOR_NUMBER"] = customer_history["DEBTOR_NUMBER"].astype(int)
        customer_history = customer_history[CUSTOMER_FEATURES]
        customer_history_list = customer_history.values.tolist()
        return customer_history_list
    @staticmethod
    @logger
    def selfcure_modeling_creation(training_features_df):
        """
        This function creates selfcure model object by selecting all derived features
        """
        info(SELECTED_FEATURES)
        training_features_df = training_features_df[SELECTED_FEATURES]
        training_features_df["close_month_first_day"] = training_features_df['CLOSE_DATE'].values.astype('datetime64[M]')
        training_features_df["due_date_deviation"] = training_features_df["close_month_first_day"] - training_features_df["DUE_DATE"]
        training_features_df["due_date_deviation"] = training_features_df["due_date_deviation"].dt.days
        training_features_df.dropna(inplace=True)
        training_features_df.reset_index(drop=True, inplace=True)
        # scaling
        min_max_scaler = MinMaxScaler()
        train_scaled = min_max_scaler.fit_transform(training_features_df[NUMERICAL_FEATURES])
        train_normalized = pd.DataFrame(train_scaled, columns=NUMERICAL_FEATURES)
        training_features_df.drop(columns=NUMERICAL_FEATURES, inplace=True)
        train_normalized.reset_index(drop=True, inplace=True)
        train_data = pd.concat([training_features_df, train_normalized], axis=1)
        # prepare data for modelling
        X_train = train_data.drop(columns=TRAINING_DROP_COLUMNS)
        X_train = X_train[MODEL_ORDER]
        y_train = train_data["is_past_due"]
        # modelling
        logistic_classifier = LogisticRegression()
        model = logistic_classifier.fit(X_train, y_train)
        return model
    @staticmethod
    @logger
    def get_current_model_name():
        """
        This function constructs model file name to be stored model file with
        """
        curr_date = datetime.now()
        current_model_name = 'USCAN_'+str(curr_date.strftime("%d_%m_%Y_%H_%M_%S"))
        info(current_model_name)
        return current_model_name
    @logger
    def load_latest_model_from_pickle(self, model):
        """
        This function loads model files stored in the drive for the scoring and other location as needed 
        """
        parent_dir = self.config.get_location()
        model_name = self.load_last_model_name(model)
        info(model_name)
        return pickle.load(open(parent_dir+model_name+"_"+model+'.sav', 'rb'))
    @logger
    def remove_files(self, dates_list):
        """
        This function removes old filee & retains latest 6 files 
        """
        count = 0
        parent_dir = self.config.get_location()
        info(dates_list) 
        dates_list.sort(reverse = True)
        for index,filename in enumerate(dates_list):
            count += 1
            if count > 6 :
                current_model_name = 'USCAN_'+str(filename.strftime("%d_%m_%Y_%H_%M_%S"))
                fullpath = os.path.join(parent_dir, current_model_name+"_SC_LR.sav")
                info(fullpath)
                os.remove(fullpath)
    @logger
    def load_last_model_name(self, model_name):
        """
        this function loads latest model file from the disk based on model file date time if there are more than one files on the disk 
        """
        files = []
        dates_list = []
        parent_dir = self.config.get_location()
        for r, d, filedir in os.walk(parent_dir):
            for file in filedir:
                if model_name+'.sav' in file:
                    files.append(file)
        for index, filename in enumerate(files):
            try:
                info(filename)
                filename_token = filename.split('_')
                day = filename_token[1]
                month = filename_token[2]
                year = filename_token[3]
                hour = filename_token[4]
                minutes = filename_token[5]
                info(filename_token[6])
                sec = filename_token[6]
                date_str = year+"-"+month+"-"+day+" "+hour+":"+minutes+":"+sec
                date_time_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                dates_list.append(date_time_obj)
            except Exception as errorstr:
                error('Exception Occured while training the data :' + str(errorstr))
                raise Exception('Exception Occured while training the data : {}'.format(str(errorstr)))
        dates_list.sort(reverse = True)
        current_model_name = ""
        if len(dates_list) > 0:
           current_model_name = 'USCAN_'+str(dates_list[0].strftime("%d_%m_%Y_%H_%M_%S"))
        else:
           current_model_name = ""
        info(dates_list)
        self.remove_files(dates_list)
        info(current_model_name)
        return current_model_name
    @logger
    def save_to_pickle(self, model_object):
        """
        This function saves model object to the disk in picle file with save extension
        """
        parent_dir = self.config.get_location()
        filename = self.get_current_model_name()
        pickle.dump(model_object, open(parent_dir+filename+'_SC_LR.sav', 'wb'))
    @logger
    def validate_model_avaiable_for_scoring(self):
        """
        This fucntion validates whether there is at least one model file on the disk 
        for scoring 
        """
        selfcure_logistic_regression = None
        try:
            #SC_LR means selfcure logistics regression for is_past due
            selfcure_logistic_regression = self.load_last_model_name('_SC_LR')
            info(selfcure_logistic_regression)
        except Exception:
            error("Failled to fetch existing model files . model file might not be avaiable at the location!!!!")
        if len(selfcure_logistic_regression) > 0:
            info("avaiable latest model is :"+selfcure_logistic_regression)
        else:
            error('no model files avaiable .pleaae train the model before scoring')
            raise Exception('no model files avaiable .pleaae train the model before scoring')
    @logger
    def get_selfcure_lr_prediction(self, testing_data, customer_history_data, model):
        """
        this function used to get prediction for open invoicess by passing open invouce with their 
        customer histor
        """
        info('creating due deviation feature')
        cutoff_date = datetime.today()
        testing_data["due_date_deviation"] = cutoff_date - testing_data["DUE_DATE"]
        testing_data["due_date_deviation"] = (testing_data["due_date_deviation"].dt.days)*(0.3)
        # selecting customer features, merging and scaling
        features = customer_history_data[CUSTOMER_MODEL_FEATURES]
        testing_data["DEBTOR_NUMBER"] = testing_data["DEBTOR_NUMBER"].astype(int)
        testing_data = pd.merge(testing_data, features, on="DEBTOR_NUMBER", how="left")
        min_max_scaler = MinMaxScaler()
        test_scaled = min_max_scaler.fit_transform(testing_data[NUMERICAL_FEATURES])
        test_normalized = pd.DataFrame(test_scaled, columns=NUMERICAL_FEATURES)
        testing_data.drop(columns=NUMERICAL_FEATURES, inplace=True)
        test_normalized.reset_index(drop=True, inplace=True)
        testing_data = pd.concat([testing_data, test_normalized], axis=1)
        for testing_data_column in testing_data:
            testing_data.loc[testing_data[testing_data_column].isnull(), 'isnan'] = False
        testing_nan_data = testing_data.loc[testing_data['isnan'] == False]
        self.database.update_missing_feature_as_n_flag(testing_nan_data)
        testing_data = testing_data.drop(columns=["isnan"])
        testing_data.dropna(inplace=True)
        self.dataframe_empty_check(testing_data)
        X_test = testing_data.drop(columns=["DEBTOR_NUMBER", "INVOICE_ID", "DUE_DATE"])
        X_test = X_test[MODEL_ORDER]
        model = self.load_latest_model_from_pickle(model)
        # get predicitons
        predictions_prob = model.predict_proba(X_test).tolist()
        predictions = pd.DataFrame(predictions_prob, columns=["probability_of_being_on_time", "probability_of_being_past_due"])
        testing_data["probability_of_being_on_time"] = round(predictions["probability_of_being_on_time"], 6)
        testing_data["probability_of_being_on_time"].fillna(0, inplace=True)
        testing_data["prediction"] = np.where(testing_data["probability_of_being_on_time"] >= 0.5, 0, 1)
        testing_data["confidence_to_pay_on_time"] = np.where(testing_data["probability_of_being_on_time"] >= 0.5, "high", "low")
        # return predictions
        return testing_data[OUTPUT_COLUMNS]
    
