"""
This module defines all constants variabl and used across other modeuless
"""
# pylint: disable=line-too-long
NUMERICAL_FEATURES = ["AVG_DPD", "AVERAGE_FIRST_TOUCH_AGEING", "AVG_CONTACT_DURATION", "AVG_TOUCH_DURATION_BY_CLIENT", "NUMBER_OF_PAYMENT_TERM"]
# pylint: disable=line-too-long
CUSTOMER_MODEL_FEATURES = ["DEBTOR_NUMBER", "AVG_DPD", "AVERAGE_FIRST_TOUCH_AGEING", "AVG_CONTACT_DURATION", "AVG_TOUCH_DURATION_BY_CLIENT", "PERCENTAGE_PAST_DUE_INVOICES",
                           "PERC_OF_DISPUTED_INVOICES", "CUSTOMER_WADCO", "NUMBER_OF_PAYMENT_TERM"]
# pylint: disable=line-too-long
SELECTED_FEATURES = ["avg_dpd","average_first_touch_ageing","avg_contact_duration","avg_touch_duration_by_client","percentage_past_due_invoices",
                     "perc_of_disputed_invoices","number_of_payment_term","customer_wadco","INVOICE_ID","is_past_due","DEBTOR_NUMBER","CLOSE_DATE","DUE_DATE"]

# pylint: disable=line-too-long
CUSTOMER_FEATURES = ["DEBTOR_NUMBER", "AVG_DPD", "median_dpd", "avg_ar", "median_ar", "total_ar_value_by_client", "number_invoices_above_20k", "CUSTOMER_WADCO",
                     "invoice_frequency", "disputed_invoices_frequency", "PERC_OF_DISPUTED_INVOICES", "disputed_invoices_ar_value", "perc_disputed_invoice_value",
                     "total_past_due_invoices", "PERCENTAGE_PAST_DUE_INVOICES", "late_payment_invoice_ar_value", "NUMBER_OF_PAYMENT_TERM", "customer_wapt",
                     "number_bill_to_country_codes", "last_5_invoice_ageing", "customer_wadc", "noi_30_days", "noi_60_days", "noi_90_days", "noi_120_days",
                     "tpi_30_days", "tpi_60_days", "tpi_90_days", "tpi_120_days", "ropi_last_30_days", "ropi_last_60_days", "ropi_last_90_days", "ropi_last_120_days",
                     "tav_last_30_days", "tav_last_60_days", "tav_last_90_days", "tav_last_120_days", "tpav_last_30_days", "tpav_last_60_days", "tpav_last_90_days",
                     "tpav_last_120_days", "ppa_last_120_days", "ppa_last_90_days", "ppa_last_60_days", "ppa_last_30_days", "average_touch_duration",
                     "touch_frequency", "AVG_TOUCH_DURATION_BY_CLIENT", "AVERAGE_FIRST_TOUCH_AGEING", "AVG_CONTACT_DURATION", "total_touches", "avg_touches_per_dollar"]
# pylint: disable=line-too-long
MODEL_ORDER = ['PERCENTAGE_PAST_DUE_INVOICES', 'PERC_OF_DISPUTED_INVOICES', 'CUSTOMER_WADCO', 'due_date_deviation', 'AVG_DPD', 'AVERAGE_FIRST_TOUCH_AGEING', 'AVG_CONTACT_DURATION', 'AVG_TOUCH_DURATION_BY_CLIENT', 'NUMBER_OF_PAYMENT_TERM']
# pylint: disable=line-too-long
OUTPUT_COLUMNS = ["INVOICE_ID", "probability_of_being_on_time", "prediction", "confidence_to_pay_on_time"]
# pylint: disable=line-too-long
TRAINING_DROP_COLUMNS = ["DEBTOR_NUMBER","INVOICE_ID","CLOSE_DATE","DUE_DATE","is_past_due","close_month_first_day"]
