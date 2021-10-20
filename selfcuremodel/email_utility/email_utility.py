"""
This module contains all the query which are used in the database module .
all this query constructed dynamically based on the paramaters configured in default config file
"""
from selfcuremodel.logging_wrapper.logger import info,error
from airflow.utils.email import send_email
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.html_utility.html_utility import HtmlUtility
class SendEmail:
    """
        Class Definition
    """
    def __init__(self, config):
        """
        SendEmail Class Constructor
        """
        self.config = config
    def task_success_email_alert(job_name,task_id,execution_date,try_number,max_tries,hostname,mark_success_url, task_Owner, trigger_type):
        """
        Send custom email alerts when job success
        """
        info("inside task_success_email_alert function")
        try:
            default_config = Config()
            to_mail_addresses = default_config.get_to_mail_addresses()
            hostname = default_config.get_server_env()
            title = "Job Success Alert | Predictive Model | US Healthacare , Canada | "+ str(task_id) +"|"+ str(execution_date)
            email_html_header = HtmlUtility.get_email_html_header()
            email_html_header_img = ""
            success_email_summary_box = ""
            email_html_msg_content = HtmlUtility.get_email_html_msg_content('Success')
            inforow = HtmlUtility.get_generic_content_box_with_data(execution_date,try_number,max_tries,hostname,task_id,mark_success_url, task_Owner, trigger_type)
            if job_name == "Scoring":
                email_html_header_img = HtmlUtility.get_email_html_header_img_for_scoring('Success')
                success_email_summary_box = HtmlUtility.get_success_email_summary_box_for_scoring()
            if job_name == "Training":
                email_html_header_img = HtmlUtility.get_email_html_header_img_for_training('Success')
                success_email_summary_box = HtmlUtility.get_success_email_summary_box_for_training()
            email_html_footer_content = HtmlUtility.get_email_html_footer_content()
            body = email_html_header + email_html_header_img +  email_html_msg_content + inforow + success_email_summary_box + email_html_footer_content
            email_id_list = to_mail_addresses.split(";")
            unique_email_id_list = list(set(email_id_list))
            for email in unique_email_id_list:
                send_email(email, title, body)
            info("exiting task_success_email_alert function")     
        except Exception as estr:
            error(estr)
        return None
    def task_failure_email_alert(job_name,task_id,execution_date,try_number,max_tries,hostname,log_url, task_Owner, trigger_type):
        """
        Send custom email alerts when job success
        """
        info("inside task_failure_email_alert function")
        try:
            default_config = Config()
            to_mail_addresses = default_config.get_to_mail_addresses()
            hostname = default_config.get_server_env()
            title = "Job Failure Alert | Predictive Model | US Healthacare , Canada | "+ str(task_id) +"|"+ str(execution_date)
            email_html_header = HtmlUtility.get_email_html_header()
            email_html_header_img=""
            if job_name == "Scoring":
                email_html_header_img = HtmlUtility.get_email_html_header_img_for_scoring('Failure')
            if job_name == "Training":
                email_html_header_img = HtmlUtility.get_email_html_header_img_for_training('Failure')
            email_html_msg_content = HtmlUtility.get_email_html_msg_content('Failure')
            email_html_footer_content = HtmlUtility.get_email_html_footer_content()
            inforow = HtmlUtility.get_generic_content_box_with_data(execution_date,try_number,max_tries,hostname,task_id,log_url, task_Owner, trigger_type)
            body = str(email_html_header) + str(email_html_header_img) + str(email_html_msg_content) + str(inforow)+str(email_html_footer_content)
            email_id_list = to_mail_addresses.split(";")
            unique_email_id_list = list(set(email_id_list))
            for email in unique_email_id_list:
                send_email(email, title, body)
            info("Exiting task_failure_email_alert function")
        except Exception as estr:
            error(estr)
