"""
This module parses default_config.config file and loads configurationvalue 
from the file and suplies for  
"""
from configparser import ConfigParser
from selfcuremodel.logging_wrapper.logger import logger, info, error
class Config:
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor
        """
        try:
            parser = ConfigParser()
            config_file_path ="/u01/AirflowVEnv/dags/default.config"
            info(config_file_path)
            parser.read(config_file_path)
            location = parser['model file location']['location']
            h3businesid = parser['Database Filter Conditions']['h3businesid']
            doc_type_description = parser['Database Filter Conditions']['doc_type_description']
            retionperiod = parser['Database Filter Conditions']['dataretionperiod']
            oracleconnectionname = parser['Database Connection Name']['default_connection_name']
            rescoring = parser['ReScoring']['rescoring']
            to_mail_addresses = parser['To Mail Adressess']['to_mail_addresses']
            server = parser['Server Environment']['Environment']
            self._location = location
            self._h3businesid = h3businesid
            self._doc_type_description = doc_type_description
            self._retionperiod = retionperiod
            self._connection = oracleconnectionname
            self._rescoring = rescoring
            self._to_mail_addresses = to_mail_addresses
            self._server = server
        except Exception as errorstr:
            error(str(errorstr))
            raise Exception(str(errorstr))
    @logger
    def get_location(self):
        """
        returns location
        """
        return self._location
    @logger
    def get_h3_business_id(self):
        """
        returns h3_business_id
        """
        return self._h3businesid
    @logger
    def get_doc_type_description(self):
        """
        returns doc_type_description
        """
        return self._doc_type_description
    @logger
    def get_retionperiod(self):
        """
        returns retionperiod
        """
        return self._retionperiod
    @logger
    def get_connection(self):
        """
        returns connection name
        """
        return self._connection
    @logger
    def get_rescoring(self):
        """
        returns rescoring flag
        """
        return self._rescoring
    @logger
    def get_to_mail_addresses(self):
        """
        returns rescoring flag
        """
        return self._to_mail_addresses
    @logger
    def get_server_env(self):
        """
        returns server env flag
        """
        return self._server
