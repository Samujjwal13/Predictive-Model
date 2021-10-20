"""
This module used for validating the training phase and to do sanity check before 
running training for selfcure
"""
from selfcuremodel.logging_wrapper.logger import logger, error
from selfcuremodel.entity.parser.config import  Config
from selfcuremodel.query_formatter.query_formatter import  Query
from selfcuremodel.database.database import  Database
class ValidateTraining():
    """
    Class Definition
    """
    def __init__(self):
        """
        Constructor for ValidateTraining class and dependancy initialization
        """
        default_config = Config()
        query = Query(default_config)
        database = Database(default_config)
        self.config = default_config
        self.query = query
        self.database = database
    @logger
    def run_job(self):
        """
        This function calls sequence of other module functions for completing ValidateTraining phase
        """
        try:
            train_valiate_query = self.query.get_train_valiate_query()
            self.database.validate_training_data(train_valiate_query)
            train_valiate_query = self.query.get_touch_validate_query()
            self.database.validate_touch_data(train_valiate_query)
            self.set_actual_score()
        except Exception as err:
            error('Exception Occured while running validateTrainingJob  :' + str(err))
            raise Exception('Exception Occured while running validateTrainingJob  : {}'.format(str(err)))
    @logger
    def set_actual_score(self):
        """
        This function fetches all predicted invoicess and checks actual score of the invoice if there is an entry in training
        phase and sets actual score 0f the model
        """
        try:
            actual_score_query = self.query.get_actual_bin_and_score_query()
            actual_score_data = self.database.get_data_from_database(actual_score_query)
            self.database.update_actual_score(actual_score_data)  
        except Exception as err:
            error('Exception Occured while running set_actual_score job :' + str(err))
            raise Exception('Exception Occured while running set_actual_score job : {}'.format(str(err)))

            
