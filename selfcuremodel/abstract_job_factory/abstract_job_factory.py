"""This is abstract factory design class which used to fetch diferent job / job class based on the parameter"""
from selfcuremodel.abstract_job_factory.validate_training.validate_training import ValidateTraining
from selfcuremodel.abstract_job_factory.validate_test.validate_test import ValidateTest
from selfcuremodel.abstract_job_factory.batch_push.batch_push import BatchPush
from selfcuremodel.abstract_job_factory.model_training.model_training import ModelTrainiing
from selfcuremodel.abstract_job_factory.inferencing.inferencing import Inferencing
from selfcuremodel.abstract_job_factory.feature_engineering.feature_engineering import FeatureEngineering
from selfcuremodel.logging_wrapper.logger import logger, info
class AbstractJob():
    """
    Class Definition
    """
    @staticmethod
    @logger
    def get_job_class(job_type_string):
        """ Abstract job factory method"""
        info('received parameter for getJobClass function is '+str(job_type_string))
        if job_type_string == "ValidateTraining":
            return ValidateTraining()
        if job_type_string == "FeatureEngineering":
            return FeatureEngineering()
        if job_type_string == "ModelTrainiing":
            return ModelTrainiing()
        if job_type_string == "ValidateTest":
            return ValidateTest()
        if job_type_string == "Inferencing":
            return Inferencing()
        if job_type_string == "BatchPush":
            return BatchPush()
        raise Exception('Invalid input for JobTypeString variable.'+str(job_type_string))
