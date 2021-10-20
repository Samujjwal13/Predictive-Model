"""
This module used as logging wrapper and for the common exception handling 
for across the all 
"""
import logging
logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logging.getLogger(__name__)
def logger(func):
    """
    python module import
    """
    def wrapper(*args):
        """
        wrping function that is being called on annotation of other function using logger annottaion
        """
        try:
            func_name = func.__name__
            logging.info("Entering function : %s", func_name)
            retval = func(*args)
            logging.info("Exiting function : %s after successfully running ", func_name)
            return retval
        except TypeError as type_error:
            errorstr = 'takes 0 positional arguments but'
            if errorstr in str(type_error):
                try:
                    func_name = func.__name__
                    retval = func()
                    logging.info("Exiting function : %s after successfully running ", func_name)
                    return retval
                except Exception as estr:
                    err = "There was an exception found in function   "+func.__name__+' ERROR is :'+str(estr)
                    #logging.error(err)
                    #logging.exception(err)
                    raise Exception(err)
        except Exception as estr:
            err = "There was an exception found in function   "+func.__name__+' ERROR is :'+str(estr)
            #logging.error(err)
            #logging.exception(err)
            raise Exception(err)
    return wrapper
def info(msg, extra=None):
    """
    common wrapper for info
    """
    logging.info(msg, extra=extra)

def error(msg, extra=None):
    """
    common wrapper for error
    """
    logging.error(msg, extra=extra)

def debug(msg, extra=None):
    """
    common wrapper for debug
    """
    logging.debug(msg, extra=extra)

def warn(msg, extra=None):
    """
    common wrapper for warning
    """
    logging.warning(msg, extra=extra)
