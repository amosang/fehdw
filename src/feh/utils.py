import sys
import logging


def dec_err_handler(retries=0):
    """
    Decorator function to handle logging and retries.
    Usage: Call without the retries parameter to have it log exceptions only.

    Ref: https://stackoverflow.com/questions/11731136/python-class-method-decorator-with-self-arguments

    :retries: Number of times to retry, in addition to original try.
    """
    # if not isinstance(logger, logging.Logger):  # Ensures that a Logger object is provided.
    #     print('[ERROR] Please provide an instance of class: logging.Logger')
    #     sys.exit('[ERROR] Please provide an instance of class: logging.Logger')

    def wrap(f):  # Doing the wrapping here. Called during the decoration of the function.
        def wrapped_err_handler(*args):
            logger = args[0].logger  # args[0] is "self". Assumes that self.logger has already been created on __init__.

            if not isinstance(logger, logging.Logger):  # Ensures that a Logger object is provided.
                print('[ERROR] Please provide an instance of class: logging.Logger')
                sys.exit('[ERROR] Please provide an instance of class: logging.Logger')

            for i in range(retries + 1):  # First attempt 0 does not count as a retry.
                try:
                    f(*args)
                    #print('No exception encountered')
                    break  # So you don't run f() multiple times!
                except Exception as ex:
                    # To only log exceptions.
                    #print('Exception: ' + str(ex))
                    if i > 0:
                        logger.info(f'[RETRYING] {f.__name__}: {i}/{retries}')
                    logger.error(ex)

        wrapped_err_handler.__name__ = f.__name__  # Nicety. Rename the error handler function name to that of the wrapped function.
        return wrapped_err_handler

    return wrap

