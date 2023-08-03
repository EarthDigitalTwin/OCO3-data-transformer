

# Base class of all exceptions
class BaseOCOException(Exception):
    def __init__(self, *args):
        Exception.__init__(self, *args)


# Base class of exceptions which should result in an input batch being discarded from input queue
class NonRetryableException(BaseOCOException):
    pass


# If all files in a batch are unreadable
class NoValidFilesException(NonRetryableException):
    pass


# If GranuleReader cannot open an input file
class ReaderException(BaseOCOException):
    pass


# Errors that should kill the whole program
class NonRecoverableError(BaseOCOException):
    pass


