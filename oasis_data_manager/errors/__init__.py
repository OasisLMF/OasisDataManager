class OasisDataManagerException(Exception):
    """
    Base exception for oasis-data-manager.

    Example
    -------
    In [call]: raise OasisDataManagerException('Error Message 1', OSError('Root of error'))
    OasisDataManagerException: Error Message 1, from OSError: Root of error
    """

    def __init__(self, msg, original_exception=None):
        self.original_exception = original_exception
        if original_exception:
            # This is a wrapped exception
            super().__init__(
                f"{msg}, {original_exception.__class__.__name__}: {original_exception}"
            )
        else:
            # Message only exception
            super().__init__(msg)


# Backward-compatibility alias — use OasisDataManagerException in new code
OasisException = OasisDataManagerException
