import logging

class AuthFailureHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.failed = False
        self.msg = None

    def emit(self, record):
        msg = record.getMessage()
        if "Authentication failed" in msg:
            self.failed = True
            self.msg = msg 

def install_auth_failure_handler():
    handler = AuthFailureHandler()
    logging.getLogger().addHandler(handler)
    return handler