import signal

class TimeoutException(Exception):
    """
    Exception that is thrown when a timeout occurs.
    """
    pass

class Timeout(object):
    """
    A class that implements timeout using ALARM signal.
    Wrap any python call using the with(Timeout(seconds))
    which will raise a TimoutException if the python call
    does not finish within 'seconds'
    """

    def __init__(self, sec, cmd=None):
        self.sec = sec
        self.cmd = cmd

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)    # disable alarm

    def raise_timeout(self, *args):
        if self.cmd:
            self.cmd.cancel()
        raise TimeoutException()

