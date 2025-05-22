import logging
import os
import sys
from datetime import datetime

'''
THIS MODULE HANDLES LOGGING FOR MULTIPROCESSING APPLICATIONS.
WE WILL PROCESS MULTIPLE DAYS IN PARALLEL BASED ON THE CONFIGURATION FILE.
EACH PYTHON PROCESS THAT WILL BE LAUNCHED WILL HAVE ITS OWN LOG FILE (FOR EACH DAY).
'''

# Environment variable for main process detection
MAIN_PID = os.environ.get('MAIN_PID')
if not MAIN_PID:
    os.environ['MAIN_PID'] = str(os.getpid())
    MAIN_PID = os.environ['MAIN_PID']

def is_main_process():
    """Determine if current process is the main application process"""
    return str(os.getpid()) == MAIN_PID

class ProcessLogger:
    def __init__(self):
        self._data_date = None
        self._logger = None
        self._formatter = None
        self._date_range = None
        self._console_fallback = False

    def set_date_range(self, start_date, end_date):
        """Set date range for main process logs (optional)"""
        if not is_main_process():
            raise RuntimeError("Only main process can set date range")
        self._date_range = f"{start_date}_{end_date}"

    def set_date(self, date):
        """Set date for child processes (MANDATORY for workers)"""
        if is_main_process():
            raise RuntimeError("Main process should not set data dates")
        self._data_date = date
        self._logger = None  # Reset logger when date changes

    def _get_logger(self):
        """Create or retrieve the appropriate logger instance"""
        if self._logger:
            return self._logger
        pid = os.getpid()
        now = datetime.now()
        try:
            logger = logging.getLogger(f"process_{pid}")
            logger.setLevel(logging.INFO)
            for handler in logger.handlers[:]:
                logger.removeHandler(handler)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] PID:%(process)d - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            self._logger = logger
            return logger
        except Exception as e:
            if not self._console_fallback:
                self._console_fallback = True
                logger = logging.getLogger(f"console_fallback_{pid}")
                logger.setLevel(logging.INFO)
                for handler in logger.handlers[:]:
                    logger.removeHandler(handler)
                handler = logging.StreamHandler(sys.stderr)
                formatter = logging.Formatter("%(asctime)s [%(levelname)s] PID:%(process)d - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                self._logger = logger
                logger.error(f"Failed to set up console logging: {e}")
                return logger
            else:
                raise

    def __getattr__(self, name):
        """Delegate logging methods to the current logger"""
        logger = self._get_logger()
        def logging_proxy(msg, *args, **kwargs):
            extra = kwargs.get('extra', {})
            extra['data_date'] = getattr(self._formatter, 'default_data_date', 'N/A') if self._formatter else 'N/A'
            kwargs['extra'] = extra
            return getattr(logger, name)(msg, *args, **kwargs)
        return logging_proxy

# Global logger instance (console only)
logger = ProcessLogger()