import logging
import os
from datetime import datetime

'''
THIS MODULE HANDLES LOGGING FOR MULTIPROCESSING APPLICATIONS.
WE WILL PROCESS MULTIPLE DAYS IN PARALLEL BASED ON THE CONFIGURATION FILE.
EACH PYTHON PROCESS THAT WILL BE LAUNCHED WILL HAVE ITS OWN LOG FILE (FOR EACH DAY).
'''


# Configure log directory
log_dir = "exported_log_files"
os.makedirs(log_dir, exist_ok=True)

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
        self._date_range = None  # New attribute for date range
        
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
        
        if is_main_process():
            # Include date range in filename if set
            range_suffix = f"_{self._date_range}" if self._date_range else ""
            filename = f"MAINPROCESS{range_suffix}_{now:%Y%m%d_%H%M%S}.log"
            display_date = "MAIN_PROCESS"
        else:
            if not self._data_date:
                raise ValueError("Child process must set date before logging")
            date_str = self._data_date.strftime("%Y%m%d")
            filename = f"CHILDPROCESS_{pid}_{date_str}_{now:%Y%m%d_%H%M%S}.log"
            display_date = self._data_date.strftime("%Y-%m-%d")

        # Create new logger
        logger = logging.getLogger(f"process_{pid}_{date_str if not is_main_process() else 'main'}")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            
        # Create file handler
        handler = logging.FileHandler(os.path.join(log_dir, filename))
        
        # Create and store formatter
        self._formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] PID:%(process)d Data:%(data_date)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self._formatter.default_data_date = display_date
        handler.setFormatter(self._formatter)
        logger.addHandler(handler)
        
        self._logger = logger
        return logger
        
    def __getattr__(self, name):
        """Delegate logging methods to the current logger"""
        logger = self._get_logger()
        
        # Create proxy method to handle data_date injection
        def logging_proxy(msg, *args, **kwargs):
            extra = kwargs.get('extra', {})
            extra['data_date'] = self._formatter.default_data_date
            kwargs['extra'] = extra
            return getattr(logger, name)(msg, *args, **kwargs)
            
        return logging_proxy

# Global logger instance
logger = ProcessLogger()