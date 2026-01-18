from asyncio.log import logger
import logging
from pathlib import Path
from datetime import datetime

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

class EngineLogger:
    def __init__(self, base_path: str, client: str = "UNKNOWN", level: str = "INFO"):
        self.base_path = Path(base_path)
        self.client = client

        self.level_name = level.upper()
        self.level = LOG_LEVELS.get(self.level_name, logging.INFO)

        self.run_dt = datetime.now()
        self.run_date = self.run_dt.strftime("%d-%m-%Y")
        self.run_ts = self.run_dt.strftime("%Y-%m-%d_%H%M%S")

        self.log_dir = self.base_path / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.normal_log_file = self.log_dir / f"allocator_engine_{self.run_ts}.log"
        self.error_log_file = self.log_dir / f"Error_{self.run_ts}.log"

        self.logger = self._setup_logger()

        self._write_run_header()

    # ---------------- SETUP ----------------
    def _setup_logger(self):
        logger = logging.getLogger(f"allocator_engine_{self.run_ts}")
        logger.setLevel(self.level)
        logger.propagate = False

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        normal_handler = logging.FileHandler(self.normal_log_file, mode="a")
        normal_handler.setLevel(self.level)
        normal_handler.setFormatter(formatter)

        error_handler = logging.FileHandler(self.error_log_file, mode="a")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(formatter)

        logger.addHandler(normal_handler)
        logger.addHandler(error_handler)
        logger.addHandler(console_handler)

        return logger


    # ---------------- HEADER / FOOTER ----------------
    def _write_run_header(self):
        header = (
            f"Today's Date : {self.run_date}\n"
            f"Client : {self.client}\n"
            f"Run Started At : {self.run_dt.strftime('%H:%M:%S')}\n"
            f"{'-'*100}"
        )
        self.logger.info(header)

    def write_run_footer(self):
        footer = (
            f"Run Ended At : {datetime.now().strftime('%H:%M:%S')}\n"
            f"{'-'*100}"
        )
        self.logger.info(footer, extra={"end": "\n"})

    # ---------------- PUBLIC METHODS ----------------
    def info(self, msg: str, *args):
        self.logger.info(msg, *args)

    def warning(self, msg: str, *args):
        self.logger.warning(msg, *args)

    def error(self, msg: str, exc_info=False, *args):
        self.logger.error(msg, exc_info=exc_info, *args)

    def debug(self, msg: str, *args):
        self.logger.debug(msg, *args)

    def critical(self, msg: str, exc_info=False, *args):
        self.logger.critical(msg, exc_info=exc_info, *args)

    def exception(self, msg: str, *args):
        self.logger.exception(msg, *args)

