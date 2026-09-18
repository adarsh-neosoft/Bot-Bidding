import logging
import os
from logging.handlers import TimedRotatingFileHandler

from logging_amns import init_logging

# Defaults to this repo's own directory (matches the production deployment
# path); override with BOT_LOG_BASE_DIR if logs should go elsewhere.
BASE_DIR = os.getenv("BOT_LOG_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))


AuctionPlayer = init_logging(log_name='AuctionPlayer', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))

bot_runner = init_logging(log_name='bot_runner', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))

join_scheduler = init_logging(log_name='join_scheduler', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))

scheduler = init_logging(log_name='scheduler', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))

state_store = init_logging(log_name='state_store', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))

ws_adapter = init_logging(log_name='ws_adapter', log_level='DEBUG', enable_mailing=False,
                             rotation_criteria='time', delay=1, rotate_interval=1, rotate_when='d',
                             backup_count=750,
                             log_directory=os.path.join(BASE_DIR, 'logs'))


def attach_combined_handler():
    """Mirror every log record (any module, any level) into one file.

    The per-module/per-level files created above (AuctionPlayer, bot_runner,
    join_scheduler, scheduler, state_store, ws_adapter) keep working as-is.
    This adds one extra handler on the root logger so a single run of
    bot_runner.py — including its spawned ws_adapter child processes, which
    each re-import this module and call this function too — also gets one
    combined, chronologically-ordered log file. Safe to call more than once
    per process; later calls are no-ops.
    """
    root_logger = logging.getLogger()

    combined_log_file = os.path.abspath(os.path.join(BASE_DIR, 'logs', 'bot_runner_combined.log'))
    already_attached = any(
        isinstance(h, TimedRotatingFileHandler) and getattr(h, 'baseFilename', None) == combined_log_file
        for h in root_logger.handlers
    )
    if already_attached:
        return

    os.makedirs(os.path.dirname(combined_log_file), exist_ok=True)

    combined_handler = TimedRotatingFileHandler(
        combined_log_file, when='d', interval=1, backupCount=750, delay=True,
    )
    combined_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] -- %(levelname)s - %(name)s - %(filename)s -- %(funcName)s - '
        'Line %(lineno)d -- %(message)s'
    ))
    # NOTSET so it passes through whatever level the originating logger
    # already allowed, instead of filtering anything out again here.
    combined_handler.setLevel(logging.NOTSET)

    root_logger.addHandler(combined_handler)
