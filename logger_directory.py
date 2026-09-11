import os

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
